package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/smallnest/goclaw/bus"
	"github.com/smallnest/goclaw/config"
	"github.com/smallnest/goclaw/internal/logger"
	"github.com/tencent-connect/botgo"
	"github.com/tencent-connect/botgo/dto"
	"github.com/tencent-connect/botgo/log"
	"github.com/tencent-connect/botgo/openapi"
	"github.com/tencent-connect/botgo/token"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
)

// QQChannel QQ 官方开放平台 Bot 通道
// 使用 botgo SDK 实现：https://github.com/tencent-connect/botgo
type QQChannel struct {
	*BaseChannelImpl
	appID        string
	appSecret    string
	api          openapi.OpenAPI
	tokenSource  oauth2.TokenSource
	tokenCancel  context.CancelFunc
	session      *dto.WebsocketAP
	ctx          context.Context
	cancel       context.CancelFunc
	conn         *websocket.Conn
	connMu       sync.Mutex
	mu           sync.RWMutex
	sessionID    string
	lastSeq      uint32
	heartbeatInt int
	accessToken  string
	msgSeqMap    map[string]uint32 // 消息序列号管理，用于去重
}

// filteredLogger 静默 botgo SDK 的日志
type filteredLogger struct{}

func (f *filteredLogger) Debug(v ...interface{})                 {}
func (f *filteredLogger) Info(v ...interface{})                  {}
func (f *filteredLogger) Warn(v ...interface{})                  {}
func (f *filteredLogger) Error(v ...interface{})                 {}
func (f *filteredLogger) Debugf(format string, v ...interface{}) {}
func (f *filteredLogger) Infof(format string, v ...interface{})  {}
func (f *filteredLogger) Warnf(format string, v ...interface{})  {}
func (f *filteredLogger) Errorf(format string, v ...interface{}) {}
func (f *filteredLogger) Sync() error                            { return nil }

// WSPayload WebSocket 消息负载
type WSPayload struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d"`
	S  uint32          `json:"s"`
	T  string          `json:"t"`
}

// HelloData Hello 事件数据
type HelloData struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

// ReadyData Ready 事件数据
type ReadyData struct {
	SessionID string `json:"session_id"`
	Version   int    `json:"version"`
	User      struct {
		ID       string `json:"id"`
		Username string `json:"username"`
		Bot      bool   `json:"bot"`
	} `json:"user"`
}

// MessageAttachment 附件定义
type MessageAttachment struct {
	URL          string `json:"url,omitempty"`
	FileName     string `json:"filename,omitempty"`
	Height       int    `json:"height,omitempty"`
	Size         int    `json:"size,omitempty"`
	Width        int    `json:"width,omitempty"`
	ContentType  string `json:"content_type,omitempty"`   // voice:语音, image/xxx: 图片 video/xxx: 视频
	VoiceWavURL  string `json:"voice_wav_url,omitempty"`  // 当为语音时，语音的wav格式下发URL
	AsrReferText string `json:"asr_refer_text,omitempty"` // 当为语音时，语音的asr参考文本
}

// MessageEventData C2C和群 消息事件数据
type MessageEventData struct {
	ID        string `json:"id"`
	Content   string `json:"content"`
	Timestamp string `json:"timestamp"`
	Author    struct {
		ID string `json:"id"`
	} `json:"author"`
	GroupOpenID string               `json:"group_openid"` // 群消息回调时的群OpenID
	Attachments []*MessageAttachment `json:"attachments"`
}

// ATMessageEventData 频道 @消息事件数据
type ATMessageEventData struct {
	ID        string `json:"id"`
	Content   string `json:"content"`
	Timestamp string `json:"timestamp"`
	Author    struct {
		ID       string `json:"id"`
		Username string `json:"username"`
	} `json:"author"`
	ChannelID string `json:"channel_id"`
	GuildID   string `json:"guild_id"`
}

// RichMediaMessage rich media message.
// It is recommended to upload first, then send using message type 7.
type RichMediaMessage struct {
	FileType uint64 `json:"file_type,omitempty"` // file type: 1-image, 2-video, 3-voice (currently voice only supports silk format)
	URL      string `json:"url,omitempty"`       // rich media file to send, HTTP or HTTPS link
	FileName string `json:"file_name,omitempty"` // file name for files sent via FileData
	FileData []byte `json:"file_data,omitempty"` // file binary data for files sent via FileData
}

// GetEventID event ID
func (msg RichMediaMessage) GetEventID() string {
	return ""
}

// GetSendType message type
func (msg RichMediaMessage) GetSendType() dto.SendType {
	return dto.RichMedia
}

// NewQQChannel 创建 QQ 官方 Bot 通道
func NewQQChannel(accountID string, cfg config.QQChannelConfig, bus *bus.MessageBus) (*QQChannel, error) {
	if cfg.AppID == "" || cfg.AppSecret == "" {
		return nil, fmt.Errorf("qq app_id and app_secret are required")
	}

	baseCfg := BaseChannelConfig{
		Enabled:    cfg.Enabled,
		AccountID:  accountID,
		AllowedIDs: cfg.AllowedIDs,
	}

	return &QQChannel{
		BaseChannelImpl: NewBaseChannelImpl("qq", accountID, baseCfg, bus),
		appID:           cfg.AppID,
		appSecret:       cfg.AppSecret,
		msgSeqMap:       make(map[string]uint32),
	}, nil
}

// Start 启动 QQ 官方 Bot 通道
func (c *QQChannel) Start(ctx context.Context) error {
	if err := c.BaseChannelImpl.Start(ctx); err != nil {
		return err
	}

	logger.Info("Starting QQ Official Bot channel", zap.String("app_id", c.appID))

	// 设置自定义 logger，静默 SDK 日志
	log.DefaultLogger = &filteredLogger{}

	// 创建 token source
	credentials := &token.QQBotCredentials{
		AppID:     c.appID,
		AppSecret: c.appSecret,
	}
	c.tokenSource = token.NewQQBotTokenSource(credentials)

	// 启动 token 自动刷新
	tokenCtx, cancel := context.WithCancel(context.Background())
	c.tokenCancel = cancel
	if err := token.StartRefreshAccessToken(tokenCtx, c.tokenSource); err != nil {
		return fmt.Errorf("failed to start token refresh: %w", err)
	}

	// 初始化 OpenAPI
	c.api = botgo.NewOpenAPI(c.appID, c.tokenSource).WithTimeout(10 * time.Second).SetDebug(false)

	// 启动 WebSocket 连接
	c.ctx, c.cancel = context.WithCancel(ctx)
	go c.connectWebSocket(c.ctx)

	logger.Info("QQ Official Bot channel started (WebSocket mode)")

	return nil
}

// connectWebSocket 连接 WebSocket
func (c *QQChannel) connectWebSocket(ctx context.Context) {
	reconnectDelay := 1000 * time.Millisecond
	maxDelay := 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			logger.Info("QQ WebSocket connection stopped by context")
			return
		default:
			if err := c.doConnect(ctx); err != nil {
				logger.Error("QQ WebSocket connection failed, will retry",
					zap.Error(err),
					zap.Duration("retry_after", reconnectDelay),
				)
				time.Sleep(reconnectDelay)
				// 递增延迟
				reconnectDelay *= 2
				if reconnectDelay > maxDelay {
					reconnectDelay = maxDelay
				}
			} else {
				// 连接成功，重置延迟
				reconnectDelay = 1000 * time.Millisecond
				// 等待连接关闭或上下文取消
				c.waitForConnection(ctx)
			}
		}
	}
}

// doConnect 执行单次连接
func (c *QQChannel) doConnect(ctx context.Context) error {
	// 获取 access token
	token, err := c.tokenSource.Token()
	if err != nil {
		return fmt.Errorf("failed to get access token: %w", err)
	}
	c.accessToken = token.AccessToken

	// 获取 WebSocket URL
	wsResp, err := c.api.WS(ctx, map[string]string{}, "")
	if err != nil {
		return fmt.Errorf("failed to get websocket URL: %w", err)
	}

	c.mu.Lock()
	c.session = wsResp
	c.mu.Unlock()

	logger.Debug("QQ WebSocket URL obtained")

	// 连接 WebSocket
	c.connMu.Lock()
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, wsResp.URL, nil)
	c.connMu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to dial websocket: %w", err)
	}

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

	logger.Debug("QQ WebSocket connected")

	// 等待 Hello 消息并处理
	return c.waitForHello(ctx)
}

// waitForHello 等待并处理 Hello 消息
func (c *QQChannel) waitForHello(ctx context.Context) error {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()

	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	// 读取第一条消息（应该是 Hello）
	_, message, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read Hello message: %w", err)
	}

	var payload WSPayload
	if err := json.Unmarshal(message, &payload); err != nil {
		return fmt.Errorf("failed to parse Hello message: %w", err)
	}

	// Hello 事件 (op=10)
	if payload.Op != 10 {
		return fmt.Errorf("expected Hello (op=10), got op=%d", payload.Op)
	}

	var helloData HelloData
	if err := json.Unmarshal(payload.D, &helloData); err != nil {
		return fmt.Errorf("failed to parse Hello data: %w", err)
	}

	c.heartbeatInt = helloData.HeartbeatInterval
	logger.Debug("QQ Hello received", zap.Int("heartbeat_interval", c.heartbeatInt))

	// 如果有 session_id，尝试 Resume；否则发送 Identify
	if c.sessionID != "" {
		return c.sendResume()
	}
	return c.sendIdentify()
}

// sendIdentify 发送 Identify
func (c *QQChannel) sendIdentify() error {
	// 尝试完整权限（群聊+私信+频道）
	intents := (1 << 25) | (1 << 12) | (1 << 30) | (1 << 0) | (1 << 1)

	payload := map[string]interface{}{
		"op": 2,
		"d": map[string]interface{}{
			"token":   fmt.Sprintf("QQBot %s", c.accessToken),
			"intents": intents,
			"shard":   []uint32{0, 1},
		},
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if err := c.conn.WriteJSON(payload); err != nil {
		return fmt.Errorf("failed to send identify: %w", err)
	}

	logger.Debug("QQ Identify sent", zap.Int("intents", intents))
	return nil
}

// sendResume 发送 Resume
func (c *QQChannel) sendResume() error {
	payload := map[string]interface{}{
		"op": 6,
		"d": map[string]interface{}{
			"token":      fmt.Sprintf("QQBot %s", c.accessToken),
			"session_id": c.sessionID,
			"seq":        c.lastSeq,
		},
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if err := c.conn.WriteJSON(payload); err != nil {
		return fmt.Errorf("failed to send resume: %w", err)
	}

	logger.Debug("QQ Resume sent", zap.String("session_id", c.sessionID), zap.Uint32("seq", c.lastSeq))
	return nil
}

// waitForConnection 等待 WebSocket 连接关闭
func (c *QQChannel) waitForConnection(ctx context.Context) {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()

	if conn == nil {
		return
	}

	// 启动心跳
	heartbeatTicker := time.NewTicker(time.Duration(c.heartbeatInt) * time.Millisecond)
	defer heartbeatTicker.Stop()

	// 消息读取通道
	messageChan := make(chan []byte, 100)
	errorChan := make(chan error, 1)

	// 单独的 goroutine 读取消息
	go func() {
		for {
			c.connMu.Lock()
			currentConn := c.conn
			c.connMu.Unlock()

			if currentConn == nil {
				errorChan <- fmt.Errorf("connection closed")
				return
			}

			_, message, err := currentConn.ReadMessage()
			if err != nil {
				errorChan <- err
				return
			}
			messageChan <- message
		}
	}()

	// 消息处理循环
	for {
		select {
		case <-ctx.Done():
			logger.Debug("QQ WebSocket context cancelled")
			return
		case <-heartbeatTicker.C:
			c.sendHeartbeat()
		case message := <-messageChan:
			c.handleMessage(message)
		case err := <-errorChan:
			logger.Warn("WebSocket read error", zap.Error(err))
			return
		}
	}
}

// sendMessage 发送消息到 WebSocket
func (c *QQChannel) sendMessage(op int, d interface{}) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("connection is nil")
	}

	payload := map[string]interface{}{
		"op": op,
		"d":  d,
	}

	return c.conn.WriteJSON(payload)
}

// sendHeartbeat 发送心跳
func (c *QQChannel) sendHeartbeat() {
	if err := c.sendMessage(1, c.lastSeq); err != nil {
		logger.Warn("Failed to send heartbeat", zap.Error(err))
	}
}

// handleMessage 处理 WebSocket 消息
func (c *QQChannel) handleMessage(message []byte) {
	var payload WSPayload
	if err := json.Unmarshal(message, &payload); err != nil {
		logger.Warn("Failed to parse WebSocket message", zap.Error(err))
		return
	}

	// 更新 seq
	if payload.S > 0 {
		c.lastSeq = payload.S
	}

	switch payload.Op {
	case 0: // Dispatch
		c.handleDispatch(payload.T, payload.D)
	case 1: // Heartbeat ACK
		logger.Debug("QQ Heartbeat ACK")
	case 7: // Reconnect
		logger.Debug("QQ Reconnect requested")
	default:
		logger.Debug("QQ WebSocket message", zap.Int("op", payload.Op), zap.String("t", payload.T))
	}
}

// handleDispatch 处理 Dispatch 事件
func (c *QQChannel) handleDispatch(eventType string, data json.RawMessage) {
	switch eventType {
	case "READY":
		c.handleReady(data)
	case "RESUMED":
		logger.Debug("QQ Session resumed")
	case "C2C_MESSAGE_CREATE":
		c.handleC2CMessage(data)
	case "GROUP_AT_MESSAGE_CREATE":
		c.handleGroupATMessage(data)
	case "AT_MESSAGE_CREATE":
		// 频道消息（已经不支持)
		// c.handleChannelATMessage(data)
	case "DIRECT_MESSAGE_CREATE":
		// 频道私信（暂不处理）
	default:
		logger.Debug("QQ Event", zap.String("event_type", eventType))
	}
}

// handleReady 处理 Ready 事件
func (c *QQChannel) handleReady(data json.RawMessage) {
	var readyData ReadyData
	if err := json.Unmarshal(data, &readyData); err != nil {
		logger.Warn("Failed to parse Ready data", zap.Error(err))
		return
	}

	c.sessionID = readyData.SessionID
	logger.Debug("QQ Ready", zap.String("session_id", c.sessionID))
}

// handleC2CMessage 处理 C2C 消息
func (c *QQChannel) handleC2CMessage(data json.RawMessage) {
	var event MessageEventData
	if err := json.Unmarshal(data, &event); err != nil {
		logger.Warn("Failed to parse C2C message", zap.Error(err))
		return
	}

	senderID := event.Author.ID
	if !c.IsAllowed(senderID) {
		return
	}

	content, media := c.deocdeContentAndMedia(event)

	logger.Info("QQ Group @c2c", zap.String("content", content))

	msg := &bus.InboundMessage{
		ID:        event.ID,
		Content:   content,
		AccountID: c.AccountID(),
		SenderID:  senderID,
		ChatID:    senderID,
		Channel:   c.Name(),
		Timestamp: time.Now(),
		Metadata: map[string]interface{}{
			"chat_type": "c2c",
			"msg_id":    event.ID,
		},
		Media: media,
	}

	logger.Debug("QQ C2C message", zap.String("sender", senderID), zap.String("content", event.Content))
	_ = c.PublishInbound(context.Background(), msg)
}

// handleGroupATMessage 处理群 @消息
func (c *QQChannel) handleGroupATMessage(data json.RawMessage) {
	var event MessageEventData
	if err := json.Unmarshal(data, &event); err != nil {
		logger.Warn("Failed to parse Group @message", zap.Error(err))
		return
	}

	senderID := event.Author.ID
	if !c.IsAllowed(senderID) && !c.IsAllowed(event.GroupOpenID) {
		return
	}

	content, media := c.deocdeContentAndMedia(event)

	logger.Info("QQ Group @message", zap.String("content", content))

	msg := &bus.InboundMessage{
		ID:        event.ID,
		Content:   content,
		AccountID: c.AccountID(),
		SenderID:  senderID,
		ChatID:    event.GroupOpenID,
		Channel:   c.Name(),
		Timestamp: time.Now(),
		Metadata: map[string]interface{}{
			"chat_type":     "group",
			"group_id":      event.GroupOpenID,
			"member_openid": senderID,
			"msg_id":        event.ID,
		},
		Media: media,
	}

	logger.Debug("QQ Group @message", zap.String("group", event.GroupOpenID),
		zap.String("sender", senderID), zap.String("content", event.Content))
	_ = c.PublishInbound(context.Background(), msg)
}

func (c *QQChannel) deocdeContentAndMedia(event MessageEventData) (string, []bus.Media) {
	// 解析表情文本
	content := c.parseEmojiText(event.Content)

	var mediaList []bus.Media

	// 处理附件
	if event.Attachments != nil && len(event.Attachments) > 0 {
		for _, attachment := range event.Attachments {
			mediaType := c.getAttachmentType(attachment)
			media := bus.Media{
				Type:     mediaType,
				URL:      attachment.URL,
				MimeType: attachment.ContentType,
			}

			if attachment.VoiceWavURL != "" {
				media.URL = attachment.VoiceWavURL
				media.MimeType = "audio/wav"
			}

			// 如果是语音，添加ASR参考文本
			if mediaType == "audio" && attachment.AsrReferText != "" {
				// 在内容中添加语音转写文本
				if content != "" {
					content += "\n"
				}
				content += fmt.Sprintf("[语音: %s]", attachment.AsrReferText)
			}
			mediaList = append(mediaList, media)
		}
	}

	return content, mediaList
}

// parseEmojiText 解析QQ表情符号
func (c *QQChannel) parseEmojiText(content string) string {
	// 替换常见的转义字符
	content = strings.ReplaceAll(content, `\\`, `\`)
	content = strings.ReplaceAll(content, "\\u003c", "<")
	content = strings.ReplaceAll(content, "\\u003e", ">")
	content = strings.ReplaceAll(content, `\"`, `"`)

	// 使用正则表达式匹配表情符号
	// QQ表情格式示例: <emoji:id=xxx> 或 <face:xxx>
	emojiPattern := regexp.MustCompile(`<[^>]+>`)
	content = emojiPattern.ReplaceAllStringFunc(content, func(match string) string {
		// 提取表情描述
		if strings.Contains(match, "emoji") || strings.Contains(match, "face") {
			return "[表情]"
		}
		return match
	})

	return strings.TrimSpace(content)
}

// getAttachmentType 根据ContentType确定附件类型
func (c *QQChannel) getAttachmentType(attachment *MessageAttachment) string {
	if strings.HasPrefix(attachment.ContentType, "image") {
		return "image"
	} else if strings.HasPrefix(attachment.ContentType, "video") {
		return "video"
	} else if strings.HasPrefix(attachment.ContentType, "audio") ||
		strings.HasPrefix(attachment.ContentType, "voice") {
		return "audio"
	} else {
		return "file"
	}
}

// handleChannelATMessage 处理频道 @消息
func (c *QQChannel) handleChannelATMessage(data json.RawMessage) {
	var event ATMessageEventData
	if err := json.Unmarshal(data, &event); err != nil {
		logger.Warn("Failed to parse Channel @message", zap.Error(err))
		return
	}

	senderID := event.Author.ID
	if !c.IsAllowed(senderID) && !c.IsAllowed(event.ChannelID) {
		return
	}

	msg := &bus.InboundMessage{
		ID:        event.ID,
		Content:   event.Content,
		AccountID: c.AccountID(),
		SenderID:  senderID,
		ChatID:    event.ChannelID,
		Channel:   c.Name(),
		Timestamp: time.Now(),
		Metadata: map[string]interface{}{
			"chat_type":  "channel",
			"channel_id": event.ChannelID,
			"group_id":   event.GuildID,
			"msg_id":     event.ID,
		},
	}

	logger.Debug("QQ Channel @message", zap.String("channel", event.ChannelID), zap.String("sender", senderID), zap.String("content", event.Content))
	_ = c.PublishInbound(context.Background(), msg)
}

// Send 发送消息
func (c *QQChannel) Send(msg *bus.OutboundMessage) error {
	if c.api == nil {
		return fmt.Errorf("QQ API not initialized")
	}

	ctx := context.Background()

	var err error
	// 先发送媒体文件
	if len(msg.Media) > 0 {
		err = c.sendMedias(ctx, msg)
		if err != nil {
			logger.Warn("QQ Channel send media err", zap.String("err", err.Error()))
		}
	}
	// 再发送文本内容
	if len(msg.Content) > 0 {
		err = c.sendMsg(ctx, msg.ChatID, msg.Content, msg.Metadata)
		if err != nil {
			logger.Warn("QQ Channel send msg err", zap.String("err", err.Error()))
		}
	}

	return err
}

func (c *QQChannel) sendMsg(ctx context.Context, chatID string, content string, meta map[string]interface{}) error {
	var replyID string

	sendFunc := c.sendC2CMessage
	// 判断消息类型并调用对应 API
	if meta != nil {
		if chatType, ok := meta["chat_type"].(string); ok {
			switch chatType {
			case "group":
				sendFunc = c.sendGroupMessage
			}
			replyID, _ = meta["msg_id"].(string)
		}
	}

	// 获取或递增 msg_seq
	msgSeq := c.getNextMsgSeq(chatID)

	markdownMsg := &dto.MessageToCreate{
		MsgType: dto.MarkdownMsg,
		Markdown: &dto.Markdown{
			Content: content,
		},
		MsgID:  replyID,
		MsgSeq: msgSeq,
	}

	textMsg := &dto.MessageToCreate{
		MsgType: dto.TextMsg,
		Content: content,
		MsgID:   replyID,
		MsgSeq:  msgSeq,
	}

	var err error
	for _, messageToSend := range []*dto.MessageToCreate{markdownMsg, textMsg} {
		_, err = sendFunc(ctx, chatID, messageToSend)
		if err == nil {
			return nil
		}
		logger.Error("QQ Channel send msg err", zap.String("err", err.Error()))
	}
	return err
}

// 发送媒体文件
func (c *QQChannel) sendMedias(ctx context.Context, msg *bus.OutboundMessage) error {
	if len(msg.Media) == 0 {
		return nil
	}

	var err error
	for _, media := range msg.Media {
		err = c.sendOneMedia(ctx, msg, media)
		if err != nil {
			logger.Debug("QQ Channel send media err", zap.String("err", err.Error()))
			// 将错误信息发送给用户
			c.sendMsg(ctx, msg.ChatID, err.Error(), msg.Metadata)
		}
	}
	return err
}

// sendOneMedia 发送单个媒体文件
func (c *QQChannel) sendOneMedia(ctx context.Context, msg *bus.OutboundMessage, media bus.Media) (err error) {
	chatKind := "c2c"

	sendFunc := c.sendC2CMessage
	var replyID string
	// 判断消息类型并调用对应 API
	if msg.Metadata != nil {
		if chatType, ok := msg.Metadata["chat_type"].(string); ok {
			chatKind = chatType
			switch chatType {
			case "group":
				sendFunc = c.sendGroupMessage
			case "channel":
				return fmt.Errorf("暂不支持频道文件发送")
			}
		}
		replyID, _ = msg.Metadata["msg_id"].(string)
	}

	mediaPath := media.URL
	// 根据媒体类型映射到QQ文件类型：1=图片，2=视频，3=音频，4=文件
	var fileType uint64
	switch media.Type {
	case "image":
		fileType = 1
	case "video":
		fileType = 2
	case "audio":
		fileType = 3
	default:
		fileType = 4 // 文件
	}

	richMedia := &RichMediaMessage{FileType: fileType}
	if isHTTPURL(mediaPath) {
		richMedia.URL = mediaPath
	} else {
		fdata, err := os.ReadFile(mediaPath)
		if err != nil {
			logger.Error("Failed to read media file", zap.String("path", mediaPath), zap.Error(err))
			return fmt.Errorf("读取本地文件[%v] 失败", mediaPath)
		}
		richMedia.FileData = fdata
		richMedia.FileName = filepath.Base(mediaPath)
	}

	// 群聊文件大小限制检查
	if chatKind == "group" && fileType == 4 {
		return fmt.Errorf("群对话暂不支持文件发送")
	}

	if len(richMedia.FileData) > 10*1024*1024 {
		logger.Warn("File size exceeds 10M, skipping send",
			zap.String("filename", richMedia.FileName),
			zap.Int("size", len(richMedia.FileData)))
		return fmt.Errorf("本地文件[%v] 大小超过10M, 发送失败", richMedia.FileName)
	}

	var sendErr error
	result, sendErr := sendFunc(ctx, msg.ChatID, richMedia)
	if sendErr != nil {
		logger.Warn("QQ send media failed",
			zap.String("type", media.Type),
			zap.String("chat_id", msg.ChatID),
			zap.Error(sendErr))
		return fmt.Errorf("上传文件[%v]失败[%v]", media.URL, sendErr.Error())
	}

	fileMsg := dto.MessageToCreate{
		MsgType: dto.RichMediaMsg,
		Media:   &dto.MediaInfo{FileInfo: result.FileInfo},
		MsgID:   replyID,
		MsgSeq:  c.getNextMsgSeq(msg.ChatID),
	}

	_, sendErr = sendFunc(ctx, msg.ChatID, fileMsg)
	if sendErr != nil {
		logger.Warn("QQ send media failed",
			zap.String("type", media.Type),
			zap.String("chat_id", msg.ChatID),
			zap.Error(sendErr))
		return fmt.Errorf("发送文件[%v]失败[%v]", media.URL, sendErr.Error())
	}
	logger.Debug("QQ media sent successfully",
		zap.String("chat_id", msg.ChatID),
		zap.String("type", media.Type))
	return sendErr
}

//// getChatKind 获取聊天类型（"group" 或 "direct"）
//func (c *QQChannel) getChatKind(chatID string) string {
//	// 这里需要根据实际情况实现聊天类型判断
//	// 暂时默认返回 "group"
//	return "group"
//}

// isHTTPURL 判断是否为HTTP/HTTPS URL
func isHTTPURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// GetMediaStore 获取媒体存储接口
func (c *QQChannel) GetMediaStore() interface{} {
	// 这里需要根据实际情况返回媒体存储接口
	// 暂时返回nil，需要根据项目结构实现
	// 注意：实际实现中需要返回一个具有Resolve方法的接口
	return nil
}

// sendC2CMessage 发送 C2C 消息
func (c *QQChannel) sendC2CMessage(ctx context.Context, openID string, msg dto.APIMessage) (*dto.Message, error) {
	return c.api.PostC2CMessage(ctx, openID, msg)
}

// sendGroupMessage 发送群消息
func (c *QQChannel) sendGroupMessage(ctx context.Context, groupID string, msg dto.APIMessage) (*dto.Message, error) {
	return c.api.PostGroupMessage(ctx, groupID, msg)
}

// sendChannelMessage 发送频道消息
func (c *QQChannel) sendChannelMessage(ctx context.Context, channelID string, msg *dto.MessageToCreate) (*dto.Message, error) {
	return c.api.PostMessage(ctx, channelID, msg)
}

// getNextMsgSeq 获取下一个消息序列号
func (c *QQChannel) getNextMsgSeq(chatID string) uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()

	seq := c.msgSeqMap[chatID] + 1
	if seq > math.MaxInt32 {
		seq = 1
	}
	c.msgSeqMap[chatID] = seq
	return seq
}

// Stop 停止 QQ 官方 Bot 通道
func (c *QQChannel) Stop() error {
	logger.Info("Stopping QQ Official Bot channel")

	// 停止 token 刷新
	if c.tokenCancel != nil {
		c.tokenCancel()
	}

	// 取消上下文，断开 WebSocket
	if c.cancel != nil {
		c.cancel()
	}

	// 关闭连接
	c.closeConnection()

	return c.BaseChannelImpl.Stop()
}

// closeConnection 关闭 WebSocket 连接
func (c *QQChannel) closeConnection() {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()

	if conn != nil {
		conn.Close()
	}
}

// HandleWebhook 处理 QQ Webhook 回调（WebSocket 模式下不使用）
func (c *QQChannel) HandleWebhook(ctx context.Context, event []byte) error {
	return nil
}

// GetSession 获取当前会话信息（用于调试）
func (c *QQChannel) GetSession() *dto.WebsocketAP {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.session
}
