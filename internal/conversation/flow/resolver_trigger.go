package flow

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	sdk "github.com/memohai/twilight-ai/sdk"

	agentpkg "github.com/memohai/memoh/internal/agent"
	"github.com/memohai/memoh/internal/conversation"
	"github.com/memohai/memoh/internal/heartbeat"
	"github.com/memohai/memoh/internal/schedule"
)

// TriggerSchedule executes a scheduled command via the internal agent.
func (r *Resolver) TriggerSchedule(ctx context.Context, botID string, payload schedule.TriggerPayload, token string) (schedule.TriggerResult, error) {
	if strings.TrimSpace(botID) == "" {
		return schedule.TriggerResult{}, errors.New("bot id is required")
	}
	if strings.TrimSpace(payload.Command) == "" {
		return schedule.TriggerResult{}, errors.New("schedule command is required")
	}

	req := conversation.ChatRequest{
		BotID:     botID,
		ChatID:    botID,
		SessionID: payload.SessionID,
		Query:     payload.Command,
		UserID:    payload.OwnerUserID,
		Token:     token,
	}
	rc, err := r.resolve(ctx, req)
	if err != nil {
		return schedule.TriggerResult{}, err
	}

	cfg := rc.runConfig
	cfg.SessionType = "schedule"
	cfg.Identity.ChannelIdentityID = strings.TrimSpace(payload.OwnerUserID)

	schedulePrompt := agentpkg.GenerateSchedulePrompt(agentpkg.Schedule{
		ID:          payload.ID,
		Name:        payload.Name,
		Description: payload.Description,
		Pattern:     payload.Pattern,
		MaxCalls:    payload.MaxCalls,
		Command:     payload.Command,
	})
	cfg.Messages = append(cfg.Messages, sdk.UserMessage(schedulePrompt))
	cfg = r.prepareRunConfig(ctx, cfg)

	result, err := r.agent.Generate(ctx, cfg)
	if err != nil {
		return schedule.TriggerResult{}, err
	}

	outputMessages := sdkMessagesToModelMessages(result.Messages)
	roundMessages := prependUserMessage(req.Query, outputMessages)
	storeErr := r.storeRound(ctx, req, roundMessages, rc.model.ID)

	totalUsageJSON, _ := json.Marshal(result.Usage)
	return schedule.TriggerResult{
		Status:     "ok",
		Text:       strings.TrimSpace(result.Text),
		UsageBytes: totalUsageJSON,
		ModelID:    rc.model.ID,
	}, storeErr
}

// TriggerHeartbeat executes a heartbeat check via the internal agent.
func (r *Resolver) TriggerHeartbeat(ctx context.Context, botID string, payload heartbeat.TriggerPayload, token string) (heartbeat.TriggerResult, error) {
	if strings.TrimSpace(botID) == "" {
		return heartbeat.TriggerResult{}, errors.New("bot id is required")
	}

	var heartbeatModel string
	if botSettings, err := r.loadBotSettings(ctx, botID); err == nil {
		heartbeatModel = strings.TrimSpace(botSettings.HeartbeatModelID)
	}

	req := conversation.ChatRequest{
		BotID:     botID,
		ChatID:    botID,
		SessionID: payload.SessionID,
		Query:     "heartbeat",
		UserID:    payload.OwnerUserID,
		Token:     token,
		Model:     heartbeatModel,
	}
	rc, err := r.resolve(ctx, req)
	if err != nil {
		return heartbeat.TriggerResult{}, err
	}

	cfg := rc.runConfig
	cfg.SessionType = "heartbeat"
	cfg.Identity.ChannelIdentityID = strings.TrimSpace(payload.OwnerUserID)

	var checklist string
	if r.agent != nil {
		nowFn := time.Now
		if cfg.Identity.TimezoneLocation != nil {
			nowFn = func() time.Time { return time.Now().In(cfg.Identity.TimezoneLocation) }
		}
		fs := agentpkg.NewFSClient(r.agent.BridgeProvider(), botID, nowFn)
		checklist = fs.ReadTextSafe(ctx, "/data/HEARTBEAT.md")
	}
	now := time.Now().UTC()
	if cfg.Identity.TimezoneLocation != nil {
		now = now.In(cfg.Identity.TimezoneLocation)
	}
	heartbeatPrompt := agentpkg.GenerateHeartbeatPrompt(payload.Interval, checklist, now, payload.LastHeartbeatAt)
	cfg.Messages = append(cfg.Messages, sdk.UserMessage(heartbeatPrompt))
	cfg = r.prepareRunConfig(ctx, cfg)

	result, err := r.agent.Generate(ctx, cfg)
	if err != nil {
		return heartbeat.TriggerResult{}, err
	}

	status := "alert"
	text := strings.TrimSpace(result.Text)
	if isHeartbeatOK(text) {
		status = "ok"
	}

	outputMessages := sdkMessagesToModelMessages(result.Messages)
	roundMessages := prependUserMessage(heartbeatPrompt, outputMessages)
	_ = r.storeRound(ctx, req, roundMessages, rc.model.ID)

	totalUsageJSON, _ := json.Marshal(result.Usage)
	return heartbeat.TriggerResult{
		Status:     status,
		Text:       text,
		Usage:      totalUsageJSON,
		UsageBytes: totalUsageJSON,
		ModelID:    rc.model.ID,
		SessionID:  payload.SessionID,
	}, nil
}

func isHeartbeatOK(text string) bool {
	t := strings.TrimSpace(text)
	return strings.HasPrefix(t, "HEARTBEAT_OK") || strings.HasSuffix(t, "HEARTBEAT_OK") || t == "HEARTBEAT_OK"
}


// TriggerBackgroundNotification is called when a background task completes.
// It drains all pending notifications for the session and groups them by
// (CurrentPlatform, ReplyTarget) — each group gets its own agent call so
// every notification is delivered to the exact channel it originated from,
// even if multiple tasks in the same session were spawned from different channels.
func (r *Resolver) TriggerBackgroundNotification(ctx context.Context, botID, sessionID string) {
	r.logger.Info("background notification trigger called",
		slog.String("bot_id", botID),
		slog.String("session_id", sessionID),
	)
	if strings.TrimSpace(botID) == "" || strings.TrimSpace(sessionID) == "" {
		return
	}
	if r.bgManager == nil {
		return
	}

	// Per-session concurrency guard: only one notification delivery loop may
	// run at a time for a given (botID, sessionID). If a loop is already
	// active, skip — the running loop's prepareStep hook or the drain-loop
	// below will pick up any new notifications. This prevents the "agent
	// storm" where N concurrent Generate() calls each spawn more tasks,
	// creating exponential growth.
	key := botID + ":" + sessionID
	if _, loaded := r.bgNotifActive.LoadOrStore(key, true); loaded {
		r.logger.Info("background notification trigger: already active, skipping",
			slog.String("bot_id", botID),
			slog.String("session_id", sessionID),
		)
		return
	}
	defer r.bgNotifActive.Delete(key)

	// Drain loop: after delivering one batch, check for new notifications
	// that arrived during the Generate() call (e.g. another task completed
	// while the agent was responding to the first). Capped to prevent
	// runaway loops if the agent keeps spawning tasks in its response.
	const maxRounds = 10
	for round := 0; round < maxRounds; round++ {
		n := r.bgManager.DrainOneNotification(botID, sessionID)
		if n == nil {
			return
		}

		var prefix string
		if n.Stalled {
			prefix = "A background task appears stuck and may need attention:"
		} else {
			prefix = "A background task completed:"
		}
		msgs := []sdk.Message{sdk.UserMessage(prefix + "\n" + n.FormatForAgent())}
		r.deliverBackgroundNotifications(ctx, botID, sessionID, n.CurrentPlatform, n.ReplyTarget, msgs)
	}
	r.logger.Warn("background notification trigger: max rounds reached",
		slog.String("bot_id", botID),
		slog.String("session_id", sessionID),
		slog.Int("max_rounds", maxRounds),
	)
}

// deliverBackgroundNotifications runs a single agent call to deliver a batch of
// background-task notifications to a specific (platform, replyTarget) channel.
func (r *Resolver) deliverBackgroundNotifications(ctx context.Context, botID, sessionID, currentPlatform, replyTarget string, notifMessages []sdk.Message) {
	r.logger.Info("background notification delivery",
		slog.String("bot_id", botID),
		slog.String("session_id", sessionID),
		slog.String("platform", currentPlatform),
		slog.String("reply_target", replyTarget),
		slog.Int("count", len(notifMessages)),
	)
	// Use replyTarget as ChatID so history is loaded for this specific chat
	// (e.g. private chat vs group chat), not the bot-wide catch-all. Without
	// this, the agent sees recent messages from other chats in the same session
	// and may call send with the wrong target.
	chatID := strings.TrimSpace(replyTarget)
	if chatID == "" {
		chatID = botID
	}
	req := conversation.ChatRequest{
		BotID:          botID,
		ChatID:         chatID,
		SessionID:      sessionID,
		Query:          "[background notification]",
		CurrentChannel: currentPlatform,
		ReplyTarget:    replyTarget,
	}
	rc, err := r.resolve(ctx, req)
	if err != nil {
		r.logger.Warn("background notification trigger: resolve failed",
			slog.String("bot_id", botID),
			slog.String("session_id", sessionID),
			slog.String("platform", currentPlatform),
			slog.String("reply_target", replyTarget),
			slog.Any("error", err),
		)
		return
	}

	cfg := rc.runConfig
	// Inject drained notifications so the first LLM call sees them.
	cfg.Messages = append(cfg.Messages, notifMessages...)
	// Clear query so prepareRunConfig does not append a redundant user message.
	cfg.Query = ""
	// Use the natural session type — same system prompt, same tools, same
	// personality as a regular conversation turn. This matches Claude Code's
	// design where between-turn notifications go through the same query()
	// function as normal user messages.
	cfg = r.prepareRunConfig(ctx, cfg)

	result, err := r.agent.Generate(ctx, cfg)
	if err != nil {
		r.logger.Warn("background notification trigger: generate failed",
			slog.String("bot_id", botID),
			slog.String("session_id", sessionID),
			slog.String("platform", currentPlatform),
			slog.String("reply_target", replyTarget),
			slog.Any("error", err),
		)
		return
	}
	r.logger.Info("background notification trigger: generate ok",
		slog.String("bot_id", botID),
		slog.String("platform", currentPlatform),
		slog.String("reply_target", replyTarget),
		slog.Int("messages", len(result.Messages)),
	)

	if len(result.Messages) > 0 {
		outputMessages := sdkMessagesToModelMessages(result.Messages)
		notifModelMessages := sdkMessagesToModelMessages(notifMessages)
		roundMessages := append(notifModelMessages, outputMessages...)
		_ = r.storeRound(ctx, req, roundMessages, rc.model.ID)
	}

	// Auto-deliver the agent's text response to the user — mirrors Claude
	// Code where the agent's output is displayed through the normal REPL
	// output path, not through a special "send" tool call.
	if text := strings.TrimSpace(result.Text); text != "" && r.outboundFn != nil {
		if err := r.outboundFn(ctx, botID, currentPlatform, replyTarget, text); err != nil {
			r.logger.Warn("background notification: outbound delivery failed",
				slog.String("bot_id", botID),
				slog.String("platform", currentPlatform),
				slog.String("reply_target", replyTarget),
				slog.Any("error", err),
			)
		}
	}
}
