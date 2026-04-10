// Package background implements a background task manager for long-running
// commands executed inside bot containers. It follows the same architectural
// pattern as Claude Code's LocalShellTask system:
//
//  1. Commands can be started in the background (fire-and-forget).
//  2. Output is collected asynchronously and written to a file in the container.
//  3. When a task completes, a structured Notification is enqueued.
//  4. The agent loop drains notifications at step boundaries and injects them
//     as context messages so the model learns about completed work.
//
// The manager is a server-level singleton, safe for concurrent use.
package background

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/memohai/memoh/internal/workspace/bridge"
)

const (
	// DefaultExecTimeout is the default timeout for foreground exec calls.
	DefaultExecTimeout int32 = 30
	// MaxExecTimeout is the maximum allowed timeout (10 minutes).
	MaxExecTimeout int32 = 600
	// BackgroundExecTimeout is the timeout for background tasks (30 minutes).
	BackgroundExecTimeout int32 = 1800
	// OutputLogDir is the directory inside the container where background
	// task output logs are written.
	OutputLogDir = "/tmp/memoh-bg"

	// stallCheckInterval is how often the stall watchdog checks output growth.
	stallCheckInterval = 5 * time.Second
	// stallThreshold is the duration of zero output growth before we consider
	// the command stalled and possibly waiting for interactive input.
	stallThreshold = 45 * time.Second
)

// ExecFunc executes a command in a container and returns the result.
// This is the signature that bridge.Client.Exec satisfies.
type ExecFunc func(ctx context.Context, command, workDir string, timeout int32) (*bridge.ExecResult, error)

// WriteFileFunc writes content to a file in the container.
type WriteFileFunc func(ctx context.Context, path string, data []byte) error

// Manager tracks background tasks and delivers completion notifications.
type Manager struct {
	mu            sync.Mutex
	tasks         map[string]*Task   // taskID -> Task
	notifications []Notification     // pending notifications, protected by mu
	seq           uint64             // monotonic task ID counter
	logger        *slog.Logger
}

// New creates a new background task Manager.
func New(logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		tasks:  make(map[string]*Task),
		logger: logger.With(slog.String("service", "background")),
	}
}

// Spawn starts a command in the background. It returns the task ID immediately.
// The command runs asynchronously; when it completes, a Notification is sent
// to the Notifications channel.
//
// execFn should call bridge.Client.Exec (or equivalent).
// writeFn should call bridge.Client.WriteFile to persist output logs.
func (m *Manager) Spawn(
	botID, sessionID, command, workDir, description string,
	execFn ExecFunc,
	writeFn WriteFileFunc,
) string {
	m.mu.Lock()
	m.seq++
	taskID := fmt.Sprintf("bg_%s_%d", botID[:min(8, len(botID))], m.seq)
	outputFile := fmt.Sprintf("%s/%s.log", OutputLogDir, taskID)

	task := &Task{
		ID:          taskID,
		BotID:       botID,
		SessionID:   sessionID,
		Command:     command,
		Description: description,
		WorkDir:     workDir,
		Status:      TaskRunning,
		OutputFile:  outputFile,
		StartedAt:   time.Now(),
	}
	m.tasks[taskID] = task
	m.mu.Unlock()

	m.logger.Info("background task spawned",
		slog.String("task_id", taskID),
		slog.String("bot_id", botID),
		slog.String("command", truncate(command, 120)),
	)

	go m.run(task, execFn, writeFn)
	return taskID
}

// SpawnAdopt registers a background task for a command that is already running
// externally (e.g. via ExecStream). Instead of re-executing the command, it
// waits for the result on the provided channel. This enables "flip to background"
// where a foreground stream is handed off without killing the process.
func (m *Manager) SpawnAdopt(
	botID, sessionID, command, workDir, description string,
	resultCh <-chan AdoptResult,
	writeFn WriteFileFunc,
) (taskID, outputFile string) {
	m.mu.Lock()
	m.seq++
	taskID = fmt.Sprintf("bg_%s_%d", botID[:min(8, len(botID))], m.seq)
	outputFile = fmt.Sprintf("%s/%s.log", OutputLogDir, taskID)

	task := &Task{
		ID:          taskID,
		BotID:       botID,
		SessionID:   sessionID,
		Command:     command,
		Description: description,
		WorkDir:     workDir,
		Status:      TaskRunning,
		OutputFile:  outputFile,
		StartedAt:   time.Now(),
	}
	m.tasks[taskID] = task
	m.mu.Unlock()

	m.logger.Info("background task adopted",
		slog.String("task_id", taskID),
		slog.String("bot_id", botID),
		slog.String("command", truncate(command, 120)),
	)

	go m.runAdopt(task, resultCh, writeFn)
	return taskID, outputFile
}

// runAdopt waits for the adopted stream result and handles completion.
func (m *Manager) runAdopt(task *Task, resultCh <-chan AdoptResult, writeFn WriteFileFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(BackgroundExecTimeout)*time.Second)
	task.mu.Lock()
	task.cancel = cancel
	task.mu.Unlock()
	defer cancel()

	// Ensure output directory exists.
	_ = m.ensureOutputDir(ctx, task, writeFn)

	// Start stall watchdog.
	go m.stallWatchdog(ctx, task)

	// Wait for the result from the already-running stream.
	var result AdoptResult
	select {
	case result = <-resultCh:
	case <-ctx.Done():
		result = AdoptResult{Err: ctx.Err()}
	}

	// Collect output.
	if result.Err != nil {
		task.AppendOutput(fmt.Sprintf("[error] %v\n", result.Err))
	} else {
		task.AppendOutput(result.Stdout)
		if result.Stderr != "" {
			task.AppendOutput(result.Stderr)
		}
	}

	// Write output to log file in container.
	if writeFn != nil && result.Err == nil {
		combined := result.Stdout
		if result.Stderr != "" {
			combined += "\n--- stderr ---\n" + result.Stderr
		}
		_ = writeFn(context.Background(), task.OutputFile, []byte(combined))
	}

	task.mu.Lock()
	if task.Status == TaskKilled {
		task.mu.Unlock()
		return
	}
	task.CompletedAt = time.Now()
	if result.Err != nil {
		task.Status = TaskFailed
		task.ExitCode = -1
	} else {
		task.ExitCode = result.ExitCode
		if result.ExitCode == 0 {
			task.Status = TaskCompleted
		} else {
			task.Status = TaskFailed
		}
	}
	status := task.Status
	exitCode := task.ExitCode
	task.mu.Unlock()

	duration := task.CompletedAt.Sub(task.StartedAt)
	m.logger.Info("adopted background task finished",
		slog.String("task_id", task.ID),
		slog.String("status", string(status)),
		slog.Int("exit_code", int(exitCode)),
		slog.Duration("duration", duration),
	)

	if !task.MarkNotified() {
		return
	}

	n := Notification{
		TaskID:      task.ID,
		BotID:       task.BotID,
		SessionID:   task.SessionID,
		Status:      status,
		Command:     task.Command,
		Description: task.Description,
		ExitCode:    exitCode,
		OutputFile:  task.OutputFile,
		OutputTail:  task.OutputTail(),
		Duration:    duration,
	}

	m.mu.Lock()
	m.notifications = append(m.notifications, n)
	m.mu.Unlock()
}

func (m *Manager) run(task *Task, execFn ExecFunc, writeFn WriteFileFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(BackgroundExecTimeout)*time.Second)
	task.mu.Lock()
	task.cancel = cancel
	task.mu.Unlock()
	defer cancel()

	// Ensure output directory exists.
	_ = m.ensureOutputDir(ctx, task, writeFn)

	// Start stall watchdog to detect commands waiting for interactive input.
	go m.stallWatchdog(ctx, task)

	// Wrap command to tee output to the log file inside the container.
	// This way, even if the agent wants to read partial output mid-run,
	// it can use the read tool on the output file.
	wrappedCmd := fmt.Sprintf(
		"{ %s ; } 2>&1 | tee -a %s",
		task.Command, task.OutputFile,
	)

	result, err := execFn(ctx, wrappedCmd, task.WorkDir, BackgroundExecTimeout)

	// Collect output before taking the lock (AppendOutput also locks).
	if err != nil {
		task.AppendOutput(fmt.Sprintf("[error] %v\n", err))
	} else {
		task.AppendOutput(result.Stdout)
		if result.Stderr != "" {
			task.AppendOutput(result.Stderr)
		}
	}

	task.mu.Lock()
	// If the task was already killed, don't overwrite its status.
	if task.Status == TaskKilled {
		task.mu.Unlock()
		return
	}
	task.CompletedAt = time.Now()
	if err != nil {
		task.Status = TaskFailed
		task.ExitCode = -1
	} else {
		task.ExitCode = result.ExitCode
		if result.ExitCode == 0 {
			task.Status = TaskCompleted
		} else {
			task.Status = TaskFailed
		}
	}
	status := task.Status
	exitCode := task.ExitCode
	task.mu.Unlock()

	duration := task.CompletedAt.Sub(task.StartedAt)
	m.logger.Info("background task finished",
		slog.String("task_id", task.ID),
		slog.String("status", string(status)),
		slog.Int("exit_code", int(exitCode)),
		slog.Duration("duration", duration),
	)

	// Enqueue notification unless already notified (e.g. by Kill or auto-background race).
	if !task.MarkNotified() {
		return
	}

	n := Notification{
		TaskID:      task.ID,
		BotID:       task.BotID,
		SessionID:   task.SessionID,
		Status:      status,
		Command:     task.Command,
		Description: task.Description,
		ExitCode:    exitCode,
		OutputFile:  task.OutputFile,
		OutputTail:  task.OutputTail(),
		Duration:    duration,
	}

	m.mu.Lock()
	m.notifications = append(m.notifications, n)
	m.mu.Unlock()
}

func (m *Manager) ensureOutputDir(ctx context.Context, task *Task, writeFn WriteFileFunc) error {
	if writeFn == nil {
		return nil
	}
	// Create a marker file to ensure the directory exists.
	return writeFn(ctx, OutputLogDir+"/.keep", []byte(""))
}

// Kill cancels a running background task.
func (m *Manager) Kill(taskID string) error {
	m.mu.Lock()
	task, ok := m.tasks[taskID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	task.mu.Lock()
	if task.Status != TaskRunning {
		task.mu.Unlock()
		return fmt.Errorf("task %s is not running (status: %s)", taskID, task.Status)
	}
	task.Status = TaskKilled
	task.CompletedAt = time.Now()
	task.mu.Unlock()

	task.Cancel()
	m.logger.Info("background task killed", slog.String("task_id", taskID))
	return nil
}

// Get returns a task by ID, or nil if not found.
func (m *Manager) Get(taskID string) *Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tasks[taskID]
}

// ListForSession returns all tasks for a given bot+session, most recent first.
func (m *Manager) ListForSession(botID, sessionID string) []*Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*Task
	for _, t := range m.tasks {
		if t.BotID == botID && t.SessionID == sessionID {
			result = append(result, t)
		}
	}
	return result
}

// DrainNotifications returns all pending notifications for a given
// bot+session without blocking. Used by the resolver to inject
// notifications at the start of a new agent run.
func (m *Manager) DrainNotifications(botID, sessionID string) []Notification {
	m.mu.Lock()
	defer m.mu.Unlock()

	var matched []Notification
	remaining := m.notifications[:0] // reuse backing array
	for _, n := range m.notifications {
		if n.BotID == botID && n.SessionID == sessionID {
			matched = append(matched, n)
		} else {
			remaining = append(remaining, n)
		}
	}
	m.notifications = remaining
	return matched
}

// RunningTasksSummary returns a text summary of currently running tasks
// for a given bot+session. This is injected into the system prompt so the
// agent knows about ongoing background work.
func (m *Manager) RunningTasksSummary(botID, sessionID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var lines []string
	for _, t := range m.tasks {
		if t.BotID == botID && t.SessionID == sessionID && t.Status == TaskRunning {
			desc := t.Description
			if desc == "" {
				desc = truncate(t.Command, 80)
			}
			lines = append(lines, fmt.Sprintf("- [%s] %s (started %s ago, output: %s)",
				t.ID, desc,
				time.Since(t.StartedAt).Round(time.Second),
				t.OutputFile,
			))
		}
	}
	if len(lines) == 0 {
		return ""
	}
	return "Currently running background tasks:\n" + joinLines(lines)
}

// Cleanup removes completed tasks older than the given duration.
func (m *Manager) Cleanup(maxAge time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := time.Now().Add(-maxAge)
	for id, t := range m.tasks {
		if t.Status != TaskRunning && t.CompletedAt.Before(cutoff) {
			delete(m.tasks, id)
		}
	}
}

// promptPatterns matches common interactive prompt endings that indicate
// a command is waiting for user input.
var promptPatterns = regexp.MustCompile(
	`(?i)(\$ ?$|> ?$|# ?$|password\s*:|passphrase\s*:|y/n\]|yes/no\)|enter .*:|Press .* to continue|Are you sure|Continue\?|Proceed\?)`,
)

// stallWatchdog monitors a background task's output for stalls that might
// indicate the command is waiting for interactive input. If detected, it
// enqueues a notification advising the agent to kill and retry.
func (m *Manager) stallWatchdog(ctx context.Context, task *Task) {
	ticker := time.NewTicker(stallCheckInterval)
	defer ticker.Stop()

	var lastLen int
	var stalledSince time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		task.mu.Lock()
		if task.Status != TaskRunning {
			task.mu.Unlock()
			return
		}
		currentLen := task.output.Len()
		// Read tail inline (we already hold the lock).
		tail := task.output.String()
		if len(tail) > maxTailBytes {
			tail = tail[len(tail)-maxTailBytes:]
		}
		task.mu.Unlock()

		if currentLen != lastLen {
			// Output is still growing — reset stall timer.
			lastLen = currentLen
			stalledSince = time.Time{}
			continue
		}

		// Output hasn't grown.
		if stalledSince.IsZero() {
			stalledSince = time.Now()
			continue
		}

		if time.Since(stalledSince) < stallThreshold {
			continue
		}

		// Stalled long enough. Check if the tail looks like an interactive prompt.
		if !promptPatterns.MatchString(tail) {
			continue
		}

		m.logger.Warn("background task appears stalled on interactive prompt",
			slog.String("task_id", task.ID),
		)

		// Enqueue a stall notification (only once).
		if !task.MarkNotified() {
			return
		}

		n := Notification{
			TaskID:      task.ID,
			BotID:       task.BotID,
			SessionID:   task.SessionID,
			Status:      TaskRunning, // still running, but stalled
			Command:     task.Command,
			Description: task.Description,
			ExitCode:    0,
			OutputFile:  task.OutputFile,
			OutputTail:  tail,
			Duration:    time.Since(task.StartedAt),
			Stalled:     true,
		}

		m.mu.Lock()
		m.notifications = append(m.notifications, n)
		m.mu.Unlock()
		return // only notify once per task
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func joinLines(lines []string) string {
	result := ""
	for _, l := range lines {
		result += l + "\n"
	}
	return result
}
