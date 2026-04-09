package background

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// TaskStatus represents the lifecycle state of a background task.
type TaskStatus string

const (
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskKilled    TaskStatus = "killed"
)

// Task represents a single background command execution.
type Task struct {
	ID          string
	BotID       string
	SessionID   string
	Command     string
	Description string
	WorkDir     string
	Status      TaskStatus
	ExitCode    int32
	OutputFile  string // path inside container where output is being written
	StartedAt   time.Time
	CompletedAt time.Time

	mu     sync.Mutex
	cancel context.CancelFunc
	output strings.Builder // buffered output tail
}

// Cancel requests cancellation of the task's context.
func (t *Task) Cancel() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancel != nil {
		t.cancel()
	}
}

// AppendOutput appends text to the buffered output tail.
// Only the last maxTailBytes are kept.
func (t *Task) AppendOutput(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.output.WriteString(s)
	// Keep tail bounded
	if t.output.Len() > maxTailBytes*2 {
		tail := t.output.String()
		t.output.Reset()
		if len(tail) > maxTailBytes {
			t.output.WriteString(tail[len(tail)-maxTailBytes:])
		} else {
			t.output.WriteString(tail)
		}
	}
}

// OutputTail returns the last portion of collected output.
func (t *Task) OutputTail() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.output.String()
	if len(s) > maxTailBytes {
		return s[len(s)-maxTailBytes:]
	}
	return s
}

const maxTailBytes = 4096

// Notification is the structured event sent to the agent when a background
// task reaches a terminal state.
type Notification struct {
	TaskID      string
	BotID       string
	SessionID   string
	Status      TaskStatus
	Command     string
	Description string
	ExitCode    int32
	OutputFile  string
	OutputTail  string // last N bytes of output for quick summary
	Duration    time.Duration
}

// FormatForAgent returns a human-readable task-notification block that can be
// injected into the agent's message stream, following the same pattern as
// Claude Code's <task-notification>.
func (n Notification) FormatForAgent() string {
	var b strings.Builder
	fmt.Fprintf(&b, "<task-notification>\n")
	fmt.Fprintf(&b, "  <task-id>%s</task-id>\n", n.TaskID)
	fmt.Fprintf(&b, "  <status>%s</status>\n", n.Status)
	fmt.Fprintf(&b, "  <command>%s</command>\n", n.Command)
	if n.Description != "" {
		fmt.Fprintf(&b, "  <description>%s</description>\n", n.Description)
	}
	fmt.Fprintf(&b, "  <exit-code>%d</exit-code>\n", n.ExitCode)
	fmt.Fprintf(&b, "  <duration>%s</duration>\n", n.Duration.Round(time.Millisecond))
	if n.OutputFile != "" {
		fmt.Fprintf(&b, "  <output-file>%s</output-file>\n", n.OutputFile)
	}
	if n.OutputTail != "" {
		fmt.Fprintf(&b, "  <output-tail>\n%s\n  </output-tail>\n", strings.TrimRight(n.OutputTail, "\n"))
	}
	fmt.Fprintf(&b, "</task-notification>")
	return b.String()
}
