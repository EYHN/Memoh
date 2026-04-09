package background

import (
	"context"
	"testing"
	"time"

	"github.com/memohai/memoh/internal/workspace/bridge"
)

func TestSpawnAndNotify(t *testing.T) {
	mgr := New(nil)

	called := make(chan struct{})
	execFn := func(_ context.Context, cmd, wd string, timeout int32) (*bridge.ExecResult, error) {
		close(called)
		return &bridge.ExecResult{Stdout: "hello world\n", ExitCode: 0}, nil
	}

	taskID := mgr.Spawn("bot1", "sess1", "echo hello", "/data", "test echo", execFn, nil)

	if taskID == "" {
		t.Fatal("expected non-empty task ID")
	}

	// Wait for exec to be called.
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("execFn was not called within timeout")
	}

	// Wait for notification.
	select {
	case n := <-mgr.Notifications():
		if n.TaskID != taskID {
			t.Errorf("expected task ID %s, got %s", taskID, n.TaskID)
		}
		if n.Status != TaskCompleted {
			t.Errorf("expected status completed, got %s", n.Status)
		}
		if n.ExitCode != 0 {
			t.Errorf("expected exit code 0, got %d", n.ExitCode)
		}
		if n.BotID != "bot1" || n.SessionID != "sess1" {
			t.Errorf("unexpected bot/session: %s/%s", n.BotID, n.SessionID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no notification received within timeout")
	}

	// Verify task state.
	task := mgr.Get(taskID)
	if task == nil {
		t.Fatal("task not found after completion")
	}
	if task.Status != TaskCompleted {
		t.Errorf("expected task status completed, got %s", task.Status)
	}
}

func TestSpawnFailedCommand(t *testing.T) {
	mgr := New(nil)

	execFn := func(_ context.Context, cmd, wd string, timeout int32) (*bridge.ExecResult, error) {
		return &bridge.ExecResult{
			Stdout:   "some output\n",
			Stderr:   "error: not found\n",
			ExitCode: 1,
		}, nil
	}

	taskID := mgr.Spawn("bot1", "sess1", "false", "/data", "failing cmd", execFn, nil)

	select {
	case n := <-mgr.Notifications():
		if n.TaskID != taskID {
			t.Errorf("expected task ID %s, got %s", taskID, n.TaskID)
		}
		if n.Status != TaskFailed {
			t.Errorf("expected status failed, got %s", n.Status)
		}
		if n.ExitCode != 1 {
			t.Errorf("expected exit code 1, got %d", n.ExitCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no notification received within timeout")
	}
}

func TestKillTask(t *testing.T) {
	mgr := New(nil)

	started := make(chan struct{})
	execFn := func(ctx context.Context, cmd, wd string, timeout int32) (*bridge.ExecResult, error) {
		close(started)
		<-ctx.Done()
		return &bridge.ExecResult{ExitCode: -1}, ctx.Err()
	}

	taskID := mgr.Spawn("bot1", "sess1", "sleep 300", "/data", "long task", execFn, nil)

	// Wait for the task to start.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not start within timeout")
	}

	if err := mgr.Kill(taskID); err != nil {
		t.Fatalf("kill failed: %v", err)
	}

	task := mgr.Get(taskID)
	if task == nil {
		t.Fatal("task not found")
	}
	if task.Status != TaskKilled {
		t.Errorf("expected status killed, got %s", task.Status)
	}
}

func TestListForSession(t *testing.T) {
	mgr := New(nil)

	started := make(chan struct{}, 2)
	execFn := func(ctx context.Context, cmd, wd string, timeout int32) (*bridge.ExecResult, error) {
		started <- struct{}{}
		<-ctx.Done()
		return &bridge.ExecResult{ExitCode: -1}, ctx.Err()
	}

	mgr.Spawn("bot1", "sess1", "cmd1", "/data", "d1", execFn, nil)
	mgr.Spawn("bot1", "sess1", "cmd2", "/data", "d2", execFn, nil)
	mgr.Spawn("bot2", "sess2", "cmd3", "/data", "d3", execFn, nil)

	// Wait for all to start.
	for range 3 {
		<-started
	}

	tasks := mgr.ListForSession("bot1", "sess1")
	if len(tasks) != 2 {
		t.Errorf("expected 2 tasks for bot1/sess1, got %d", len(tasks))
	}

	tasks = mgr.ListForSession("bot2", "sess2")
	if len(tasks) != 1 {
		t.Errorf("expected 1 task for bot2/sess2, got %d", len(tasks))
	}
}

func TestDrainNotifications(t *testing.T) {
	mgr := New(nil)

	execFn := func(_ context.Context, cmd, wd string, timeout int32) (*bridge.ExecResult, error) {
		return &bridge.ExecResult{Stdout: "ok\n", ExitCode: 0}, nil
	}

	mgr.Spawn("bot1", "sess1", "echo 1", "/data", "", execFn, nil)
	mgr.Spawn("bot1", "sess2", "echo 2", "/data", "", execFn, nil)
	mgr.Spawn("bot2", "sess1", "echo 3", "/data", "", execFn, nil)

	// Wait for all to complete.
	time.Sleep(500 * time.Millisecond)

	// Drain only bot1/sess1.
	notifications := mgr.DrainNotifications("bot1", "sess1")
	if len(notifications) != 1 {
		t.Errorf("expected 1 notification for bot1/sess1, got %d", len(notifications))
	}

	// The other two should still be in the channel.
	notifications = mgr.DrainNotifications("bot1", "sess2")
	if len(notifications) != 1 {
		t.Errorf("expected 1 notification for bot1/sess2, got %d", len(notifications))
	}
}

func TestNotificationFormat(t *testing.T) {
	n := Notification{
		TaskID:      "bg_test_1",
		Status:      TaskCompleted,
		Command:     "npm install",
		Description: "Install dependencies",
		ExitCode:    0,
		OutputFile:  "/tmp/memoh-bg/bg_test_1.log",
		OutputTail:  "added 1337 packages\n",
		Duration:    45 * time.Second,
	}

	text := n.FormatForAgent()
	if text == "" {
		t.Fatal("expected non-empty notification text")
	}
	for _, want := range []string{
		"<task-notification>",
		"bg_test_1",
		"completed",
		"npm install",
		"Install dependencies",
		"/tmp/memoh-bg/bg_test_1.log",
		"added 1337 packages",
		"</task-notification>",
	} {
		if !contains(text, want) {
			t.Errorf("notification text missing %q:\n%s", want, text)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
