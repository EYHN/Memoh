package tools

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"

	sdk "github.com/memohai/twilight-ai/sdk"

	"github.com/memohai/memoh/internal/agent/background"
	"github.com/memohai/memoh/internal/workspace/bridge"
)

const defaultContainerExecWorkDir = "/data"

type ContainerProvider struct {
	clients     bridge.Provider
	bgManager   *background.Manager
	execWorkDir string
	logger      *slog.Logger
}

func NewContainerProvider(log *slog.Logger, clients bridge.Provider, bgManager *background.Manager, execWorkDir string) *ContainerProvider {
	if log == nil {
		log = slog.Default()
	}
	wd := strings.TrimSpace(execWorkDir)
	if wd == "" {
		wd = defaultContainerExecWorkDir
	}
	return &ContainerProvider{clients: clients, bgManager: bgManager, execWorkDir: wd, logger: log.With(slog.String("tool", "container"))}
}

func (p *ContainerProvider) Tools(_ context.Context, session SessionContext) ([]sdk.Tool, error) {
	wd := p.execWorkDir
	sess := session

	readDesc := fmt.Sprintf("Read file content inside the bot container. Supports pagination for large files. Max %d lines / %d bytes per call.", readMaxLines, readMaxBytes)
	if sess.SupportsImageInput {
		readDesc += " Also supports reading image files (PNG, JPEG, GIF, WebP) — binary images are loaded into model context automatically."
	}

	return []sdk.Tool{
		{
			Name:        "read",
			Description: readDesc,
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":        map[string]any{"type": "string", "description": fmt.Sprintf("File path (relative to %s or absolute inside container)", wd)},
					"line_offset": map[string]any{"type": "integer", "description": "Line number to start reading from (1-indexed). Default: 1.", "minimum": 1, "default": 1},
					"n_lines":     map[string]any{"type": "integer", "description": fmt.Sprintf("Number of lines to read per call. Default: %d. Max: %d.", readMaxLines, readMaxLines), "minimum": 1, "maximum": readMaxLines, "default": readMaxLines},
				},
				"required": []string{"path"},
			},
			Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) {
				return p.execRead(ctx.Context, sess, inputAsMap(input))
			},
		},
		{
			Name:        "write",
			Description: "Write file content inside the bot container.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":    map[string]any{"type": "string", "description": fmt.Sprintf("File path (relative to %s or absolute inside container)", wd)},
					"content": map[string]any{"type": "string", "description": "File content"},
				},
				"required": []string{"path", "content"},
			},
			Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) {
				return p.execWrite(ctx.Context, sess, inputAsMap(input))
			},
		},
		{
			Name:        "list",
			Description: fmt.Sprintf("List directory entries inside the bot container. Supports pagination. Max %d entries per call. In recursive mode, subdirectories with >%d items are collapsed to a summary.", listMaxEntries, listCollapseThreshold),
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":      map[string]any{"type": "string", "description": fmt.Sprintf("Directory path (relative to %s or absolute inside container)", wd)},
					"recursive": map[string]any{"type": "boolean", "description": "List recursively"},
					"offset":    map[string]any{"type": "integer", "description": "Entry offset to start from (0-indexed). Default: 0.", "minimum": 0, "default": 0},
					"limit":     map[string]any{"type": "integer", "description": fmt.Sprintf("Max entries to return per call. Default: %d. Max: %d.", listMaxEntries, listMaxEntries), "minimum": 1, "maximum": listMaxEntries, "default": listMaxEntries},
				},
				"required": []string{"path"},
			},
			Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) {
				return p.execList(ctx.Context, sess, inputAsMap(input))
			},
		},
		{
			Name:        "edit",
			Description: "Replace exact text in a file inside the bot container.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":     map[string]any{"type": "string", "description": fmt.Sprintf("File path (relative to %s or absolute inside container)", wd)},
					"old_text": map[string]any{"type": "string", "description": "Exact text to find"},
					"new_text": map[string]any{"type": "string", "description": "Replacement text"},
				},
				"required": []string{"path", "old_text", "new_text"},
			},
			Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) {
				return p.execEdit(ctx.Context, sess, inputAsMap(input))
			},
		},
		{
			Name: "exec",
			Description: fmt.Sprintf(`Execute a shell command in the bot container. Runs in the bot's data directory (%s) by default.

# Instructions
- Use this tool to run shell commands for installing packages, running scripts, building code, running tests, and other system operations.
- If your command will take a long time (package installs, builds, test suites), set run_in_background to true. You will be notified when it completes.
- If waiting for a background task, you will be notified when it completes — do NOT poll or sleep.
- You may specify a custom timeout (up to %d seconds) for commands you know will take longer than the default %d seconds.
- Avoid unnecessary sleep commands — if you need to wait for a background task, you will be notified automatically.`, wd, background.MaxExecTimeout, background.DefaultExecTimeout),
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"command":           map[string]any{"type": "string", "description": "Shell command to run (e.g. ls -la, npm install, python script.py)"},
					"work_dir":          map[string]any{"type": "string", "description": fmt.Sprintf("Working directory inside the container (default: %s)", wd)},
					"description":       map[string]any{"type": "string", "description": "Short description of what this command does (shown in task status)"},
					"timeout":           map[string]any{"type": "integer", "description": fmt.Sprintf("Timeout in seconds (default: %d, max: %d). Only applies to foreground execution.", background.DefaultExecTimeout, background.MaxExecTimeout), "minimum": 1, "maximum": background.MaxExecTimeout},
					"run_in_background": map[string]any{"type": "boolean", "description": "If true, run the command in the background. Returns immediately with a task ID. You will be notified when it completes. Use for long-running commands (installs, builds, test suites)."},
				},
				"required": []string{"command"},
			},
			Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) {
				return p.execExec(ctx.Context, sess, inputAsMap(input))
			},
		},
		{
			Name:        "bg_status",
			Description: "Check the status of background tasks or kill a running one.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"action":  map[string]any{"type": "string", "enum": []string{"list", "status", "kill"}, "description": "Action to perform: list all tasks, get status of one task, or kill a running task"},
					"task_id": map[string]any{"type": "string", "description": "Task ID (required for status and kill actions)"},
				},
				"required": []string{"action"},
			},
			Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) {
				return p.execBgStatus(ctx.Context, sess, inputAsMap(input))
			},
		},
	}, nil
}

func (p *ContainerProvider) normalizePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return path
	}
	prefix := p.execWorkDir
	if prefix == "" {
		prefix = defaultContainerExecWorkDir
	}
	if path == prefix {
		return "."
	}
	if strings.HasPrefix(path, prefix+"/") {
		return strings.TrimLeft(strings.TrimPrefix(path, prefix+"/"), "/")
	}
	return path
}

func (p *ContainerProvider) getClient(ctx context.Context, botID string) (*bridge.Client, error) {
	botID = strings.TrimSpace(botID)
	if botID == "" {
		return nil, errors.New("bot_id is required")
	}
	client, err := p.clients.MCPClient(ctx, botID)
	if err != nil {
		return nil, fmt.Errorf("container not reachable: %w", err)
	}
	return client, nil
}

func (p *ContainerProvider) execRead(ctx context.Context, session SessionContext, args map[string]any) (any, error) {
	client, err := p.getClient(ctx, session.BotID)
	if err != nil {
		return nil, err
	}
	filePath := p.normalizePath(StringArg(args, "path"))
	if filePath == "" {
		return nil, errors.New("path is required")
	}
	lineOffset := int32(1)
	if offset, ok, err := IntArg(args, "line_offset"); err != nil {
		return nil, fmt.Errorf("invalid line_offset: %w", err)
	} else if ok {
		if offset < 1 {
			return nil, errors.New("line_offset must be >= 1")
		}
		if offset > math.MaxInt32 {
			return nil, errors.New("line_offset exceeds maximum")
		}
		lineOffset = int32(offset)
	}
	nLines := int32(readMaxLines)
	if n, ok, err := IntArg(args, "n_lines"); err != nil {
		return nil, fmt.Errorf("invalid n_lines: %w", err)
	} else if ok {
		if n < 1 {
			return nil, errors.New("n_lines must be >= 1")
		}
		if n > readMaxLines {
			n = readMaxLines
		}
		nLines = int32(n) //nolint:gosec // bounded by readMaxLines
	}
	resp, err := client.ReadFile(ctx, filePath, lineOffset, nLines)
	if err != nil {
		return nil, err
	}
	if resp.GetBinary() {
		if !session.SupportsImageInput {
			return nil, errors.New("file appears to be binary. Read tool only supports text files (image reading not available for this model)")
		}
		return ReadImageFromContainer(ctx, client, filePath, defaultReadMediaMaxBytes), nil
	}
	content := addLineNumbers(resp.GetContent(), lineOffset)
	return map[string]any{"content": content, "total_lines": resp.GetTotalLines()}, nil
}

func (p *ContainerProvider) execWrite(ctx context.Context, session SessionContext, args map[string]any) (any, error) {
	client, err := p.getClient(ctx, session.BotID)
	if err != nil {
		return nil, err
	}
	filePath := p.normalizePath(StringArg(args, "path"))
	content := StringArg(args, "content")
	if filePath == "" {
		return nil, errors.New("path is required")
	}
	if err := client.WriteFile(ctx, filePath, []byte(content)); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

func (p *ContainerProvider) execList(ctx context.Context, session SessionContext, args map[string]any) (any, error) {
	client, err := p.getClient(ctx, session.BotID)
	if err != nil {
		return nil, err
	}
	dirPath := p.normalizePath(StringArg(args, "path"))
	if dirPath == "" {
		dirPath = "."
	}
	recursive, _, _ := BoolArg(args, "recursive")

	offset := int32(0)
	if v, ok, err := IntArg(args, "offset"); err != nil {
		return nil, fmt.Errorf("invalid offset: %w", err)
	} else if ok {
		if v < 0 {
			return nil, errors.New("offset must be >= 0")
		}
		if v > math.MaxInt32 {
			return nil, errors.New("offset exceeds maximum")
		}
		offset = int32(v) //nolint:gosec // bounded above
	}

	limit := int32(listMaxEntries)
	if v, ok, err := IntArg(args, "limit"); err != nil {
		return nil, fmt.Errorf("invalid limit: %w", err)
	} else if ok {
		if v < 1 {
			return nil, errors.New("limit must be >= 1")
		}
		if v > listMaxEntries {
			v = listMaxEntries
		}
		limit = int32(v) //nolint:gosec // bounded by listMaxEntries
	}

	var collapseThreshold int32
	if recursive {
		collapseThreshold = listCollapseThreshold
	}

	result, err := client.ListDir(ctx, dirPath, recursive, offset, limit, collapseThreshold)
	if err != nil {
		return nil, err
	}

	entriesMaps := make([]map[string]any, 0, len(result.Entries))
	for _, e := range result.Entries {
		m := map[string]any{
			"path": e.GetPath(), "is_dir": e.GetIsDir(), "size": e.GetSize(),
			"mode": e.GetMode(), "mod_time": e.GetModTime(),
		}
		if s := e.GetSummary(); s != "" {
			m["summary"] = s
		}
		entriesMaps = append(entriesMaps, m)
	}

	return map[string]any{
		"path":        dirPath,
		"entries":     entriesMaps,
		"total_count": result.TotalCount,
		"truncated":   result.Truncated,
		"offset":      offset,
		"limit":       limit,
	}, nil
}

func (p *ContainerProvider) execEdit(ctx context.Context, session SessionContext, args map[string]any) (any, error) {
	client, err := p.getClient(ctx, session.BotID)
	if err != nil {
		return nil, err
	}
	filePath := p.normalizePath(StringArg(args, "path"))
	oldText := StringArg(args, "old_text")
	newText := StringArg(args, "new_text")
	if filePath == "" || oldText == "" {
		return nil, errors.New("path, old_text and new_text are required")
	}
	reader, err := client.ReadRaw(ctx, filePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	raw, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	updated, err := applyEdit(string(raw), filePath, oldText, newText)
	if err != nil {
		return nil, err
	}
	if err := client.WriteFile(ctx, filePath, []byte(updated)); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

func (p *ContainerProvider) execExec(ctx context.Context, session SessionContext, args map[string]any) (any, error) {
	botID := strings.TrimSpace(session.BotID)
	client, err := p.getClient(ctx, botID)
	if err != nil {
		return nil, err
	}
	command := strings.TrimSpace(StringArg(args, "command"))
	if command == "" {
		return nil, errors.New("command is required")
	}
	workDir := strings.TrimSpace(StringArg(args, "work_dir"))
	if workDir == "" {
		workDir = p.execWorkDir
	}
	description := strings.TrimSpace(StringArg(args, "description"))

	// Parse timeout (default 30s, max 600s).
	timeout := background.DefaultExecTimeout
	if t, ok, err := IntArg(args, "timeout"); err != nil {
		return nil, fmt.Errorf("invalid timeout: %w", err)
	} else if ok {
		if t < 1 {
			return nil, errors.New("timeout must be >= 1")
		}
		if int32(t) > background.MaxExecTimeout {
			t = int(background.MaxExecTimeout)
		}
		timeout = int32(t) //nolint:gosec // bounded above
	}

	// Background execution path.
	runInBg, _, _ := BoolArg(args, "run_in_background")
	if runInBg && p.bgManager != nil {
		return p.execExecBackground(ctx, session, client, command, workDir, description)
	}

	// Foreground execution with configurable timeout.
	result, err := client.Exec(ctx, command, workDir, timeout)
	if err != nil {
		return nil, err
	}
	stdout := pruneToolOutputText(result.Stdout, "tool result (exec stdout)")
	stderr := pruneToolOutputText(result.Stderr, "tool result (exec stderr)")
	return map[string]any{"stdout": stdout, "stderr": stderr, "exit_code": result.ExitCode}, nil
}

// execExecBackground spawns the command as a background task and returns immediately.
func (p *ContainerProvider) execExecBackground(
	_ context.Context, session SessionContext, client *bridge.Client,
	command, workDir, description string,
) (any, error) {
	execFn := func(ctx context.Context, cmd, wd string, timeout int32) (*bridge.ExecResult, error) {
		return client.Exec(ctx, cmd, wd, timeout)
	}
	writeFn := func(ctx context.Context, path string, data []byte) error {
		return client.WriteFile(ctx, path, data)
	}

	taskID := p.bgManager.Spawn(
		session.BotID, session.SessionID,
		command, workDir, description,
		execFn, writeFn,
	)

	task := p.bgManager.Get(taskID)
	outputFile := ""
	if task != nil {
		outputFile = task.OutputFile
	}

	return map[string]any{
		"status":      "background_started",
		"task_id":     taskID,
		"output_file": outputFile,
		"message":     fmt.Sprintf("Command started in background with task ID: %s. You will be notified when it completes. Output is being written to: %s. Do NOT poll or sleep — you will receive a notification automatically.", taskID, outputFile),
	}, nil
}

// execBgStatus handles the bg_status tool for listing/checking/killing background tasks.
func (p *ContainerProvider) execBgStatus(_ context.Context, session SessionContext, args map[string]any) (any, error) {
	if p.bgManager == nil {
		return nil, errors.New("background task manager not available")
	}

	action := strings.TrimSpace(StringArg(args, "action"))
	taskID := strings.TrimSpace(StringArg(args, "task_id"))

	switch action {
	case "list":
		tasks := p.bgManager.ListForSession(session.BotID, session.SessionID)
		entries := make([]map[string]any, 0, len(tasks))
		for _, t := range tasks {
			entries = append(entries, map[string]any{
				"task_id":     t.ID,
				"command":     truncateStr(t.Command, 120),
				"description": t.Description,
				"status":      string(t.Status),
				"output_file": t.OutputFile,
				"started_at":  session.FormatTime(t.StartedAt),
			})
		}
		return map[string]any{"tasks": entries, "count": len(entries)}, nil

	case "status":
		if taskID == "" {
			return nil, errors.New("task_id is required for status action")
		}
		task := p.bgManager.Get(taskID)
		if task == nil {
			return nil, fmt.Errorf("task %s not found", taskID)
		}
		result := map[string]any{
			"task_id":     task.ID,
			"command":     task.Command,
			"description": task.Description,
			"status":      string(task.Status),
			"output_file": task.OutputFile,
			"started_at":  session.FormatTime(task.StartedAt),
		}
		if task.Status != background.TaskRunning {
			result["exit_code"] = task.ExitCode
			result["completed_at"] = session.FormatTime(task.CompletedAt)
			result["output_tail"] = task.OutputTail()
		}
		return result, nil

	case "kill":
		if taskID == "" {
			return nil, errors.New("task_id is required for kill action")
		}
		if err := p.bgManager.Kill(taskID); err != nil {
			return nil, err
		}
		return map[string]any{"ok": true, "message": fmt.Sprintf("Task %s has been killed.", taskID)}, nil

	default:
		return nil, fmt.Errorf("unknown action: %s (expected: list, status, kill)", action)
	}
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func addLineNumbers(content string, startLine int32) string {
	if content == "" {
		return content
	}
	lines := strings.Split(strings.TrimRight(content, "\n"), "\n")
	var out strings.Builder
	out.Grow(len(content) + len(lines)*8)
	for i, line := range lines {
		fmt.Fprintf(&out, "%6d\t%s\n", int(startLine)+i, line)
	}
	return out.String()
}
