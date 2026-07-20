package reflowthroughput

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// MaxCapturedStderr bounds retained stderr per stage for structural diagnostics.
const MaxCapturedStderr = 64 << 10 // 64 KiB

// StageResult is the independent disposition of one supervised child.
type StageResult struct {
	Name       string        `json:"name"`
	ExitCode   int           `json:"exit_code"`
	Err        string        `json:"error,omitempty"`
	Stderr     string        `json:"stderr_excerpt,omitempty"`
	Duration   time.Duration `json:"duration_ns"`
	StartedAt  time.Time     `json:"-"`
	FinishedAt time.Time     `json:"-"`
}

// PointResult aggregates one measured sweep point.
type PointResult struct {
	Profile          string
	PointID          string
	Parallel         int
	ProbeConcurrency int
	GOMEMLIMIT       string
	// MemoryBudget is the operator --memory-budget value passed to the child
	// (empty when the arm let the product derive the budget).
	MemoryBudget    string
	CheckpointClass string // disk | tmpfs — never the path
	ProviderClass   string // file | moto | s3-compatible | gcs
	Stages          map[string]StageResult
	Tap             TapStats
	Stdout          []byte // small in-memory only; prefer StdoutPath for scale
	StdoutPath      string // external run artifact for reflow JSONL
	Elapsed         time.Duration
	// CompletedObjects is derived from summary/object counts when available.
	CompletedObjects int64
	WallRate         float64 // objects / elapsed seconds (0 if incomplete)
}

// ChildEnv builds a per-child environment copy. It never calls os.Setenv.
// When gomemlimit is non-empty it replaces/sets GOMEMLIMIT on the copy only.
// extra pairs (e.g. moto AWS keys) are appended/replaced without ambient mutation.
func ChildEnv(base []string, gomemlimit string, extra ...string) []string {
	if base == nil {
		base = os.Environ()
	}
	out := make([]string, 0, len(base)+1+len(extra))
	seen := false
	for _, kv := range base {
		if len(kv) >= 11 && kv[:11] == "GOMEMLIMIT=" {
			if gomemlimit != "" {
				out = append(out, "GOMEMLIMIT="+gomemlimit)
				seen = true
			}
			// If gomemlimit empty, drop ambient GOMEMLIMIT so sibling leakage
			// does not affect measurement (operator must supply explicitly).
			continue
		}
		// Drop keys that extra will replace.
		skip := false
		for _, e := range extra {
			if eq, _, _ := strings.Cut(e, "="); eq != "" && strings.HasPrefix(kv, eq+"=") {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		out = append(out, kv)
	}
	if gomemlimit != "" && !seen {
		out = append(out, "GOMEMLIMIT="+gomemlimit)
	}
	out = append(out, extra...)
	return out
}

// StageRunOpts configures a supervised child stage.
type StageRunOpts struct {
	Binary         string
	InputPath      string
	DestURI        string
	Parallel       int
	CheckpointPath string
	GOMEMLIMIT     string
	// MemoryBudget is the operator --memory-budget size string. Empty omits the
	// flag entirely, so the product derives the budget from the detected limit.
	MemoryBudget  string
	NoAdaptive    bool
	ProviderClass string
	ExtraArgs     []string
	ChildExtraEnv []string
	// StdoutPath streams reflow JSONL to a file (bounded memory). Required for scale.
	StdoutPath string
	// RewriteFrom/To map probe output without dest_rel_key onto destinations.
	RewriteFrom string
	RewriteTo   string
}

// RunReflowOnly executes: binary transfer reflow --stdin ... with pre-materialized input.
func RunReflowOnly(ctx context.Context, opts StageRunOpts) (PointResult, error) {
	if opts.ProviderClass == "" {
		opts.ProviderClass = ProviderFile
	}
	pr := PointResult{
		Parallel:        opts.Parallel,
		GOMEMLIMIT:      opts.GOMEMLIMIT,
		MemoryBudget:    opts.MemoryBudget,
		ProviderClass:   opts.ProviderClass,
		Stages:          map[string]StageResult{},
		CheckpointClass: "disk",
	}
	args := []string{
		"transfer", "reflow",
		"--stdin",
		"--dest", opts.DestURI,
		"--on-collision", "skip-if-duplicate",
		"--parallel", fmt.Sprintf("%d", opts.Parallel),
	}
	if opts.CheckpointPath != "" {
		args = append(args, "--checkpoint", opts.CheckpointPath)
	}
	if opts.NoAdaptive {
		args = append(args, "--no-adaptive")
	}
	if strings.TrimSpace(opts.MemoryBudget) != "" {
		args = append(args, "--memory-budget", strings.TrimSpace(opts.MemoryBudget))
	}
	if opts.RewriteFrom != "" {
		args = append(args, "--rewrite-from", opts.RewriteFrom, "--rewrite-to", opts.RewriteTo)
	}
	args = append(args, opts.ExtraArgs...)

	in, err := os.Open(opts.InputPath) // #nosec G304 -- harness-owned pre-materialized input
	if err != nil {
		return pr, err
	}
	defer func() { _ = in.Close() }()

	stdoutW, stdoutPath, err := openStdoutSink(opts.StdoutPath)
	if err != nil {
		return pr, err
	}
	if closer, ok := stdoutW.(io.Closer); ok && opts.StdoutPath != "" {
		defer func() { _ = closer.Close() }()
	}
	pr.StdoutPath = stdoutPath

	cmd := exec.CommandContext(ctx, opts.Binary, args...)
	cmd.Env = ChildEnv(nil, opts.GOMEMLIMIT, opts.ChildExtraEnv...)
	cmd.Stdin = in
	var stderr bytes.Buffer
	cmd.Stdout = stdoutW
	cmd.Stderr = &limitedBuffer{max: MaxCapturedStderr, buf: &stderr}

	start := monoNow()
	runErr := cmd.Run()
	elapsed := monoNow().Sub(start)
	pr.Elapsed = elapsed

	// For small local smokes without a file sink, keep bytes for callers that still use Stdout.
	if opts.StdoutPath == "" {
		if buf, ok := stdoutW.(*bytes.Buffer); ok {
			pr.Stdout = buf.Bytes()
		}
	}

	code := 0
	errStr := ""
	if runErr != nil {
		errStr = runErr.Error()
		if ee, ok := runErr.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			code = -1
		}
	}
	pr.Stages["reflow"] = StageResult{
		Name:       "reflow",
		ExitCode:   code,
		Err:        errStr,
		Stderr:     stderr.String(),
		Duration:   elapsed,
		StartedAt:  start,
		FinishedAt: monoNow(),
	}
	if runErr != nil {
		return pr, fmt.Errorf("reflow stage failed: %w", runErr)
	}
	return pr, nil
}

func openStdoutSink(path string) (io.Writer, string, error) {
	if path == "" {
		return &bytes.Buffer{}, "", nil
	}
	f, err := os.Create(path) // #nosec G304 -- harness-owned run artifact
	if err != nil {
		return nil, "", err
	}
	return f, path, nil
}

// FullPipeOpts configures a supervised probe | tap | reflow point. Named
// fields rather than positional arguments: the several adjacent string
// parameters (checkpoint path, GOMEMLIMIT, memory budget, stdout path) are
// otherwise trivially transposable at the call site.
type FullPipeOpts struct {
	Binary         string
	SourcePrefix   string
	ProbeConfig    string
	DestURI        string
	ProbeConc      int
	ReflowParallel int
	CheckpointPath string
	GOMEMLIMIT     string
	// MemoryBudget is the operator --memory-budget size string; empty omits
	// the flag so the product derives the budget from the detected limit.
	MemoryBudget string
	StdoutPath   string
}

// RunFullPipe supervises probe | tap | reflow without a shell.
// Shared cancel on any stage/tap failure; rewrite templates supply dest mapping
// for probe-emitted reflow.input (no dest_rel_key).
func RunFullPipe(ctx context.Context, opts FullPipeOpts) (PointResult, error) {
	binary := opts.Binary
	sourcePrefix := opts.SourcePrefix
	probeConfig := opts.ProbeConfig
	destURI := opts.DestURI
	probeConc := opts.ProbeConc
	reflowParallel := opts.ReflowParallel
	checkpointPath := opts.CheckpointPath
	gomemlimit := opts.GOMEMLIMIT
	stdoutPath := opts.StdoutPath
	pr := PointResult{
		Parallel:         reflowParallel,
		ProbeConcurrency: probeConc,
		GOMEMLIMIT:       gomemlimit,
		MemoryBudget:     opts.MemoryBudget,
		ProviderClass:    ProviderFile,
		Stages:           map[string]StageResult{},
		CheckpointClass:  "disk",
		StdoutPath:       stdoutPath,
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	probeR, probeW, err := os.Pipe()
	if err != nil {
		return pr, err
	}
	reflowR, tapW, err := os.Pipe()
	if err != nil {
		_ = probeR.Close()
		_ = probeW.Close()
		return pr, err
	}

	// Probe does not emit dest_rel_key. Reflow rewrite is segment-based (not
	// whole-key {key}): synthetic hive keys are always 4 segments
	// (entity/device/date/object), so identity rewrite is 4 captures.
	reflowArgs := []string{
		"transfer", "reflow",
		"--stdin",
		"--dest", destURI,
		"--on-collision", "skip-if-duplicate",
		"--parallel", fmt.Sprintf("%d", reflowParallel),
		"--rewrite-from", "{entity}/{device}/{date}/{object}",
		"--rewrite-to", "{entity}/{device}/{date}/{object}",
	}
	if checkpointPath != "" {
		reflowArgs = append(reflowArgs, "--checkpoint", checkpointPath)
	}
	if strings.TrimSpace(opts.MemoryBudget) != "" {
		reflowArgs = append(reflowArgs, "--memory-budget", strings.TrimSpace(opts.MemoryBudget))
	}
	reflowCmd := exec.CommandContext(ctx, binary, reflowArgs...)
	reflowCmd.Env = ChildEnv(nil, gomemlimit)
	reflowCmd.Stdin = reflowR
	stdoutW, _, err := openStdoutSink(stdoutPath)
	if err != nil {
		closeAll(probeR, probeW, reflowR, tapW)
		return pr, err
	}
	if closer, ok := stdoutW.(io.Closer); ok && stdoutPath != "" {
		defer func() { _ = closer.Close() }()
	}
	var reflowStderr bytes.Buffer
	reflowCmd.Stdout = stdoutW
	reflowCmd.Stderr = &limitedBuffer{max: MaxCapturedStderr, buf: &reflowStderr}

	probeArgs := []string{
		"content", "probe",
		sourcePrefix,
		"--config", probeConfig,
		"--emit", "reflow-input",
		"--concurrency", fmt.Sprintf("%d", probeConc),
	}
	probeCmd := exec.CommandContext(ctx, binary, probeArgs...)
	probeCmd.Env = ChildEnv(nil, gomemlimit)
	probeCmd.Stdout = probeW
	var probeStderr bytes.Buffer
	probeCmd.Stderr = &limitedBuffer{max: MaxCapturedStderr, buf: &probeStderr}

	tap := &Tap{AcceptReflowInputOnly: true, MaxRecordBytes: DefaultMaxRecordBytes}

	start := monoNow()
	if err := reflowCmd.Start(); err != nil {
		closeAll(probeR, probeW, reflowR, tapW)
		return pr, fmt.Errorf("start reflow: %w", err)
	}
	_ = reflowR.Close()

	if err := probeCmd.Start(); err != nil {
		cancel()
		_ = probeR.Close()
		_ = probeW.Close()
		_ = tapW.Close()
		_ = reflowCmd.Process.Kill()
		_ = reflowCmd.Wait()
		return pr, fmt.Errorf("start probe: %w", err)
	}
	_ = probeW.Close()

	var tapErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = probeR.Close(); _ = tapW.Close() }()
		tapErr = tap.Copy(tapW, probeR)
		if tapErr != nil {
			cancel() // unblock probe/reflow
		}
	}()

	// Concurrent waits: do not serialize probe-before-reflow when tap fails.
	var probeWaitErr, reflowWaitErr error
	var waitWG sync.WaitGroup
	waitWG.Add(2)
	go func() {
		defer waitWG.Done()
		probeWaitErr = probeCmd.Wait()
		if probeWaitErr != nil {
			cancel()
		}
	}()
	go func() {
		defer waitWG.Done()
		reflowWaitErr = reflowCmd.Wait()
		if reflowWaitErr != nil {
			cancel()
		}
	}()
	wg.Wait()
	waitWG.Wait()
	elapsed := monoNow().Sub(start)

	pr.Elapsed = elapsed
	if stdoutPath == "" {
		if buf, ok := stdoutW.(*bytes.Buffer); ok {
			pr.Stdout = buf.Bytes()
		}
	}
	pr.Tap = tap.Stats()
	pr.Stages["probe"] = stageFromWait("probe", probeWaitErr, probeStderr.String(), elapsed)
	pr.Stages["reflow"] = stageFromWait("reflow", reflowWaitErr, reflowStderr.String(), elapsed)
	if tapErr != nil {
		pr.Stages["tap"] = StageResult{Name: "tap", ExitCode: -1, Err: tapErr.Error(), Duration: elapsed}
	} else {
		pr.Stages["tap"] = StageResult{Name: "tap", ExitCode: 0, Duration: elapsed}
	}

	if probeWaitErr != nil || reflowWaitErr != nil || tapErr != nil {
		return pr, fmt.Errorf("full-pipe stage failure: probe=%v tap=%v reflow=%v", probeWaitErr, tapErr, reflowWaitErr)
	}
	return pr, nil
}

// RunProbeDrain runs content probe into a draining tap sink (no reflow).
// This is the methodology-faithful producer-only probe saturation shape.
func RunProbeDrain(ctx context.Context, binary string, sourcePrefix string, probeConfig string, probeConc int, gomemlimit string) (PointResult, error) {
	pr := PointResult{
		ProbeConcurrency: probeConc,
		GOMEMLIMIT:       gomemlimit,
		ProviderClass:    ProviderFile,
		Stages:           map[string]StageResult{},
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	probeR, probeW, err := os.Pipe()
	if err != nil {
		return pr, err
	}

	probeArgs := []string{
		"content", "probe",
		sourcePrefix,
		"--config", probeConfig,
		"--emit", "reflow-input",
		"--concurrency", fmt.Sprintf("%d", probeConc),
	}
	probeCmd := exec.CommandContext(ctx, binary, probeArgs...)
	probeCmd.Env = ChildEnv(nil, gomemlimit)
	probeCmd.Stdout = probeW
	var probeStderr bytes.Buffer
	probeCmd.Stderr = &limitedBuffer{max: MaxCapturedStderr, buf: &probeStderr}

	tap := &Tap{AcceptReflowInputOnly: true, MaxRecordBytes: DefaultMaxRecordBytes}
	start := monoNow()
	if err := probeCmd.Start(); err != nil {
		closeAll(probeR, probeW)
		return pr, fmt.Errorf("start probe: %w", err)
	}
	_ = probeW.Close()

	var tapErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = probeR.Close() }()
		tapErr = tap.Copy(io.Discard, probeR)
		if tapErr != nil {
			cancel()
		}
	}()
	probeWaitErr := probeCmd.Wait()
	if probeWaitErr != nil {
		cancel()
	}
	wg.Wait()
	elapsed := monoNow().Sub(start)
	pr.Elapsed = elapsed
	pr.Tap = tap.Stats()
	pr.Stages["probe"] = stageFromWait("probe", probeWaitErr, probeStderr.String(), elapsed)
	if tapErr != nil {
		pr.Stages["tap"] = StageResult{Name: "tap", ExitCode: -1, Err: tapErr.Error(), Duration: elapsed}
	} else {
		pr.Stages["tap"] = StageResult{Name: "tap", ExitCode: 0, Duration: elapsed}
	}
	if probeWaitErr != nil || tapErr != nil {
		return pr, fmt.Errorf("probe-drain failure: probe=%v tap=%v", probeWaitErr, tapErr)
	}
	return pr, nil
}

func stageFromWait(name string, waitErr error, stderr string, d time.Duration) StageResult {
	sr := StageResult{Name: name, Stderr: stderr, Duration: d}
	if waitErr == nil {
		return sr
	}
	sr.Err = waitErr.Error()
	if ee, ok := waitErr.(*exec.ExitError); ok {
		sr.ExitCode = ee.ExitCode()
	} else {
		sr.ExitCode = -1
	}
	return sr
}

func closeAll(cs ...io.Closer) {
	for _, c := range cs {
		if c != nil {
			_ = c.Close()
		}
	}
}

type limitedBuffer struct {
	max int
	buf *bytes.Buffer
	n   int
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	if l.n >= l.max {
		return len(p), nil
	}
	remain := l.max - l.n
	if len(p) > remain {
		_, _ = l.buf.Write(p[:remain])
		l.n = l.max
		return len(p), nil
	}
	n, err := l.buf.Write(p)
	l.n += n
	return n, err
}

// HashFile returns SHA-256 hex of a file (binary provenance).
func HashFile(path string) (string, error) {
	b, err := os.ReadFile(path) // #nosec G304 -- operator/build-provided binary path
	if err != nil {
		return "", err
	}
	return ContentDigest(b), nil
}

// EnsureEmptyDir fails if path exists and is non-empty, or creates it empty.
func EnsureEmptyDir(path string) error {
	fi, err := os.Stat(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(path, 0o755)
	}
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("destination path exists and is not a directory")
	}
	ents, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(ents) > 0 {
		return fmt.Errorf("destination is not empty")
	}
	return nil
}

// EnsureAbsent fails if path already exists (fresh checkpoint identity).
func EnsureAbsent(path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("checkpoint path already exists")
}

// CountFilesRecursive counts regular files under root (post-run object count).
func CountFilesRecursive(root string) (int64, error) {
	var n int64
	err := filepathWalkFiles(root, func(string) {
		n++
	})
	return n, err
}

func filepathWalkFiles(root string, fn func(string)) error {
	return walkDir(root, fn)
}

func walkDir(root string, fn func(string)) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		p := root + string(os.PathSeparator) + e.Name()
		if e.IsDir() {
			if err := walkDir(p, fn); err != nil {
				return err
			}
			continue
		}
		if e.Type().IsRegular() {
			fn(p)
		}
	}
	return nil
}
