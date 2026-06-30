package reflow

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// collectSink captures emitted events for direct engine assertions.
type collectSink struct {
	runs      []RunRecord
	records   []Record
	warnings  []Warning
	errs      []ErrorEvent
	summaries []SummaryRecord
}

func (s *collectSink) OnRun(_ context.Context, rec RunRecord) error {
	s.runs = append(s.runs, rec)
	return nil
}
func (s *collectSink) OnSource(context.Context, SourceRunRecord) error { return nil }
func (s *collectSink) OnRecord(_ context.Context, rec Record) error {
	s.records = append(s.records, rec)
	return nil
}
func (s *collectSink) OnWarning(_ context.Context, w Warning) error {
	s.warnings = append(s.warnings, w)
	return nil
}
func (s *collectSink) OnError(_ context.Context, e ErrorEvent) error {
	s.errs = append(s.errs, e)
	return nil
}
func (s *collectSink) OnSummary(_ context.Context, rec SummaryRecord) error {
	s.summaries = append(s.summaries, rec)
	return nil
}

func dryRunConfig(sink EventSink) Config {
	return Config{
		Destination: Destination{Provider: sentinelProvider{}, ProviderID: "s3", BaseURI: "s3://dest-bucket/data/"},
		Collision:   CollisionPolicy{Mode: CollisionSkipIfDuplicate},
		Concurrency: ResolveConcurrency(1, true, DefaultResourceProbe()),
		DryRun:      true,
		Events:      sink,
	}
}

func TestRunnerDryRunRecordStream(t *testing.T) {
	sink := &collectSink{}
	runner, err := NewRunner(dryRunConfig(sink))
	require.NoError(t, err)

	line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"etag-a","source_size_bytes":7,"dest_rel_key":"a/b.xml"}}`
	summary, err := runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(line)})
	require.NoError(t, err)

	require.Len(t, sink.runs, 1)
	require.True(t, sink.runs[0].DryRun)
	require.Equal(t, "s3://dest-bucket/data/", sink.runs[0].DestURI)
	require.Equal(t, 1, sink.runs[0].Parallel)

	// Dry-run over a skip-if-duplicate object store reports the head-compare fallback.
	require.Len(t, sink.warnings, 1)
	require.Equal(t, ifAbsentFallbackWarningCode, sink.warnings[0].Code)

	require.Len(t, sink.records, 1)
	require.Equal(t, "planned", sink.records[0].Status)
	require.Equal(t, "data/a/b.xml", sink.records[0].DestKey)
	require.Equal(t, "s3://dest-bucket/data/a/b.xml", sink.records[0].DestURI)
	require.Equal(t, "s3://source-bucket/a/b.xml", sink.records[0].SourceURI)

	require.Len(t, sink.summaries, 1)
	require.Equal(t, int64(1), summary.Statuses["planned"])
	require.True(t, summary.FallbackActive)
	require.Equal(t, "inconclusive", summary.DestIfAbsentProbeStatus)
}

func (s *collectSink) emitted() bool {
	return len(s.runs)+len(s.records)+len(s.warnings)+len(s.errs)+len(s.summaries) > 0
}

const s3DryRunLine = `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","dest_rel_key":"a/b.xml"}}`

// Deferral is decided before any stream bytes are read (source form + config), so
// ErrNotImplemented emits nothing and leaves the reader untouched — a caller can
// fall back to the CLI path with the same Source.
func TestRunnerDefersBeforeReading(t *testing.T) {
	t.Run("non dry-run copy leaves reader untouched", func(t *testing.T) {
		cfg := dryRunConfig(&collectSink{})
		cfg.DryRun = false
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		reader := strings.NewReader(s3DryRunLine)
		_, err = runner.Run(context.Background(), RecordStreamSource{Records: reader})
		require.True(t, errors.Is(err, ErrNotImplemented))
		require.False(t, cfg.Events.(*collectSink).emitted())
		remaining, _ := io.ReadAll(reader)
		require.Equal(t, s3DryRunLine, string(remaining), "reader must be untouched for fallback")
	})

	t.Run("unsupported source form", func(t *testing.T) {
		sink := &collectSink{}
		runner, err := NewRunner(dryRunConfig(sink))
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), ObjectSource{Provider: sentinelProvider{}, URI: "s3://b/k"})
		require.True(t, errors.Is(err, ErrNotImplemented))
		require.False(t, sink.emitted())
	})
}

// streamSink reports each planned record's source key over a channel so a test
// can observe streaming (emission interleaved with reading).
type streamSink struct{ keys chan string }

func (s *streamSink) OnRun(context.Context, RunRecord) error          { return nil }
func (s *streamSink) OnSource(context.Context, SourceRunRecord) error { return nil }
func (s *streamSink) OnRecord(_ context.Context, rec Record) error {
	s.keys <- rec.SourceKey
	return nil
}
func (s *streamSink) OnWarning(context.Context, Warning) error       { return nil }
func (s *streamSink) OnError(context.Context, ErrorEvent) error      { return nil }
func (s *streamSink) OnSummary(context.Context, SummaryRecord) error { return nil }

// TestRunnerDryRunStreamsRecordByRecord proves the supported dry-run path does not
// accumulate the whole stream before emitting: the first record is emitted before
// the second is written. A buffer-all implementation would block reading the
// second record and deadlock.
func TestRunnerDryRunStreamsRecordByRecord(t *testing.T) {
	pr, pw := io.Pipe()
	sink := &streamSink{keys: make(chan string, 4)}
	runner, err := NewRunner(dryRunConfig(sink))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { _, runErr := runner.Run(context.Background(), RecordStreamSource{Records: pr}); done <- runErr }()

	line := func(k string) string {
		return `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://b/` + k + `","source_key":"` + k + `","dest_rel_key":"` + k + `"}}` + "\n"
	}
	_, _ = pw.Write([]byte(line("k1")))
	select {
	case key := <-sink.keys:
		require.Equal(t, "k1", key)
	case <-time.After(5 * time.Second):
		t.Fatal("dry-run did not stream: first record not emitted before the second was written")
	}
	_, _ = pw.Write([]byte(line("k2")))
	require.Equal(t, "k2", <-sink.keys)
	require.NoError(t, pw.Close())
	require.NoError(t, <-done)
}

// TestRunnerDryRunInvalidInputStreams shows per-record problems surface as
// INVALID_INPUT events (CLI-equivalent) without aborting the run.
func TestRunnerDryRunInvalidInputStreams(t *testing.T) {
	sink := &collectSink{}
	runner, err := NewRunner(dryRunConfig(sink))
	require.NoError(t, err)
	stream := strings.Join([]string{
		s3DryRunLine,
		`{"type":"gonimbus.index.object.v1","data":{}}`,
		`not-json`,
	}, "\n")
	summary, err := runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(stream)})
	// The run completes (summary + error events emitted) but reports failure, like
	// the command path's non-zero exit on invalid inputs.
	var invErr *InvalidInputsError
	require.ErrorAs(t, err, &invErr)
	require.Equal(t, int64(2), invErr.Count)
	require.Len(t, sink.records, 1)
	require.Equal(t, "a/b.xml", sink.records[0].SourceKey)
	require.Len(t, sink.errs, 2)
	require.Equal(t, ErrCodeInvalidInput, sink.errs[0].Code)
	require.Equal(t, int64(2), summary.InvalidInputs)
	require.Len(t, sink.summaries, 1)
}

func TestRunnerDryRunQuarantineAndRewrite(t *testing.T) {
	t.Run("quarantine routing", func(t *testing.T) {
		sink := &collectSink{}
		runner, err := NewRunner(dryRunConfig(sink))
		require.NoError(t, err)
		q := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://b/raw/o.xml","source_key":"raw/o.xml","routing_class":"quarantine","quarantine_prefix":"quar"}}`
		_, err = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(q)})
		require.NoError(t, err)
		require.Len(t, sink.records, 1)
		require.Equal(t, "data/quar/raw/o.xml", sink.records[0].DestKey)
	})

	t.Run("rewrite when dest_rel_key absent", func(t *testing.T) {
		cfg := dryRunConfig(&collectSink{})
		cfg.Rewrite = RewriteConfig{From: "{key}", To: "renamed/{key}"}
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		r := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://b/o.xml","source_key":"o.xml","vars":{"key":"o.xml"}}}`
		_, err = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(r)})
		require.NoError(t, err)
		sink := cfg.Events.(*collectSink)
		require.Len(t, sink.records, 1)
		require.Equal(t, "data/renamed/o.xml", sink.records[0].DestKey)
	})
}
