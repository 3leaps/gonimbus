package reflowthroughput

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/3leaps/gonimbus/pkg/reflow"
)

// ParsedReflowOutput is the allowlist-extracted view of reflow child stdout.
// It never retains destination URI or checkpoint path from product records.
type ParsedReflowOutput struct {
	RunCount          int
	SummaryCount      int
	WarningClampCount int
	ObjectComplete    int64
	objectCompleteRaw int64
	ObjectError       int64
	InvalidInputs     int64
	SummaryErrors     int64

	// Run-record concurrency (required agreement with summary).
	RunRequested int
	RunEffective int
	RunReason    string
	RunMaxActive int
	RunFinal     int
	RunAdaptive  bool

	// Summary-record concurrency.
	SummaryRequested int
	SummaryEffective int
	SummaryReason    string
	SummaryMaxActive int
	SummaryFinal     int
	SummaryAdaptive  bool

	// Consensus fields after agreement check (filled by Finalize).
	Requested       int
	Effective       int
	Reason          string
	MaxActive       int
	Final           int
	AdaptiveEnabled bool
	Initial         int
	Floor           int

	ThrottleBackoffs  int64
	AdditiveIncreases int64
	ConnectionFreezes int64
	ClampWarningOK    bool
	// Clamp warning details for three-way agreement with run/summary.
	ClampWarnRequested int
	ClampWarnEffective int
	ClampWarnReason    string
	ClampWarnAdaptive  *bool
	ParseErrors        []string
	summaryComplete    int64
	hasSummaryStatus   bool
	UnknownTypes       []string
}

// ParseReflowStdout extracts concurrency and count fields by record type.
// Malformed JSONL objects and unknown types are hard failures.
func ParseReflowStdout(stdout []byte) (ParsedReflowOutput, error) {
	return ParseReflowReader(bytes.NewReader(stdout))
}

// ParseReflowReader streams JSONL from r with bounded line reading.
func ParseReflowReader(r io.Reader) (ParsedReflowOutput, error) {
	var out ParsedReflowOutput
	sc := bufio.NewScanner(r)
	// Support product-sized lines (up to DefaultMaxRecordBytes).
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, DefaultMaxRecordBytes)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		// Human stderr-style lines must not appear on stdout JSONL; soft-skip only
		// non-object noise (should not happen on reflow stdout).
		if line[0] != '{' {
			// Allow pure human warning text if it ever leaks; otherwise fail.
			if bytes.HasPrefix(line, []byte("warning:")) {
				continue
			}
			return out, fmt.Errorf("line %d: non-JSONL stdout %q", lineNo, truncate(string(line), 80))
		}
		var env struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(line, &env); err != nil {
			return out, fmt.Errorf("line %d: malformed JSON: %w", lineNo, err)
		}
		if env.Type == "" {
			return out, fmt.Errorf("line %d: missing type", lineNo)
		}
		switch env.Type {
		case reflow.RunRecordType:
			out.RunCount++
			var rr reflow.RunRecord
			if err := json.Unmarshal(env.Data, &rr); err != nil {
				return out, fmt.Errorf("line %d: run data: %w", lineNo, err)
			}
			out.RunRequested = rr.ConcurrencyCeilingRequested
			out.RunEffective = rr.ConcurrencyCeilingEffective
			out.RunReason = rr.ConcurrencyCeilingReason
			out.RunMaxActive = rr.ConcurrencyMaxActive
			out.RunFinal = rr.ConcurrencyFinal
			out.RunAdaptive = rr.AdaptiveEnabled
			out.Initial = rr.ConcurrencyInitial
			out.Floor = rr.ConcurrencyFloor
			out.ThrottleBackoffs = rr.ConcurrencyThrottleBackoffs
			out.AdditiveIncreases = rr.ConcurrencyAdditiveIncreases
			out.ConnectionFreezes = rr.ConcurrencyConnectionErrorFreezes
		case reflow.SummaryRecordType:
			out.SummaryCount++
			var sr reflow.SummaryRecord
			if err := json.Unmarshal(env.Data, &sr); err != nil {
				return out, fmt.Errorf("line %d: summary data: %w", lineNo, err)
			}
			out.SummaryRequested = sr.ConcurrencyCeilingRequested
			out.SummaryEffective = sr.ConcurrencyCeilingEffective
			out.SummaryReason = sr.ConcurrencyCeilingReason
			out.SummaryMaxActive = sr.ConcurrencyMaxActive
			out.SummaryFinal = sr.ConcurrencyFinal
			out.SummaryAdaptive = sr.AdaptiveEnabled
			out.InvalidInputs = sr.InvalidInputs
			out.SummaryErrors = sr.Errors
			if sr.Statuses != nil {
				out.hasSummaryStatus = true
				out.summaryComplete = sr.Statuses["complete"]
				out.ObjectError = sr.Statuses["error"]
			}
		case reflow.WarningRecordType:
			var w reflow.Warning
			if err := json.Unmarshal(env.Data, &w); err != nil {
				return out, fmt.Errorf("line %d: warning data: %w", lineNo, err)
			}
			if w.Code == "REFLOW_CONCURRENCY_CEILING_CLAMPED" {
				out.WarningClampCount++
				out.ClampWarningOK = true
				if w.Details != nil {
					out.ClampWarnRequested = asInt(w.Details["concurrency_ceiling_requested"])
					out.ClampWarnEffective = asInt(w.Details["concurrency_ceiling_effective"])
					if s, ok := w.Details["concurrency_ceiling_reason"].(string); ok {
						out.ClampWarnReason = s
					}
					if b, ok := w.Details["adaptive_enabled"].(bool); ok {
						out.ClampWarnAdaptive = &b
					}
				}
			}
		case reflow.RecordType:
			var rec reflow.Record
			if err := json.Unmarshal(env.Data, &rec); err != nil {
				return out, fmt.Errorf("line %d: object record: %w", lineNo, err)
			}
			switch rec.Status {
			case "complete":
				out.objectCompleteRaw++
			case "error", "failed":
				if !out.hasSummaryStatus {
					out.ObjectError++
				}
			}
		case reflow.SourceRecordType:
			// Allowed; ignore payload (may contain paths — we do not retain).
		default:
			// Known product noise types that are not concurrency-critical.
			if env.Type == "gonimbus.preflight.v1" || env.Type == "gonimbus.error.v1" {
				continue
			}
			out.UnknownTypes = append(out.UnknownTypes, env.Type)
		}
	}
	if err := sc.Err(); err != nil {
		return out, fmt.Errorf("scan stdout: %w", err)
	}
	if len(out.UnknownTypes) > 0 {
		return out, fmt.Errorf("unknown record types: %s", strings.Join(out.UnknownTypes, ", "))
	}
	if out.RunCount != 1 {
		return out, fmt.Errorf("expected exactly one %s, got %d", reflow.RunRecordType, out.RunCount)
	}
	if out.SummaryCount != 1 {
		return out, fmt.Errorf("expected exactly one %s, got %d", reflow.SummaryRecordType, out.SummaryCount)
	}
	if err := out.agreeRunSummary(); err != nil {
		return out, err
	}
	if out.hasSummaryStatus {
		out.ObjectComplete = out.summaryComplete
	} else {
		out.ObjectComplete = out.objectCompleteRaw
	}
	if err := out.agreeClampWarning(); err != nil {
		return out, err
	}
	return out, nil
}

func (p *ParsedReflowOutput) agreeClampWarning() error {
	if p.WarningClampCount == 0 {
		return nil
	}
	if p.WarningClampCount != 1 {
		return fmt.Errorf("expected at most one clamp warning, got %d", p.WarningClampCount)
	}
	// Three-way: warning details must match run/summary consensus fields.
	// All four detail fields are required (missing adaptive_enabled is a hard fail).
	if p.ClampWarnRequested != p.Requested {
		return fmt.Errorf("clamp warning requested %d != run/summary %d", p.ClampWarnRequested, p.Requested)
	}
	if p.ClampWarnEffective != p.Effective {
		return fmt.Errorf("clamp warning effective %d != run/summary %d", p.ClampWarnEffective, p.Effective)
	}
	if p.ClampWarnReason != p.Reason {
		return fmt.Errorf("clamp warning reason %q != run/summary %q", p.ClampWarnReason, p.Reason)
	}
	if p.ClampWarnAdaptive == nil {
		return fmt.Errorf("clamp warning missing adaptive_enabled detail")
	}
	if *p.ClampWarnAdaptive != p.AdaptiveEnabled {
		return fmt.Errorf("clamp warning adaptive %v != run/summary %v", *p.ClampWarnAdaptive, p.AdaptiveEnabled)
	}
	return nil
}

func asInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}

func (p *ParsedReflowOutput) agreeRunSummary() error {
	// Ceiling identity must match between run and summary (emitted at start vs end).
	if p.RunRequested != p.SummaryRequested {
		return fmt.Errorf("run/summary requested mismatch: %d vs %d", p.RunRequested, p.SummaryRequested)
	}
	if p.RunEffective != p.SummaryEffective {
		return fmt.Errorf("run/summary effective mismatch: %d vs %d", p.RunEffective, p.SummaryEffective)
	}
	if p.RunReason != p.SummaryReason {
		return fmt.Errorf("run/summary reason mismatch: %q vs %q", p.RunReason, p.SummaryReason)
	}
	if p.RunAdaptive != p.SummaryAdaptive {
		return fmt.Errorf("run/summary adaptive mismatch: %v vs %v", p.RunAdaptive, p.SummaryAdaptive)
	}
	// max_active is a progressive peak: run may still be 0 when emitted at start;
	// summary holds the authoritative peak. Require summary >= run.
	if p.SummaryMaxActive < p.RunMaxActive {
		return fmt.Errorf("run/summary max_active regress: run=%d summary=%d", p.RunMaxActive, p.SummaryMaxActive)
	}
	p.Requested = p.RunRequested
	p.Effective = p.RunEffective
	p.Reason = p.RunReason
	p.MaxActive = p.SummaryMaxActive
	p.Final = p.SummaryFinal
	if p.Final == 0 {
		p.Final = p.RunFinal
	}
	p.AdaptiveEnabled = p.RunAdaptive
	return nil
}

// ParseReflowFile opens path and parses incrementally.
func ParseReflowFile(path string) (ParsedReflowOutput, error) {
	f, err := os.Open(path) // #nosec G304 -- harness-owned stdout artifact
	if err != nil {
		return ParsedReflowOutput{}, err
	}
	defer func() { _ = f.Close() }()
	return ParseReflowReader(f)
}
