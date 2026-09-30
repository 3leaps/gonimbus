package probe

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/3leaps/gonimbus/pkg/output"
)

// SummaryRecordType identifies terminal content-probe control evidence.
const SummaryRecordType = "gonimbus.content.probe.summary.v1"

// Summary reports handled termination, not provider custody or pipeline success.
// All counters are present even for empty or failed invocations.
type Summary struct {
	Inputs        int64            `json:"inputs"`
	Enumerated    int64            `json:"enumerated"`
	Processed     int64            `json:"processed"`
	Emitted       EmittedCounts    `json:"emitted"`
	Routing       RoutingCounts    `json:"routing"`
	Errors        int64            `json:"errors"`
	ErrorsByCode  map[string]int64 `json:"errors_by_code"`
	InvalidInputs int64            `json:"invalid_inputs"`
	Termination   string           `json:"termination"`
	ExitCode      int              `json:"exit_code"`
	BytesRead     int64            `json:"bytes_read"`
	WallMS        int64            `json:"wall_ms"`
}

type EmittedCounts struct {
	ReflowInput int64 `json:"reflow_input"`
	Probe       int64 `json:"probe"`
}

type RoutingCounts struct {
	Normal     int64 `json:"normal"`
	Quarantine int64 `json:"quarantine"`
}

// KnownErrorCode bounds the published histogram vocabulary. Rejected codes
// must not be echoed into a diagnostic or used as a map key.
func KnownErrorCode(code string) bool {
	switch code {
	case output.ErrCodeAccessDenied, output.ErrCodeNotFound, output.ErrCodeTimeout,
		output.ErrCodeThrottled, output.ErrCodeInternal, output.ErrCodeProviderUnavailable,
		output.ErrCodeTransient, output.ErrCodeAlreadyExists, output.ErrCodeInvalidInput:
		return true
	default:
		return false
	}
}

// ParseSummary validates the exact control payload without treating it as an
// object or accepting arbitrary unknown record types. Errors never echo data.
func ParseSummary(raw json.RawMessage) (Summary, error) {
	var summary Summary
	if err := requiredSummaryFields(raw, []string{"inputs", "enumerated", "processed", "emitted", "routing", "errors", "errors_by_code", "invalid_inputs", "termination", "exit_code", "bytes_read", "wall_ms"}); err != nil {
		return summary, err
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	if err := requiredSummaryFields(fields["emitted"], []string{"reflow_input", "probe"}); err != nil {
		return summary, err
	}
	if err := requiredSummaryFields(fields["routing"], []string{"normal", "quarantine"}); err != nil {
		return summary, err
	}
	var histogram map[string]json.RawMessage
	if err := json.Unmarshal(fields["errors_by_code"], &histogram); err != nil || histogram == nil {
		return summary, fmt.Errorf("invalid probe summary error histogram")
	}
	for code, rawCount := range histogram {
		var count *int64
		if err := json.Unmarshal(rawCount, &count); err != nil || count == nil || *count < 0 || !KnownErrorCode(code) {
			return summary, fmt.Errorf("invalid probe summary error histogram")
		}
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&summary); err != nil {
		return Summary{}, fmt.Errorf("invalid probe summary payload")
	}
	if err := summary.Validate(); err != nil {
		return Summary{}, err
	}
	return summary, nil
}

func requiredSummaryFields(raw json.RawMessage, names []string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return fmt.Errorf("invalid probe summary object")
	}
	for _, name := range names {
		value, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("missing probe summary field")
		}
	}
	return nil
}

func (s Summary) Validate() error {
	for _, count := range []int64{s.Inputs, s.Enumerated, s.Processed, s.Emitted.ReflowInput, s.Emitted.Probe, s.Routing.Normal, s.Routing.Quarantine, s.Errors, s.InvalidInputs, s.BytesRead, s.WallMS} {
		if count < 0 {
			return fmt.Errorf("negative probe summary counter")
		}
	}
	if s.Processed > s.Enumerated || s.Emitted.Probe > s.Processed || s.Emitted.ReflowInput > s.Processed || s.Routing.Normal > s.Processed || s.Routing.Quarantine > s.Processed-s.Routing.Normal || s.InvalidInputs > s.Inputs || s.InvalidInputs > s.Errors || s.ErrorsByCode == nil {
		return fmt.Errorf("inconsistent probe summary counters")
	}
	remaining := s.Errors
	for code, count := range s.ErrorsByCode {
		if !KnownErrorCode(code) || count < 0 || count > remaining {
			return fmt.Errorf("invalid probe summary error histogram")
		}
		remaining -= count
		if count > 0 && (s.Termination == "completed" || s.Termination == "completed_with_errors") {
			switch code {
			case output.ErrCodeAccessDenied, output.ErrCodeTimeout, output.ErrCodeThrottled,
				output.ErrCodeTransient, output.ErrCodeProviderUnavailable, output.ErrCodeInvalidInput:
				return fmt.Errorf("inconsistent probe summary error classification")
			}
		}
	}
	if remaining != 0 {
		return fmt.Errorf("inconsistent probe summary error histogram")
	}
	switch s.Termination {
	case "completed":
		if s.ExitCode != 0 || s.Errors != 0 || s.InvalidInputs != 0 || s.Processed != s.Enumerated {
			return fmt.Errorf("inconsistent completed probe summary")
		}
	case "completed_with_errors":
		if s.ExitCode != 60 || s.Errors == 0 || s.InvalidInputs != 0 || s.Processed != s.Enumerated {
			return fmt.Errorf("inconsistent partial probe summary")
		}
	case "failed", "aborted":
		if s.ExitCode <= 0 || s.ExitCode > 255 || s.ExitCode == 60 {
			return fmt.Errorf("invalid failed probe summary exit")
		}
	default:
		return fmt.Errorf("invalid probe summary termination")
	}
	return nil
}
