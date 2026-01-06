package preflight

import (
	"context"
	"fmt"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// Mode defines how aggressive preflight checks are.
type Mode string

const (
	ModePlanOnly   Mode = "plan-only"
	ModeReadSafe   Mode = "read-safe"
	ModeWriteProbe Mode = "write-probe"
)

// ProbeStrategy selects a provider-specific write probe strategy.
type ProbeStrategy string

const (
	ProbeMultipartAbort ProbeStrategy = "multipart-abort"
	ProbePutDelete      ProbeStrategy = "put-delete"
)

// Spec controls how preflight checks are executed.
type Spec struct {
	Mode          Mode
	ProbeStrategy ProbeStrategy
	ProbePrefix   string
}

// Capability names are stable strings used in JSONL output.
const (
	CapSourceList = "source.list"
	CapSourceHead = "source.head"
)

// Crawl runs a minimal read-safe preflight for crawl jobs.
//
// For v0.1.x, this validates that listing is permitted under at least one derived prefix.
func Crawl(ctx context.Context, prov provider.Provider, prefixes []string, spec Spec) (*output.PreflightRecord, error) {
	rec := &output.PreflightRecord{
		Mode:          string(spec.Mode),
		ProbeStrategy: string(spec.ProbeStrategy),
		ProbePrefix:   spec.ProbePrefix,
		Results:       []output.PreflightCheckResult{},
	}

	if spec.Mode == ModePlanOnly {
		return rec, nil
	}

	prefix := ""
	if len(prefixes) > 0 {
		prefix = prefixes[0]
	}

	_, err := prov.List(ctx, provider.ListOptions{Prefix: prefix, MaxKeys: 1})
	if err != nil {
		code := normalizeErrorCode(err)
		rec.Results = append(rec.Results, output.PreflightCheckResult{
			Capability: CapSourceList,
			Allowed:    false,
			Method:     fmt.Sprintf("List(prefix=%q,maxKeys=1)", prefix),
			ErrorCode:  code,
			Detail:     err.Error(),
		})
		return rec, err
	}

	rec.Results = append(rec.Results, output.PreflightCheckResult{
		Capability: CapSourceList,
		Allowed:    true,
		Method:     fmt.Sprintf("List(prefix=%q,maxKeys=1)", prefix),
	})

	return rec, nil
}

func normalizeErrorCode(err error) string {
	switch {
	case provider.IsAccessDenied(err):
		return output.ErrCodeAccessDenied
	case provider.IsBucketNotFound(err), provider.IsNotFound(err):
		return output.ErrCodeNotFound
	case provider.IsThrottled(err):
		return output.ErrCodeThrottled
	case provider.IsInvalidCredentials(err):
		return output.ErrCodeAccessDenied
	case provider.IsProviderUnavailable(err):
		return output.ErrCodeInternal
	default:
		return output.ErrCodeInternal
	}
}
