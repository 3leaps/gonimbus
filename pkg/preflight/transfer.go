package preflight

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

const (
	CapSourceRead = "source.read"
	CapTargetHead = "target.head"
)

type TransferOptions struct {
	RequireSourceRead       bool
	RequireTargetHead       bool
	RequireTargetWriteProbe bool
}

// Transfer performs staged preflight checks for transfer operations.
//
// Ordering (fail-fast): target write probe (when enabled) → source list → source read → target head.
func Transfer(ctx context.Context, src provider.Provider, dst provider.Provider, prefixes []string, spec Spec, opts TransferOptions) (*output.PreflightRecord, error) {
	rec := &output.PreflightRecord{
		Mode:          string(spec.Mode),
		ProbeStrategy: string(spec.ProbeStrategy),
		ProbePrefix:   spec.ProbePrefix,
		Results:       []output.PreflightCheckResult{},
	}

	if spec.Mode == ModePlanOnly {
		return rec, nil
	}

	if opts.RequireTargetWriteProbe {
		probeRec, err := WriteProbe(ctx, dst, spec)
		rec.Results = append(rec.Results, probeRec.Results...)
		if err != nil {
			return rec, err
		}
	}

	// Source list
	prefix := ""
	if len(prefixes) > 0 {
		prefix = prefixes[0]
	}
	_, err := src.List(ctx, provider.ListOptions{Prefix: prefix, MaxKeys: 1})
	if err != nil {
		rec.Results = append(rec.Results, output.PreflightCheckResult{
			Capability: CapSourceList,
			Allowed:    false,
			Method:     fmt.Sprintf("List(prefix=%q,maxKeys=1)", prefix),
			ErrorCode:  normalizeErrorCode(err),
			Detail:     err.Error(),
		})
		return rec, err
	}
	rec.Results = append(rec.Results, output.PreflightCheckResult{
		Capability: CapSourceList,
		Allowed:    true,
		Method:     fmt.Sprintf("List(prefix=%q,maxKeys=1)", prefix),
	})

	if opts.RequireSourceRead {
		getter, ok := src.(provider.ObjectGetter)
		if !ok {
			return rec, fmt.Errorf("source provider does not support GetObject")
		}

		probeKey := joinPrefix(prefix, "_gonimbus/preflight-"+uuid.NewString())
		body, _, getErr := getter.GetObject(ctx, probeKey)
		if getErr == nil {
			_ = body.Close()
		}
		if getErr != nil && !provider.IsNotFound(getErr) {
			rec.Results = append(rec.Results, output.PreflightCheckResult{
				Capability: CapSourceRead,
				Allowed:    false,
				Method:     "GetObject(random)",
				ErrorCode:  normalizeErrorCode(getErr),
				Detail:     getErr.Error(),
			})
			return rec, getErr
		}
		rec.Results = append(rec.Results, output.PreflightCheckResult{
			Capability: CapSourceRead,
			Allowed:    true,
			Method:     "GetObject(random)",
		})
	}

	if opts.RequireTargetHead {
		probePrefix := spec.ProbePrefix
		if probePrefix == "" {
			probePrefix = "_gonimbus/probe/"
		}
		probeKey := joinPrefix(probePrefix, "head-"+uuid.NewString())

		_, headErr := dst.Head(ctx, probeKey)
		if headErr != nil && !provider.IsNotFound(headErr) {
			rec.Results = append(rec.Results, output.PreflightCheckResult{
				Capability: CapTargetHead,
				Allowed:    false,
				Method:     "Head(random)",
				ErrorCode:  normalizeErrorCode(headErr),
				Detail:     headErr.Error(),
			})
			return rec, headErr
		}
		rec.Results = append(rec.Results, output.PreflightCheckResult{
			Capability: CapTargetHead,
			Allowed:    true,
			Method:     "Head(random)",
		})
	}

	return rec, nil
}

func joinPrefix(prefix, suffix string) string {
	if prefix == "" {
		return strings.TrimPrefix(suffix, "/")
	}
	if strings.HasSuffix(prefix, "/") {
		return prefix + strings.TrimPrefix(suffix, "/")
	}
	return prefix + "/" + strings.TrimPrefix(suffix, "/")
}
