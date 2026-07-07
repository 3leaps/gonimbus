package indexbuild

import (
	"fmt"
	"strings"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

func normalizeCoverageForBaseURI(baseURI string, coverage []CoverageAttestation) ([]CoverageAttestation, error) {
	if strings.TrimSpace(baseURI) == "" {
		return copyCoverage(coverage), nil
	}
	basePrefix, err := basePrefixFromURI(baseURI)
	if err != nil {
		return nil, err
	}
	out := copyCoverage(coverage)
	for i := range out {
		if out[i].Scope != nil {
			prefix, err := normalizeCoveragePrefix(basePrefix, out[i].Scope.Prefix)
			if err != nil {
				return nil, err
			}
			out[i].Scope.Prefix = prefix
		}
		for j := range out[i].Gaps {
			prefix, err := normalizeCoveragePrefix(basePrefix, out[i].Gaps[j].Prefix)
			if err != nil {
				return nil, err
			}
			out[i].Gaps[j].Prefix = prefix
		}
	}
	return out, nil
}

func normalizeCoveragePrefix(basePrefix string, prefix string) (string, error) {
	prefix = strings.TrimPrefix(strings.TrimSpace(prefix), "/")
	basePrefix = strings.TrimPrefix(strings.TrimSpace(basePrefix), "/")
	basePrefix = strings.TrimSuffix(basePrefix, "/")
	if prefix == indexsubstrate.RelativeRootScopePrefix {
		return prefix, nil
	}
	if basePrefix == "" {
		if prefix == "" {
			return indexsubstrate.RelativeRootScopePrefix, nil
		}
		return prefix, nil
	}
	if prefix == basePrefix || prefix == basePrefix+"/" {
		return indexsubstrate.RelativeRootScopePrefix, nil
	}
	baseWithSlash := basePrefix + "/"
	if strings.HasPrefix(prefix, baseWithSlash) {
		rel := strings.TrimPrefix(prefix, baseWithSlash)
		if rel == "" {
			return indexsubstrate.RelativeRootScopePrefix, nil
		}
		return rel, nil
	}
	return "", fmt.Errorf("coverage prefix %q is outside base_uri prefix %q", prefix, baseWithSlash)
}

func copyCoverage(in []CoverageAttestation) []CoverageAttestation {
	if len(in) == 0 {
		return nil
	}
	out := make([]CoverageAttestation, len(in))
	for i := range in {
		out[i] = in[i]
		if in[i].Scope != nil {
			scope := *in[i].Scope
			out[i].Scope = &scope
		}
		if len(in[i].Gaps) > 0 {
			out[i].Gaps = append([]Scope(nil), in[i].Gaps...)
		}
	}
	return out
}
