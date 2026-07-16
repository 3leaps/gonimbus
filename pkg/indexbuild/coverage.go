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

// validateCoverageMatchesCrawlPlan enforces exact set equality between the
// supplied crawl-prefix plan and the coverage attestation prefixes, anchored to
// the exact Config.CrawlPrefixes that will drive the crawl. Coverage is what
// authorizes tombstones over rows loaded from the verified parent, so a plan
// and an attestation that disagree in either direction are refused before any
// side effect: a coverage prefix that is not in the plan (including a parent or
// base prefix rolled up over the plan) would tombstone keys the crawl never
// observed, and a plan prefix with no attestation would silently under-attest
// observed coverage. Equality is exact-prefix set equality in the same
// normalized rel_key space publication uses — never substring or parent-prefix
// containment, which is the roll-up vector.
//
// The gate applies only when a crawl-prefix plan is supplied. Builds that
// derive prefixes from the matcher keep the existing caller-attested coverage
// contract, and Retry re-publishes the sealed journals of a build whose plan
// was already validated here.
func validateCoverageMatchesCrawlPlan(baseURI string, crawlPrefixes []string, coverage []CoverageAttestation) error {
	if len(crawlPrefixes) == 0 {
		return nil
	}
	basePrefix, err := basePrefixFromURI(baseURI)
	if err != nil {
		return err
	}
	crawlSet := make(map[string]struct{}, len(crawlPrefixes))
	for _, raw := range crawlPrefixes {
		key, err := normalizeCoveragePrefix(basePrefix, raw)
		if err != nil {
			return fmt.Errorf("crawl prefix plan: %w", err)
		}
		if _, dup := crawlSet[key]; dup {
			return fmt.Errorf("crawl prefix plan has duplicate prefix %q after normalization", key)
		}
		crawlSet[key] = struct{}{}
	}
	if len(coverage) == 0 {
		return fmt.Errorf("coverage attestation is required when a crawl prefix plan is supplied")
	}
	covSet := make(map[string]struct{}, len(coverage))
	for i, entry := range coverage {
		if entry.Scope == nil {
			return fmt.Errorf("coverage[%d] scope is required when a crawl prefix plan is supplied", i)
		}
		if entry.Scope.Window != nil {
			return fmt.Errorf("coverage[%d] must not set a temporal window under a crawl prefix plan", i)
		}
		key, err := normalizeCoveragePrefix(basePrefix, entry.Scope.Prefix)
		if err != nil {
			return err
		}
		if _, dup := covSet[key]; dup {
			return fmt.Errorf("coverage has duplicate prefix %q", key)
		}
		covSet[key] = struct{}{}
		if _, ok := crawlSet[key]; !ok {
			return fmt.Errorf("coverage prefix %q is not in the crawl prefix plan (roll-up or extra coverage cannot authorize tombstones)", key)
		}
	}
	for key := range crawlSet {
		if _, ok := covSet[key]; !ok {
			return fmt.Errorf("coverage is missing crawl prefix plan entry %q (under-attested plan prefix)", key)
		}
	}
	return nil
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
