// Package runbudget bounds the load one run may place on a source provider,
// shared across every consumer that touches the same provider quota.
//
// Experimental: the admission contract will churn while its consumers are
// built.
//
// The problem it exists for: a run that enumerates in N parallel lanes and
// reads content from those lanes has N places that can issue provider requests,
// and a provider that rate-limits does not care how the client is structured.
// Sizing concurrency per lane makes lane count a load multiplier, so adding
// lanes stops being a topology choice and starts being a way to get throttled.
// Here, lane count is topology: consumers draw on one budget per quota domain,
// so adding lanes redistributes work rather than enlarging the fleet.
//
// The package is neutral by construction. It knows nothing about plans, lanes,
// probes, or transfers — only that some number of consumers want to start
// requests against some quota domain — so the enumerating side and the mutating
// side can both depend on it without depending on each other.
package runbudget

import (
	"fmt"
	"sort"
	"unicode/utf8"
)

// Domain identifies a provider quota: the scope within which requests contend
// with each other for rate and concurrency.
//
// It is supplied by a provider adapter and opaque here. Only the provider knows
// what its service actually meters — one account, one bucket, one endpoint, or
// some combination — so this package never parses an ID, normalizes it, or
// infers that one domain contains another. Inferring a hierarchy would be this
// package guessing at provider policy.
//
// Equality is exact over both fields. Two requests share a budget if and only if
// their domains are equal.
//
// A Domain is deliberately not the plan's base identity. Base identity answers
// "is this the same work" and belongs to resume; a quota domain answers "does
// this request contend with that one". A run enumerating one bucket may draw on
// a domain covering an entire account, and two runs against different buckets
// may share one. Neither is derivable from the other.
//
// A Domain must never carry credentials. It describes service topology, not the
// principal making the request.
type Domain struct {
	// Version is the provider's domain-naming scheme. A provider that changes
	// how it names domains bumps this, and domains of different versions are
	// never equal — so a scheme change degrades to separate budgets rather than
	// to a silently wrong sharing decision.
	Version int `json:"version"`
	// ID names the quota within that scheme.
	ID string `json:"id"`
}

// Validate reports whether a domain is usable as a budget key.
//
// It checks that an ID exists and survives serialization unchanged; it does not
// trim, case-fold, or otherwise normalize a valid ID. Exact equality is the
// contract, so altering the bytes would silently change which requests contend
// with each other — and a whitespace-only ID is a legal opaque value, not an
// absent one.
func (d Domain) Validate() error {
	if d.Version < 1 {
		return fmt.Errorf("runbudget: domain version must be positive, got %d", d.Version)
	}
	if d.ID == "" {
		return fmt.Errorf("runbudget: domain id must not be empty")
	}
	// A Domain is JSON-shaped, and encoding invalid UTF-8 replaces the offending
	// bytes with U+FFFD. Two distinct domains could then come back equal, merging
	// quotas that do not share one, or one domain could come back different from
	// itself and split a quota that does.
	if !utf8.ValidString(d.ID) {
		return fmt.Errorf("runbudget: domain id is not valid UTF-8; it would not survive serialization unchanged")
	}
	return nil
}

// Equal reports whether two domains name the same quota.
func (d Domain) Equal(other Domain) bool {
	return d.Version == other.Version && d.ID == other.ID
}

// String renders a domain for diagnostics.
//
// The ID is quoted. It is provider-supplied opaque text that reaches logs, so an
// ID carrying newlines or control characters could otherwise forge log lines.
func (d Domain) String() string { return fmt.Sprintf("v%d/%q", d.Version, d.ID) }

// less orders domains deterministically. Ordering exists so diagnostics and
// snapshots are stable, not to make acquisition safe: acquisition takes every
// domain a request needs under one lock, so there is no lock order to get wrong.
func (d Domain) less(other Domain) bool {
	if d.Version != other.Version {
		return d.Version < other.Version
	}
	return d.ID < other.ID
}

func sortDomains(domains []Domain) {
	sort.Slice(domains, func(i, j int) bool { return domains[i].less(domains[j]) })
}

// OpClass is the kind of provider request being started.
//
// Classes are accounted separately because they are not interchangeable load: a
// listing and a ranged read of a large object consume different provider
// resources, and a budget that pooled them would let one starve the other while
// reporting healthy.
type OpClass string

const (
	// OpList is an object listing request.
	OpList OpClass = "list"
	// OpHead is a metadata request.
	OpHead OpClass = "head"
	// OpGet is a whole-object or ranged content read.
	OpGet OpClass = "get"
	// OpCopySourceRead is the source-side read a server-side copy performs.
	// It is distinct from OpGet because it is charged to the source provider
	// without the client ever holding the body.
	OpCopySourceRead OpClass = "copy_source_read"
)

var knownClasses = map[OpClass]struct{}{
	OpList:           {},
	OpHead:           {},
	OpGet:            {},
	OpCopySourceRead: {},
}

// Validate reports whether an operation class is one this package accounts.
//
// Unknown classes are refused rather than treated as unlimited. A budget that
// silently ignored a misspelled class would report itself as bounding load it
// was not bounding at all.
func (c OpClass) Validate() error {
	if _, ok := knownClasses[c]; !ok {
		return fmt.Errorf("runbudget: unknown operation class %q", c)
	}
	return nil
}
