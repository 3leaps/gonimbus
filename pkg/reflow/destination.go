package reflow

import (
	"fmt"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// Destination is the structured reflow target: an injected provider handle, the
// provider id (e.g. "s3", "file", "gcs"), and the base URI/prefix that rewritten
// keys are written under. Provider construction details (region, profile,
// endpoint, credentials) are a caller concern resolved before the handle is
// injected — embedders do not reconstruct command flags. Experimental.
type Destination struct {
	// Provider is the injected destination provider handle.
	Provider provider.Provider
	// ProviderID is the internal provider id ("s3", "file", "gcs").
	ProviderID string
	// BaseURI is the destination base prefix, e.g. s3://bucket/base/ or
	// file:///tmp/out/.
	BaseURI string
}

// String returns a redacted summary that never exposes the injected provider
// handle, which may hold credential material (TokenSource/CredentialsProvider).
// Mirrors the redaction discipline of the pkg/provider config types.
func (d Destination) String() string {
	return fmt.Sprintf("reflow.Destination{ProviderID:%q, BaseURI:%q, Provider:%s}",
		d.ProviderID, d.BaseURI, providerPresence(d.Provider == nil))
}

// GoString implements fmt %#v with the same redaction as String.
func (d Destination) GoString() string { return d.String() }

// providerPresence renders a handle's presence without exposing it.
func providerPresence(isNil bool) string {
	if isNil {
		return "<nil>"
	}
	return "<redacted>"
}
