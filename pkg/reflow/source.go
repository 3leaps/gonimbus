package reflow

import (
	"context"
	"fmt"
	"io"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// Source identifies what a Runner reads. It is a closed set of typed forms —
// ObjectSource, PrefixSource, FileTreeSource, and RecordStreamSource — so an
// embedder selects a form rather than reconstructing command flags. Experimental.
type Source interface {
	isReflowSource()
}

// ObjectSource reflows a single object addressed by URI from an injected provider.
type ObjectSource struct {
	Provider provider.Provider
	URI      string
}

// PrefixSource reflows every object under a listing prefix from an injected
// provider.
type PrefixSource struct {
	Provider provider.Provider
	URI      string
}

// FileTreeSource reflows a local filesystem tree rooted at Root.
type FileTreeSource struct {
	Root string
}

// RecordStreamSource reflows a preselected stream of reflow-input records — the
// library equivalent of `crawl --emit reflow-input | transfer reflow --stdin`.
// Because records may span buckets/providers, source providers are obtained
// per-record through Resolve.
type RecordStreamSource struct {
	Records io.Reader
	Resolve SourceResolver
}

// SourceResolver maps a parsed source URI to a source provider handle for
// RecordStreamSource inputs. Returning an error fails (or skips, per source
// policy) the affected record.
type SourceResolver func(ctx context.Context, sourceURI string) (provider.Provider, error)

func (ObjectSource) isReflowSource()       {}
func (PrefixSource) isReflowSource()       {}
func (FileTreeSource) isReflowSource()     {}
func (RecordStreamSource) isReflowSource() {}

// String returns a redacted summary that never exposes the injected provider
// handle, which may hold credential material.
func (s ObjectSource) String() string {
	return fmt.Sprintf("reflow.ObjectSource{URI:%q, Provider:%s}", s.URI, providerPresence(s.Provider == nil))
}

// GoString implements fmt %#v with the same redaction as String.
func (s ObjectSource) GoString() string { return s.String() }

// String returns a redacted summary that never exposes the injected provider
// handle, which may hold credential material.
func (s PrefixSource) String() string {
	return fmt.Sprintf("reflow.PrefixSource{URI:%q, Provider:%s}", s.URI, providerPresence(s.Provider == nil))
}

// GoString implements fmt %#v with the same redaction as String.
func (s PrefixSource) GoString() string { return s.String() }

// String renders the root by presence only: a local filesystem path is treated
// as disclosure-sensitive, consistent with the engine's local-path discretion.
func (s FileTreeSource) String() string {
	return fmt.Sprintf("reflow.FileTreeSource{Root:%s}", pathPresence(s.Root == ""))
}

// GoString implements fmt %#v with the same redaction as String.
func (s FileTreeSource) GoString() string { return s.String() }

// String renders presence only: the record stream and resolver may close over
// credential material, so neither is formatted by value.
func (s RecordStreamSource) String() string {
	return fmt.Sprintf("reflow.RecordStreamSource{Records:%s, Resolve:%s}",
		readerPresence(s.Records == nil), funcPresence(s.Resolve == nil))
}

// GoString implements fmt %#v with the same redaction as String.
func (s RecordStreamSource) GoString() string { return s.String() }

func readerPresence(isNil bool) string {
	if isNil {
		return "<nil>"
	}
	return "<set>"
}

func funcPresence(isNil bool) string {
	if isNil {
		return "<nil>"
	}
	return "<set>"
}

func pathPresence(isEmpty bool) string {
	if isEmpty {
		return "<empty>"
	}
	return "<set>"
}
