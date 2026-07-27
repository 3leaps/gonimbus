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
//
// URI must be the object AS SPELLED, not a canonicalized form. Parsing is
// escape-aware: an escaped metacharacter (`file\*.txt`) is a literal object-key
// character, and a caller that canonicalizes the URI first would strip the
// escape, leaving a spelling the engine reclassifies as a pattern and refuses.
type ObjectSource struct {
	Provider provider.Provider
	URI      string
}

// PrefixSource reflows every object under a listing prefix, optionally narrowed
// by a glob pattern, from an injected provider.
//
// URI must be the selector AS SPELLED, for the reason ObjectSource states: the
// parse is escape-aware, and a canonicalized spelling has already lost the
// escapes that decide whether a metacharacter is a glob or a literal key
// character. An exact-object URI is refused here — ObjectSource is that form —
// because listing under a whole object key would also admit its prefix siblings.
//
// Provider is required for dry-run as well as copy, unlike ObjectSource. A
// prefix names no object until it is listed, so List is the planning operation
// itself and there is no plan to produce without it.
//
// Enumeration is page-streamed: each listed object is dispatched as it is read,
// so a selector matching many objects does not accumulate a listing. A List
// failure partway through stops enumeration and fails the run with a
// SourceEnumerationError; see that type for what the run does and does not
// report in that case.
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
//
// The engine consumes the stream record-by-record (it never materializes the
// whole stream). The current implementation executes the dry-run and copy planes
// for S3 gonimbus.reflow.input.v1 records. Dry-run does not invoke Resolve; copy
// execution requires it so a source provider is obtained per record. A record
// outside the supported subset surfaces as an INVALID_INPUT event rather than
// being planned.
type RecordStreamSource struct {
	Records io.Reader
	Resolve SourceResolver
}

// SourceResolver maps a parsed source URI to a source provider handle for
// RecordStreamSource inputs. Returning an error fails (or skips, per source
// policy) the affected record.
//
// Concurrency contract: the engine invokes the resolver only from its serial
// planning stage, never concurrently — implementations (including lazy
// provider caches) need no internal synchronization. Record execution fans out
// to a worker pool, but each record's resolved handle is captured before
// dispatch, so the returned provider must tolerate concurrent object
// operations (the standing provider contract) while the resolver itself does
// not need to.
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
