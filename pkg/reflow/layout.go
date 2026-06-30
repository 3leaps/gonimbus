package reflow

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

// DestLayout is the resolved destination key/URI layout for a reflow run: the
// provider id plus the structural decomposition of the base URI (bucket+prefix
// for object stores, base directory for file). It produces destination keys and
// URIs deterministically — no provider I/O. Construct it with ParseDestLayout
// from the structured Destination's BaseURI. Experimental.
type DestLayout struct {
	ProviderID string
	BaseURI    string
	// Object-store decomposition.
	Bucket string
	Prefix string
	// File decomposition.
	BaseDir string
}

// ParseDestLayout decomposes a destination base URI into the layout the engine
// keys writes against. It mirrors the CLI's destination parsing: a file URI
// yields a cleaned base directory; an object-store URI yields bucket+prefix with
// a trailing slash. It performs no I/O.
func ParseDestLayout(baseURI string) (DestLayout, error) {
	raw := strings.TrimSpace(baseURI)
	if raw == "" {
		return DestLayout{}, fmt.Errorf("destination is required")
	}
	parsed, err := uri.ParseURI(raw)
	if err != nil {
		return DestLayout{}, err
	}
	if parsed.Provider == string(provider.ProviderFile) {
		baseDir := filepath.Clean(filepath.FromSlash(parsed.Key))
		uriStr := fileURI(baseDir)
		if !strings.HasSuffix(uriStr, "/") {
			uriStr += "/"
		}
		return DestLayout{ProviderID: string(provider.ProviderFile), BaseURI: uriStr, BaseDir: baseDir}, nil
	}
	if parsed.Provider != string(provider.ProviderS3) && parsed.Provider != string(provider.ProviderGCS) {
		return DestLayout{}, fmt.Errorf("provider %q is not supported", parsed.Provider)
	}
	if parsed.IsPattern() {
		return DestLayout{}, fmt.Errorf("destination must be a prefix URI")
	}
	if !parsed.IsPrefix() {
		parsed.Key = strings.TrimSuffix(parsed.Key, "/") + "/"
	}
	return DestLayout{
		ProviderID: parsed.Provider,
		BaseURI:    objectStoreURI(parsed.Provider, parsed.Bucket, parsed.Key),
		Bucket:     parsed.Bucket,
		Prefix:     parsed.Key,
	}, nil
}

// DestKey maps a destination-relative key to the full destination object key.
func (l DestLayout) DestKey(destRel string) string {
	destRel = strings.Trim(destRel, "/")
	switch l.ProviderID {
	case string(provider.ProviderS3), string(provider.ProviderGCS):
		key := strings.TrimPrefix(l.Prefix+destRel, "/")
		return strings.ReplaceAll(key, "//", "/")
	case string(provider.ProviderFile):
		key := filepath.Clean("/" + filepath.FromSlash(destRel))
		key = strings.TrimPrefix(key, string(filepath.Separator))
		if key == "." {
			return ""
		}
		return filepath.ToSlash(key)
	default:
		return destRel
	}
}

// DestURI renders the full destination URI for a destination object key.
func (l DestLayout) DestURI(destKey string) string {
	switch l.ProviderID {
	case string(provider.ProviderS3), string(provider.ProviderGCS):
		return objectStoreURI(l.ProviderID, l.Bucket, destKey)
	case string(provider.ProviderFile):
		full := filepath.Join(l.BaseDir, filepath.FromSlash(destKey))
		return fileURI(full)
	default:
		return ""
	}
}

// QuarantineDestRel joins a relative quarantine prefix with a source key to form
// the destination-relative key for a quarantined object.
func QuarantineDestRel(prefix string, sourceKey string) string {
	prefix = strings.Trim(prefix, "/")
	sourceKey = strings.Trim(sourceKey, "/")
	switch {
	case prefix == "":
		return sourceKey
	case sourceKey == "":
		return prefix
	default:
		return prefix + "/" + sourceKey
	}
}

// IsRelativeQuarantinePrefix reports whether prefix is a relative destination
// prefix (no leading slash, no URI scheme).
func IsRelativeQuarantinePrefix(prefix string) bool {
	prefix = strings.TrimSpace(prefix)
	if strings.HasPrefix(prefix, "/") {
		return false
	}
	u, err := url.Parse(prefix)
	return err != nil || u.Scheme == ""
}

func objectStoreURI(providerName, bucket, key string) string {
	scheme := providerName
	if scheme == string(provider.ProviderGCS) {
		scheme = "gs"
	}
	return fmt.Sprintf("%s://%s/%s", scheme, bucket, key)
}

func fileURI(path string) string {
	return "file://" + filepath.ToSlash(path)
}
