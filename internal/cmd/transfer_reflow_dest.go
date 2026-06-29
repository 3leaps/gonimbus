package cmd

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

type reflowDestKeyArbiter struct {
	mu    sync.Mutex
	gates map[string]*reflowDestKeyGate
}

type reflowDestKeyGate struct {
	mu       sync.Mutex
	refs     int
	observed bool
}

func newReflowDestKeyArbiter() *reflowDestKeyArbiter {
	return &reflowDestKeyArbiter{gates: map[string]*reflowDestKeyGate{}}
}

func (a *reflowDestKeyArbiter) acquire(key string) (*reflowDestKeyGate, func()) {
	a.mu.Lock()
	g, ok := a.gates[key]
	if !ok {
		g = &reflowDestKeyGate{}
		a.gates[key] = g
	}
	g.refs++
	a.mu.Unlock()

	g.mu.Lock()
	return g, func() {
		g.mu.Unlock()
		a.mu.Lock()
		defer a.mu.Unlock()
		g.refs--
		if g.refs == 0 && a.gates[key] == g {
			delete(a.gates, key)
		}
	}
}

func (a *reflowDestKeyArbiter) activeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.gates)
}

type reflowDestSpec struct {
	Provider string
	BaseURI  string

	// Object-store destination
	Bucket         string
	Prefix         string
	Region         string
	Profile        string
	Endpoint       string
	ForcePathStyle bool
	GCPProject     string

	// File destination
	BaseDir string
}

func parseReflowDest(raw string) (*reflowDestSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("destination is required")
	}

	parsed, err := uri.ParseURI(raw)
	if err != nil {
		return nil, err
	}
	if parsed.Provider == string(provider.ProviderFile) {
		baseDir := filepath.Clean(filepath.FromSlash(parsed.Key))
		baseURI := fileURI(baseDir)
		if !strings.HasSuffix(baseURI, "/") {
			baseURI += "/"
		}
		return &reflowDestSpec{Provider: string(provider.ProviderFile), BaseURI: baseURI, BaseDir: baseDir}, nil
	}

	if parsed.Provider != string(provider.ProviderS3) && parsed.Provider != string(provider.ProviderGCS) {
		return nil, fmt.Errorf("provider %q is not supported", parsed.Provider)
	}
	if parsed.IsPattern() {
		return nil, fmt.Errorf("destination must be a prefix URI")
	}
	if !parsed.IsPrefix() {
		parsed.Key = strings.TrimSuffix(parsed.Key, "/") + "/"
	}

	baseURI := objectStoreURI(parsed.Provider, parsed.Bucket, parsed.Key)
	return &reflowDestSpec{Provider: parsed.Provider, BaseURI: baseURI, Bucket: parsed.Bucket, Prefix: parsed.Key}, nil
}

func objectStoreURI(providerName, bucket, key string) string {
	scheme := providerName
	if scheme == string(provider.ProviderGCS) {
		scheme = "gs"
	}
	return fmt.Sprintf("%s://%s/%s", scheme, bucket, key)
}

func fileURI(path string) string {
	path = filepath.ToSlash(path)
	// For unix absolute paths, this produces file:///...
	return "file://" + path
}

func buildReflowDestKey(dest *reflowDestSpec, destRel string) string {
	destRel = strings.Trim(destRel, "/")
	if dest == nil {
		return destRel
	}
	switch dest.Provider {
	case string(provider.ProviderS3), string(provider.ProviderGCS):
		key := strings.TrimPrefix(dest.Prefix+destRel, "/")
		key = strings.ReplaceAll(key, "//", "/")
		return key
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

func buildReflowDestURI(dest *reflowDestSpec, destKey string) string {
	if dest == nil {
		return ""
	}
	switch dest.Provider {
	case string(provider.ProviderS3), string(provider.ProviderGCS):
		return objectStoreURI(dest.Provider, dest.Bucket, destKey)
	case string(provider.ProviderFile):
		full := filepath.Join(dest.BaseDir, filepath.FromSlash(destKey))
		return fileURI(full)
	default:
		return ""
	}
}

func ensureTrailingSlash(s string) string {
	if s == "" || strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}

func buildQuarantineDestRel(prefix string, sourceKey string) string {
	prefix = strings.Trim(prefix, "/")
	sourceKey = strings.Trim(sourceKey, "/")
	if prefix == "" {
		return sourceKey
	}
	if sourceKey == "" {
		return prefix
	}
	return prefix + "/" + sourceKey
}

func isRelativeQuarantinePrefix(prefix string) bool {
	prefix = strings.TrimSpace(prefix)
	if strings.HasPrefix(prefix, "/") {
		return false
	}
	u, err := url.Parse(prefix)
	return err != nil || u.Scheme == ""
}
