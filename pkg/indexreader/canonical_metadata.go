package indexreader

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/3leaps/gonimbus/pkg/manifest"
)

const (
	canonicalIdentityName = "identity.json"
	canonicalManifestName = "manifest.json"
)

// canonicalMetadataBeforeReplace is a test-only interposition point. The
// platform publisher invokes it after the durable temp file exists and before
// the final no-follow destination check and atomic replace.
var canonicalMetadataBeforeReplace func(path string) error

func publishCanonicalMetadata(dir, name string, data []byte, check func() error) error {
	dir = filepath.Clean(strings.TrimSpace(dir))
	if dir == "." || !isCanonicalMetadataName(name) || len(data) == 0 || check == nil {
		return fmt.Errorf("canonical metadata directory, name, payload, and authority check are required")
	}
	if err := check(); err != nil {
		return err
	}
	if err := publishCanonicalMetadataPlatform(dir, name, data, check); err != nil {
		return fmt.Errorf("publish canonical %s: %w", name, err)
	}
	return nil
}

func canonicalManifestData(doc *manifest.IndexManifest) ([]byte, error) {
	if doc == nil {
		return nil, fmt.Errorf("canonical manifest is required")
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal canonical manifest: %w", err)
	}
	return append(data, '\n'), nil
}

func isCanonicalMetadataName(name string) bool {
	return name == canonicalIdentityName || name == canonicalManifestName
}
