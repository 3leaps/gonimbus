package jobregistry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	DefaultScopeWarnPrefixes = 10000
	DefaultScopeMaxPrefixes  = 50000
)

var safeManagedJobName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 ._-]{0,127}$`)

// PrepareIndexBuildInvocation canonicalizes and binds an invocation to the
// exact manifest bytes that were present when the job was accepted.
func PrepareIndexBuildInvocation(manifestPath, name string, in *IndexBuildInvocation) (*IndexBuildInvocation, string, error) {
	manifestPath = strings.TrimSpace(manifestPath)
	if manifestPath == "" {
		return nil, "", fmt.Errorf("manifest path is required")
	}
	absManifest, err := filepath.Abs(manifestPath)
	if err != nil {
		return nil, "", fmt.Errorf("resolve manifest path: %w", err)
	}
	digest, err := ManifestSHA256(absManifest)
	if err != nil {
		return nil, "", err
	}

	inv := IndexBuildInvocation{}
	if in != nil {
		inv = *in
	}
	inv.SchemaVersion = IndexBuildInvocationVersion
	inv.ManifestPath = absManifest
	inv.ManifestSHA256 = digest
	inv.Name = strings.TrimSpace(name)
	inv.RequestedFormat = normalizeFormat(inv.RequestedFormat)
	inv.EffectiveFormat = normalizeFormat(inv.EffectiveFormat)
	inv.ConfigPath = strings.TrimSpace(inv.ConfigPath)
	if strings.TrimSpace(inv.DataRoot) != "" {
		inv.DataRoot = filepath.Clean(strings.TrimSpace(inv.DataRoot))
	}
	if inv.EffectiveFormat == "" {
		inv.EffectiveFormat = "durable"
	}
	if inv.RequestedFormat == "" {
		inv.RequestedFormat = inv.EffectiveFormat
	}
	if inv.ScopeWarnPrefixes == 0 && in == nil {
		inv.ScopeWarnPrefixes = DefaultScopeWarnPrefixes
	}
	if inv.ScopeMaxPrefixes == 0 && in == nil {
		inv.ScopeMaxPrefixes = DefaultScopeMaxPrefixes
	}
	trimIndexBuildInvocation(&inv)
	if err := validateIndexBuildInvocation(inv); err != nil {
		return nil, "", err
	}
	fingerprint, err := IndexBuildInvocationFingerprint(inv)
	if err != nil {
		return nil, "", err
	}
	return &inv, fingerprint, nil
}

// ManifestSHA256 hashes manifest content without persisting the content itself.
func ManifestSHA256(path string) (string, error) {
	_, digest, err := ReadManifestBytesAndSHA256(path)
	return digest, err
}

// ReadManifestBytesAndSHA256 returns one read of the manifest and the digest
// of those exact bytes so callers can parse without reopening the path.
func ReadManifestBytesAndSHA256(path string) ([]byte, string, error) {
	f, err := os.Open(path) // #nosec G304 -- caller-supplied manifest path is the command input.
	if err != nil {
		return nil, "", fmt.Errorf("open manifest: %w", err)
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, "", fmt.Errorf("read manifest: %w", err)
	}
	sum := sha256.Sum256(b)
	return b, hex.EncodeToString(sum[:]), nil
}

func IndexBuildInvocationFingerprint(inv IndexBuildInvocation) (string, error) {
	if err := validateIndexBuildInvocation(inv); err != nil {
		return "", err
	}
	// RequestedFormat is operator provenance; dedupe keys the effective child
	// behavior so the deprecated alias and explicit --format durable converge.
	inv.RequestedFormat = inv.EffectiveFormat
	b, err := json.Marshal(inv)
	if err != nil {
		return "", fmt.Errorf("marshal effective invocation: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func trimIndexBuildInvocation(inv *IndexBuildInvocation) {
	inv.ManifestPath = filepath.Clean(strings.TrimSpace(inv.ManifestPath))
	inv.ManifestSHA256 = strings.ToLower(strings.TrimSpace(inv.ManifestSHA256))
	inv.ConfigPath = strings.TrimSpace(inv.ConfigPath)
	if strings.TrimSpace(inv.DataRoot) != "" {
		inv.DataRoot = filepath.Clean(strings.TrimSpace(inv.DataRoot))
	}
	inv.RequestedFormat = normalizeFormat(inv.RequestedFormat)
	inv.EffectiveFormat = normalizeFormat(inv.EffectiveFormat)
	inv.DBPath = strings.TrimSpace(inv.DBPath)
	inv.Since = strings.TrimSpace(inv.Since)
	inv.Name = strings.TrimSpace(inv.Name)
	inv.StorageProvider = strings.TrimSpace(inv.StorageProvider)
	inv.CloudProvider = strings.TrimSpace(inv.CloudProvider)
	inv.RegionKind = strings.TrimSpace(inv.RegionKind)
	inv.Region = strings.TrimSpace(inv.Region)
	inv.EndpointHost = strings.TrimSpace(inv.EndpointHost)
}

func normalizeFormat(v string) string { return strings.ToLower(strings.TrimSpace(v)) }

func validateIndexBuildInvocation(inv IndexBuildInvocation) error {
	if inv.SchemaVersion != IndexBuildInvocationVersion {
		return fmt.Errorf("unsupported effective invocation schema_version %d", inv.SchemaVersion)
	}
	if !filepath.IsAbs(inv.ManifestPath) {
		return fmt.Errorf("effective invocation manifest_path must be absolute")
	}
	if inv.DataRoot != "" && !filepath.IsAbs(inv.DataRoot) {
		return fmt.Errorf("effective invocation data_root must be absolute")
	}
	if len(inv.ManifestSHA256) != sha256.Size*2 {
		return fmt.Errorf("effective invocation manifest_sha256 is invalid")
	}
	switch inv.EffectiveFormat {
	case "durable", "sqlite", "both":
	default:
		return fmt.Errorf("effective invocation format must be durable, sqlite, or both")
	}
	if err := ValidateManagedJobName(inv.Name); err != nil {
		return err
	}
	if err := ValidateIndexBuildSince(inv.Since); err != nil {
		return err
	}
	if strings.ContainsAny(inv.EndpointHost, "@/?#\r\n") {
		return fmt.Errorf("effective invocation endpoint_host must be host[:port] without userinfo, path, or query")
	}
	if strings.Contains(inv.DBPath, "://") {
		u, err := url.Parse(inv.DBPath)
		if err != nil {
			return fmt.Errorf("effective invocation db_path URL is invalid: %w", err)
		}
		if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("effective invocation db_path URL must not contain userinfo, query, or fragment")
		}
	}
	if err := validateNoSensitiveInvocationStrings(inv); err != nil {
		return err
	}
	return nil
}

// ValidateManagedJobName validates the bounded display-only name accepted by
// managed build APIs before it can be persisted or forwarded on argv.
func ValidateManagedJobName(value string) error {
	if value != "" && !safeManagedJobName.MatchString(value) {
		return fmt.Errorf("effective invocation name must be 1-128 safe display characters")
	}
	return nil
}

// ValidateIndexBuildSince validates the typed incremental lower bound before
// it can be persisted or forwarded on argv.
func ValidateIndexBuildSince(value string) error {
	if value != "" && !strings.EqualFold(value, "auto") {
		if _, err := parseInvocationSince(value); err != nil {
			return fmt.Errorf("effective invocation since must be auto, an ISO 8601 date, or RFC3339 timestamp")
		}
	}
	return nil
}

func parseInvocationSince(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	return time.Parse("2006-01-02", value)
}

func validateNoSensitiveInvocationStrings(inv IndexBuildInvocation) error {
	fields := []struct {
		name  string
		value string
	}{
		{"manifest_path", inv.ManifestPath},
		{"requested_format", inv.RequestedFormat},
		{"effective_format", inv.EffectiveFormat},
		{"config_path", inv.ConfigPath},
		{"data_root", inv.DataRoot},
		{"db_path", inv.DBPath},
		{"since", inv.Since},
		{"name", inv.Name},
		{"storage_provider", inv.StorageProvider},
		{"cloud_provider", inv.CloudProvider},
		{"region_kind", inv.RegionKind},
		{"region", inv.Region},
		{"endpoint_host", inv.EndpointHost},
	}
	for _, field := range fields {
		if containsCredentialOrSignedMaterial(field.value) {
			return fmt.Errorf("effective invocation %s contains credential or signed material", field.name)
		}
	}
	return nil
}

func containsCredentialOrSignedMaterial(value string) bool {
	lower := strings.ToLower(value)
	if strings.ContainsAny(value, "\r\n") {
		return true
	}
	for _, needle := range []string{
		"authorization: bearer", "x-amz-signature", "x-amz-credential", "x-amz-security-token",
		"x-goog-signature", "x-goog-credential", "x-goog-security-token", "sharedaccesssignature",
		"aws_secret_access_key", "aws_session_token", "access_token=", "refresh_token=",
		"client_secret=", "password=", "passphrase=", "secret=", "token=", "sig=",
	} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	if strings.Contains(value, "://") {
		if parsed, err := url.Parse(value); err == nil {
			if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
				return true
			}
		}
	}
	return false
}
