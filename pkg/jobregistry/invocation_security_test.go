package jobregistry

import (
	"crypto/sha256"
	"strings"
	"testing"
)

func TestInvocationRejectsSignedMaterialInEveryForwardedString(t *testing.T) {
	base := IndexBuildInvocation{
		SchemaVersion:     IndexBuildInvocationVersion,
		ManifestPath:      "/tmp/index.yaml",
		ManifestSHA256:    strings.Repeat("a", sha256.Size*2),
		RequestedFormat:   "durable",
		EffectiveFormat:   "durable",
		ScopeWarnPrefixes: DefaultScopeWarnPrefixes,
		ScopeMaxPrefixes:  DefaultScopeMaxPrefixes,
	}
	sentinel := "https://objects.example.test/key?X-Amz-Signature=sentinel-secret"
	tests := map[string]func(*IndexBuildInvocation){
		"manifest_path":    func(v *IndexBuildInvocation) { v.ManifestPath = "/tmp/X-Amz-Signature=sentinel-secret" },
		"requested_format": func(v *IndexBuildInvocation) { v.RequestedFormat = sentinel },
		"config_path":      func(v *IndexBuildInvocation) { v.ConfigPath = sentinel },
		"data_root":        func(v *IndexBuildInvocation) { v.DataRoot = sentinel },
		"db_path":          func(v *IndexBuildInvocation) { v.DBPath = sentinel },
		"since":            func(v *IndexBuildInvocation) { v.Since = sentinel },
		"name":             func(v *IndexBuildInvocation) { v.Name = sentinel },
		"storage_provider": func(v *IndexBuildInvocation) { v.StorageProvider = sentinel },
		"cloud_provider":   func(v *IndexBuildInvocation) { v.CloudProvider = sentinel },
		"region_kind":      func(v *IndexBuildInvocation) { v.RegionKind = sentinel },
		"region":           func(v *IndexBuildInvocation) { v.Region = sentinel },
		"endpoint_host":    func(v *IndexBuildInvocation) { v.EndpointHost = sentinel },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			inv := base
			mutate(&inv)
			if err := validateIndexBuildInvocation(inv); err == nil {
				t.Fatal("expected signed-material rejection")
			}
		})
	}
}

func TestInvocationValidatesSinceAndNameBeforePersistence(t *testing.T) {
	base := IndexBuildInvocation{
		SchemaVersion:     IndexBuildInvocationVersion,
		ManifestPath:      "/tmp/index.yaml",
		ManifestSHA256:    strings.Repeat("a", sha256.Size*2),
		RequestedFormat:   "durable",
		EffectiveFormat:   "durable",
		ScopeWarnPrefixes: DefaultScopeWarnPrefixes,
		ScopeMaxPrefixes:  DefaultScopeMaxPrefixes,
	}
	for _, since := range []string{"auto", "2026-07-11", "2026-07-11T12:30:00Z"} {
		inv := base
		inv.Since = since
		if err := validateIndexBuildInvocation(inv); err != nil {
			t.Fatalf("valid since %q rejected: %v", since, err)
		}
	}
	inv := base
	inv.Since = "next Tuesday"
	if err := validateIndexBuildInvocation(inv); err == nil {
		t.Fatal("expected invalid since rejection")
	}
	inv = base
	inv.Name = strings.Repeat("x", 129)
	if err := validateIndexBuildInvocation(inv); err == nil {
		t.Fatal("expected oversized name rejection")
	}
}
