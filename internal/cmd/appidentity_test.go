package cmd

import (
	"context"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/internal/appid"
)

func TestAppIdentityLoading(t *testing.T) {
	t.Run("load app identity from .fulmen/app.yaml", func(t *testing.T) {
		// Load app identity the same way the application does
		ctx := context.Background()
		identity, err := appid.Get(ctx)

		// Should load successfully
		if err != nil {
			t.Fatalf("Failed to load app identity: %v", err)
		}

		if identity == nil {
			t.Fatal("App identity is nil")
		}

		// Log the full identity for debugging
		t.Logf("Loaded identity: %+v", identity)

		// Check all expected fields are populated
		expectedFields := map[string]string{
			"Vendor":     identity.Vendor,
			"BinaryName": identity.BinaryName,
			"EnvPrefix":  identity.EnvPrefix,
			"ConfigName": identity.ConfigName,
		}

		for fieldName, value := range expectedFields {
			if value == "" {
				t.Errorf("App identity field %s is empty (expected: non-empty)", fieldName)
			} else {
				t.Logf("✅ %s = '%s'", fieldName, value)
			}
		}

		// CDRL-safe invariants: these should remain true after refit.
		if identity.EnvPrefix == "" {
			t.Errorf("Expected env_prefix to be non-empty")
		}
		if identity.EnvPrefix != "" && !strings.HasSuffix(identity.EnvPrefix, "_") {
			t.Errorf("Expected env_prefix to end with underscore, got '%s'", identity.EnvPrefix)
		}
	})
}
