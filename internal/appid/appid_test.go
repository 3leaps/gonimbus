package appid

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/fulmenhq/gofulmen/appidentity"

	appidentityassets "github.com/3leaps/gonimbus/internal/assets/appidentity"
)

func prepareIdentityForTest(t *testing.T) {
	t.Helper()

	// Ensure per-test isolation.
	//
	// gofulmen caches identity per-process, and embedded identity registration is
	// also stored globally. Reset clears both.
	appidentity.Reset()

	// Re-register embedded identity so standalone behavior is always available
	// in tests.
	if err := appidentity.RegisterEmbeddedIdentityYAML(appidentityassets.YAML); err != nil {
		t.Fatalf("RegisterEmbeddedIdentityYAML: %v", err)
	}

	t.Cleanup(func() { appidentity.Reset() })
}

func TestGet_EmbeddedIdentityFallbackOutsideRepo(t *testing.T) {
	prepareIdentityForTest(t)
	t.Setenv(appidentity.EnvIdentityPath, "")

	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWD) })

	outside := t.TempDir()
	if err := os.Chdir(outside); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	identity, err := Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if identity.BinaryName == "" {
		t.Fatalf("expected BinaryName to be set")
	}
	if identity.EnvPrefix == "" {
		t.Fatalf("expected EnvPrefix to be set")
	}
}

func TestGet_EnvVarRemainsAuthoritative(t *testing.T) {
	prepareIdentityForTest(t)

	missing := filepath.Join(t.TempDir(), "missing-app.yaml")
	t.Setenv(appidentity.EnvIdentityPath, missing)

	_, err := Get(context.Background())
	if err == nil {
		t.Fatalf("expected error")
	}

	var notFound *appidentity.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected NotFoundError, got %T: %v", err, err)
	}
}
