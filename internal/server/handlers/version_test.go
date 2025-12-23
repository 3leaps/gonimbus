package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fulmenhq/gofulmen/appidentity"
)

func TestVersionHandlerIncludesIdentityMetadata(t *testing.T) {
	SetVersionInfo("1.2.3", "abcd123", "2025-11-07T12:00:00Z")
	SetAppIdentity(&appidentity.Identity{
		BinaryName: "example-service",
	})

	req := httptest.NewRequest(http.MethodGet, "/version", nil)
	rec := httptest.NewRecorder()

	VersionHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp VersionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.App.Name != "example-service" {
		t.Fatalf("expected app name example-service, got %s", resp.App.Name)
	}

	if resp.App.Version != "1.2.3" {
		t.Fatalf("expected version 1.2.3, got %s", resp.App.Version)
	}

	if resp.App.Commit != "abcd123" {
		t.Fatalf("expected commit abcd123, got %s", resp.App.Commit)
	}

	if resp.Dependencies.Gofulmen == "" || resp.Dependencies.Crucible == "" {
		t.Fatal("expected dependency versions to be populated")
	}
}
