package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	apperrors "github.com/3leaps/gonimbus/internal/errors"
)

func TestServerUsesStandardErrorHandlers(t *testing.T) {
	srv := New("127.0.0.1", 0)

	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}

	var body apperrors.HTTPErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if body.Error.Code != "NOT_FOUND" {
		t.Fatalf("expected error code NOT_FOUND, got %s", body.Error.Code)
	}
}
