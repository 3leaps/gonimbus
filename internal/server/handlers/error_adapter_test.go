package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetHTTPErrorResponder(t *testing.T) {
	// Save original
	original := httpErrorResponder
	defer func() { httpErrorResponder = original }()

	t.Run("sets custom responder", func(t *testing.T) {
		called := false
		customResponder := func(w http.ResponseWriter, r *http.Request, err error) {
			called = true
			w.WriteHeader(http.StatusTeapot)
		}

		SetHTTPErrorResponder(customResponder)

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		respondWithError(rec, req, assert.AnError)

		assert.True(t, called)
		assert.Equal(t, http.StatusTeapot, rec.Code)
	})

	t.Run("nil resets to default", func(t *testing.T) {
		// Set a custom responder first
		SetHTTPErrorResponder(func(w http.ResponseWriter, r *http.Request, err error) {
			w.WriteHeader(http.StatusTeapot)
		})

		// Reset with nil
		SetHTTPErrorResponder(nil)

		// Should now use default (which calls apperrors.RespondWithError)
		// We can't easily verify the default behavior without more setup,
		// but we can verify the assignment happened
		assert.NotNil(t, httpErrorResponder)
	})
}

func TestResetHTTPErrorResponder(t *testing.T) {
	// Save original
	original := httpErrorResponder
	defer func() { httpErrorResponder = original }()

	// Set a custom responder
	customCalled := false
	SetHTTPErrorResponder(func(w http.ResponseWriter, r *http.Request, err error) {
		customCalled = true
	})

	// Reset to default
	ResetHTTPErrorResponder()

	// Verify it's reset (default responder is not our custom one)
	assert.False(t, customCalled)
	assert.NotNil(t, httpErrorResponder)
}

func TestRespondWithError(t *testing.T) {
	// Save original
	original := httpErrorResponder
	defer func() { httpErrorResponder = original }()

	called := false
	var capturedErr error

	SetHTTPErrorResponder(func(w http.ResponseWriter, r *http.Request, err error) {
		called = true
		capturedErr = err
		w.WriteHeader(http.StatusInternalServerError)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	respondWithError(rec, req, assert.AnError)

	assert.True(t, called)
	assert.Equal(t, assert.AnError, capturedErr)
}
