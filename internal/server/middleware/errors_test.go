package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fulmenhq/gofulmen/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecovery_NoPanic(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("success"))
	})

	middleware := Recovery(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	middleware.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "success", rec.Body.String())
}

func TestRecovery_WithPanic(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	middleware := Recovery(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	// Should not panic - middleware should recover
	assert.NotPanics(t, func() {
		middleware.ServeHTTP(rec, req)
	})

	// Should return 500 Internal Server Error
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	// Parse response body
	var response ErrorResponse
	err := json.Unmarshal(rec.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "INTERNAL_ERROR", response.Error.Code)
	assert.Contains(t, response.Error.Message, "panic: test panic")
}

func TestRecovery_WithPanicError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(assert.AnError)
	})

	middleware := Recovery(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	middleware.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	var response ErrorResponse
	err := json.Unmarshal(rec.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "INTERNAL_ERROR", response.Error.Code)
}

func TestRecovery_WithRequestID(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic with request id")
	})

	// Chain RequestID middleware before Recovery
	middleware := RequestID(Recovery(handler))

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Request-ID", "test-req-123")
	rec := httptest.NewRecorder()

	middleware.ServeHTTP(rec, req)

	var response ErrorResponse
	err := json.Unmarshal(rec.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "test-req-123", response.Error.RequestID)
}

func TestErrorHandler_IsSameAsRecovery(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test")
	})

	recoveryMiddleware := Recovery(handler)
	errorHandlerMiddleware := ErrorHandler(handler)

	// Both should produce the same behavior
	req1 := httptest.NewRequest("GET", "/test", nil)
	rec1 := httptest.NewRecorder()
	recoveryMiddleware.ServeHTTP(rec1, req1)

	req2 := httptest.NewRequest("GET", "/test", nil)
	rec2 := httptest.NewRecorder()
	errorHandlerMiddleware.ServeHTTP(rec2, req2)

	assert.Equal(t, rec1.Code, rec2.Code)
	assert.Equal(t, rec1.Header().Get("Content-Type"), rec2.Header().Get("Content-Type"))
}

func TestWriteErrorResponse(t *testing.T) {
	tests := []struct {
		name       string
		envelope   *errors.ErrorEnvelope
		statusCode int
		wantCode   string
		wantMsg    string
	}{
		{
			name:       "basic error",
			envelope:   errors.NewErrorEnvelope("TEST_ERROR", "test message"),
			statusCode: http.StatusBadRequest,
			wantCode:   "TEST_ERROR",
			wantMsg:    "test message",
		},
		{
			name:       "internal error",
			envelope:   errors.NewErrorEnvelope("INTERNAL_ERROR", "something went wrong"),
			statusCode: http.StatusInternalServerError,
			wantCode:   "INTERNAL_ERROR",
			wantMsg:    "something went wrong",
		},
		{
			name: "error with correlation ID",
			envelope: errors.NewErrorEnvelope("NOT_FOUND", "resource not found").
				WithCorrelationID("corr-123"),
			statusCode: http.StatusNotFound,
			wantCode:   "NOT_FOUND",
			wantMsg:    "resource not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()

			writeErrorResponse(rec, tt.envelope, tt.statusCode)

			assert.Equal(t, tt.statusCode, rec.Code)
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

			var response ErrorResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, response.Error.Code)
			assert.Equal(t, tt.wantMsg, response.Error.Message)
		})
	}
}

func TestWriteErrorResponse_WithContext(t *testing.T) {
	envelope := errors.NewErrorEnvelope("VALIDATION_ERROR", "invalid input")
	envelope, _ = envelope.WithContext(map[string]interface{}{
		"field": "email",
		"value": "invalid",
	})

	rec := httptest.NewRecorder()
	writeErrorResponse(rec, envelope, http.StatusBadRequest)

	var response ErrorResponse
	err := json.Unmarshal(rec.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.NotNil(t, response.Error.Details)
	assert.Equal(t, "email", response.Error.Details["field"])
	assert.Equal(t, "invalid", response.Error.Details["value"])
}
