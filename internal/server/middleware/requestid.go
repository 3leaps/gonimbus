package middleware

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

// RequestID header key
const RequestIDHeader = "X-Request-ID"

// requestIDContextKey is a custom type to avoid context key collisions
type requestIDContextKey string

const RequestIDContextKey requestIDContextKey = "request_id"

// RequestID middleware adds a unique request ID to each request
// This works alongside chi's built-in RequestID middleware
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// First check if chi's middleware already set a request ID
		requestID := middleware.GetReqID(r.Context())

		// If not found, check if request ID exists in header
		if requestID == "" {
			requestID = r.Header.Get(RequestIDHeader)
		}

		// If still not found, generate new UUID
		if requestID == "" {
			requestID = uuid.New().String()
		}

		// Add request ID to response header
		w.Header().Set(RequestIDHeader, requestID)

		// Add request ID to our context key for consistency
		ctx := context.WithValue(r.Context(), RequestIDContextKey, requestID)

		// Continue with modified context
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRequestID retrieves request ID from context
// Checks both our context key and chi's context key
func GetRequestID(ctx context.Context) string {
	// First check our context key
	if requestID, ok := ctx.Value(RequestIDContextKey).(string); ok {
		return requestID
	}

	// Fall back to chi's request ID
	if requestID := middleware.GetReqID(ctx); requestID != "" {
		return requestID
	}

	return ""
}
