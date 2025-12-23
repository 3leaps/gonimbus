package middleware

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/3leaps/gonimbus/internal/metrics"
	"github.com/fulmenhq/gofulmen/errors"
)

// Recovery middleware recovers from panics and logs them
func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// Create structured error from panic
				panicErr := errors.NewErrorEnvelope("INTERNAL_ERROR", fmt.Sprintf("panic: %v", err)).
					WithCorrelationID(GetRequestID(r.Context()))
				panicErr, _ = panicErr.WithContext(map[string]interface{}{
					"stack_trace": string(debug.Stack()),
				})
				// Set severity to critical
				panicErr, _ = panicErr.WithSeverity(errors.SeverityCritical)

				// Record metric
				metrics.RecordPanic()

				// Write error response using structured format
				writeErrorResponse(w, panicErr, http.StatusInternalServerError)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// ErrorHandler is an alias for Recovery for backward compatibility
func ErrorHandler(next http.Handler) http.Handler {
	return Recovery(next)
}

// ErrorResponse structure per API standards
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code      string                 `json:"code"`
	Message   string                 `json:"message"`
	Details   map[string]interface{} `json:"details,omitempty"`
	RequestID string                 `json:"request_id,omitempty"`
}

// writeErrorResponse writes error response directly (avoid circular import)
func writeErrorResponse(w http.ResponseWriter, envelope *errors.ErrorEnvelope, statusCode int) {
	response := ErrorResponse{
		Error: ErrorDetail{
			Code:      envelope.Code,
			Message:   envelope.Message,
			Details:   envelope.Context,
			RequestID: envelope.CorrelationID,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(response)
}
