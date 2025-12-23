package handlers

import (
	"net/http"

	apperrors "github.com/3leaps/gonimbus/internal/errors"
)

var defaultHTTPErrorResponder = func(w http.ResponseWriter, r *http.Request, err error) {
	apperrors.RespondWithError(w, r, err)
}

var httpErrorResponder = defaultHTTPErrorResponder

// SetHTTPErrorResponder allows the server package to inject the centralized error handler.
func SetHTTPErrorResponder(responder func(http.ResponseWriter, *http.Request, error)) {
	if responder == nil {
		httpErrorResponder = defaultHTTPErrorResponder
		return
	}
	httpErrorResponder = responder
}

// ResetHTTPErrorResponder restores the default responder (useful for tests).
func ResetHTTPErrorResponder() {
	httpErrorResponder = defaultHTTPErrorResponder
}

func respondWithError(w http.ResponseWriter, r *http.Request, err error) {
	httpErrorResponder(w, r, err)
}
