package transfer

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

func classifyErrCode(err error) string {
	switch {
	case provider.IsNotFound(err):
		return output.ErrCodeNotFound
	case provider.IsAccessDenied(err):
		return output.ErrCodeAccessDenied
	case provider.IsThrottled(err):
		return output.ErrCodeThrottled
	case provider.IsProviderUnavailable(err):
		return output.ErrCodeProviderUnavailable
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return output.ErrCodeTimeout
	case IsTransientNetworkError(err):
		return output.ErrCodeTransient
	case isSizeMismatch(err):
		// A stale list/index case; treat as NOT_FOUND for now to keep taxonomy small.
		return output.ErrCodeNotFound
	default:
		return output.ErrCodeInternal
	}
}

// IsTransientNetworkError reports transport failures that callers can usually
// retry at the object/chunk level without treating the engine as internally
// broken.
func IsTransientNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	for _, sentinel := range []error{
		syscall.ECONNRESET,
		syscall.ECONNABORTED,
		syscall.ECONNREFUSED,
		syscall.EPIPE,
	} {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"connection reset by peer",
		"connection refused",
		"broken pipe",
		"i/o timeout",
		"no such host",
		"unexpected eof",
		"server closed idle connection",
		"use of closed network connection",
		"tls handshake timeout",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

func isSizeMismatch(err error) bool {
	_, ok := err.(*SizeMismatchError)
	return ok
}
