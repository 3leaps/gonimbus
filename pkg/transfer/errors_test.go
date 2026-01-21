package transfer

import (
	"context"
	"errors"
	"testing"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/assert"
)

func TestClassifyErrCode_SizeMismatch_MapsToNotFound(t *testing.T) {
	// Size mismatch errors should map to NOT_FOUND (stale key semantics).
	err := &SizeMismatchError{
		Key:      "data/stale-key.txt",
		Expected: 1024,
		Got:      2048,
	}

	code := classifyErrCode(err)
	assert.Equal(t, output.ErrCodeNotFound, code)
}

func TestClassifyErrCode_ProviderNotFound(t *testing.T) {
	err := &provider.ProviderError{
		Op:       "GetObject",
		Provider: provider.ProviderS3,
		Bucket:   "test",
		Key:      "missing.txt",
		Err:      provider.ErrNotFound,
	}

	code := classifyErrCode(err)
	assert.Equal(t, output.ErrCodeNotFound, code)
}

func TestClassifyErrCode_ProviderAccessDenied(t *testing.T) {
	err := &provider.ProviderError{
		Op:       "GetObject",
		Provider: provider.ProviderS3,
		Bucket:   "test",
		Key:      "forbidden.txt",
		Err:      provider.ErrAccessDenied,
	}

	code := classifyErrCode(err)
	assert.Equal(t, output.ErrCodeAccessDenied, code)
}

func TestClassifyErrCode_ContextCanceled(t *testing.T) {
	code := classifyErrCode(context.Canceled)
	assert.Equal(t, output.ErrCodeTimeout, code)
}

func TestClassifyErrCode_ContextDeadlineExceeded(t *testing.T) {
	code := classifyErrCode(context.DeadlineExceeded)
	assert.Equal(t, output.ErrCodeTimeout, code)
}

func TestClassifyErrCode_UnknownError(t *testing.T) {
	code := classifyErrCode(errors.New("some random error"))
	assert.Equal(t, output.ErrCodeInternal, code)
}

func TestSizeMismatchError_Message(t *testing.T) {
	err := &SizeMismatchError{
		Key:      "path/to/object.txt",
		Expected: 100,
		Got:      200,
	}

	msg := err.Error()
	assert.Contains(t, msg, "source size mismatch")
	assert.Contains(t, msg, "path/to/object.txt")
	assert.Contains(t, msg, "expected=100")
	assert.Contains(t, msg, "got=200")
}

func TestIsSizeMismatch(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "SizeMismatchError",
			err:  &SizeMismatchError{Key: "k", Expected: 1, Got: 2},
			want: true,
		},
		{
			name: "ProviderError",
			err:  &provider.ProviderError{Err: provider.ErrNotFound},
			want: false,
		},
		{
			name: "PlainError",
			err:  errors.New("something"),
			want: false,
		},
		{
			name: "Nil",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isSizeMismatch(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}
