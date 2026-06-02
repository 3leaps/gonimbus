package opcheckpoint

import (
	"errors"
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

func TestClassifyFatalErrorOrdering(t *testing.T) {
	tests := []struct {
		name string
		err  error
		in   ClassifierInput
		want Classification
	}{
		{
			name: "refreshable credential refresh failure is resumable",
			err:  errors.New("s3 List: get identity: failed to refresh cached credentials: invalid_grant"),
			in:   ClassifierInput{RefreshableCredentials: true},
			want: Classification{Class: ErrorClassCredentialsRefreshFailed, Resumable: true},
		},
		{
			name: "static credential model never emits refresh failure",
			err:  errors.New("s3 List: get identity: failed to refresh cached credentials: invalid_grant"),
			in:   ClassifierInput{RefreshableCredentials: false},
			want: Classification{Class: ErrorClassRuntimeFailure, Resumable: false},
		},
		{
			name: "access denied wins over refresh wording",
			err:  errors.New("AccessDenied: failed to refresh cached credentials after revoked access"),
			in:   ClassifierInput{RefreshableCredentials: true},
			want: Classification{Class: ErrorClassAuthDenied, Resumable: false},
		},
		{
			name: "access denied wins over interruption",
			err:  errors.New("AccessDenied: revoked access while context interrupted"),
			in:   ClassifierInput{Interrupted: true},
			want: Classification{Class: ErrorClassAuthDenied, Resumable: false},
		},
		{
			name: "provider invalid credentials is non resumable",
			err:  provider.ErrInvalidCredentials,
			in:   ClassifierInput{RefreshableCredentials: true},
			want: Classification{Class: ErrorClassAuthDenied, Resumable: false},
		},
		{
			name: "operator interruption is resumable without auth model coupling",
			err:  contextCanceledForTest{},
			in:   ClassifierInput{Interrupted: true},
			want: Classification{Class: ErrorClassInterrupted, Resumable: true},
		},
		{
			name: "retry exhausted is resumable trigger",
			err:  errors.New("retry budget exhausted"),
			in:   ClassifierInput{TransientRetryExhausted: true},
			want: Classification{Class: ErrorClassTransientRetryExhausted, Resumable: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ClassifyFatalError(tt.err, tt.in))
		})
	}
}

type contextCanceledForTest struct{}

func (contextCanceledForTest) Error() string { return "context canceled" }
