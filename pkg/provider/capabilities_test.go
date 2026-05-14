package provider

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPutPreconditionValidate(t *testing.T) {
	etag := "abc123"
	empty := ""

	tests := []struct {
		name    string
		precond PutPrecondition
		wantErr string
	}{
		{
			name:    "if absent",
			precond: PutPrecondition{IfAbsent: true},
		},
		{
			name:    "if match etag",
			precond: PutPrecondition{IfMatchETag: &etag},
		},
		{
			name:    "empty",
			precond: PutPrecondition{},
			wantErr: "exactly one put precondition must be set",
		},
		{
			name: "multiple predicates",
			precond: PutPrecondition{
				IfAbsent:    true,
				IfMatchETag: &etag,
			},
			wantErr: "exactly one put precondition must be set",
		},
		{
			name:    "empty etag",
			precond: PutPrecondition{IfMatchETag: &empty},
			wantErr: "IfMatchETag precondition must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.precond.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
