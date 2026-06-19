package reflow

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
)

func TestSanitizeOperationCauseMessageRedactsSensitiveProviderErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "bearer token",
			err:  errors.New("GET failed Authorization: Bearer SECRET-BEARER"),
			want: "provider error redacted",
		},
		{
			name: "aws signed url",
			err:  errors.New("GET https://example.test/key?X-Amz-Signature=SECRET-SIG&X-Amz-Credential=SECRET-CRED&visible=value failed"),
			want: "provider error redacted",
		},
		{
			name: "gcs signed url",
			err:  errors.New("GET https://storage.example.test/key?X-Goog-Signature=SECRET-SIG&X-Goog-Credential=SECRET-CRED failed"),
			want: "provider error redacted",
		},
		{
			name: "userinfo url",
			err:  errors.New("GET https://user:SECRET-PASS@example.test/key failed"),
			want: "provider error redacted",
		},
		{
			name: "generic sig query",
			err:  errors.New("GET https://example.test/key?sig=SECRET-SIG failed"),
			want: "provider error redacted",
		},
		{
			name: "generic token key value",
			err:  errors.New("provider returned token=SECRET-TOKEN"),
			want: "provider returned token=<redacted>",
		},
		{
			name: "service account email",
			err:  errors.New("credential subject worker@project.iam.gserviceaccount.com rejected"),
			want: "provider error redacted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeOperationCauseMessage(tt.err)
			if got != tt.want {
				t.Fatalf("SanitizeOperationCauseMessage() = %q, want %q", got, tt.want)
			}
			for _, marker := range []string{
				"SECRET-BEARER",
				"SECRET-SIG",
				"SECRET-CRED",
				"SECRET-PASS",
				"SECRET-TOKEN",
				"worker@project.iam.gserviceaccount.com",
			} {
				if strings.Contains(got, marker) {
					t.Fatalf("sanitized message leaked marker %q in %q", marker, got)
				}
			}
		})
	}
}

func TestSanitizeOperationCauseMessageKeepsSafeRootSentinels(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "deadline",
			err:  errors.Join(context.DeadlineExceeded, errors.New("GET https://token@example.test/key?X-Amz-Signature=SECRET failed")),
			want: "context deadline exceeded",
		},
		{
			name: "throttled",
			err: &provider.ProviderError{
				Op:       "GetObject",
				Provider: provider.ProviderS3,
				Err:      errors.Join(provider.ErrThrottled, errors.New("GET https://example.test/key?X-Amz-Signature=SECRET failed")),
			},
			want: provider.ErrThrottled.Error(),
		},
		{
			name: "access denied",
			err: &provider.ProviderError{
				Op:       "PutObject",
				Provider: provider.ProviderS3,
				Err:      errors.Join(provider.ErrAccessDenied, errors.New("Authorization: Bearer SECRET failed")),
			},
			want: provider.ErrAccessDenied.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeOperationCauseMessage(tt.err)
			if got != tt.want {
				t.Fatalf("SanitizeOperationCauseMessage() = %q, want %q", got, tt.want)
			}
			if strings.Contains(got, "SECRET") || strings.Contains(got, "token@example.test") {
				t.Fatalf("sanitized sentinel leaked sensitive marker in %q", got)
			}
		})
	}
}

func TestSanitizeOperationCauseMessageRedactsURLsWithoutCredentialMaterial(t *testing.T) {
	got := SanitizeOperationCauseMessage(errors.New("GET https://example.test/key?visible=value failed"))
	if strings.Contains(got, "visible=value") {
		t.Fatalf("sanitized URL retained raw query value: %q", got)
	}
	if !strings.Contains(got, "visible=%3Credacted%3E") {
		t.Fatalf("sanitized URL did not redact query value: %q", got)
	}
}

func TestNewPathErrorHonorsVerboseOption(t *testing.T) {
	err := NewPathError("source root is not accessible", "/tmp/private/source: permission denied", PathErrorOptions{})
	if got, want := err.Error(), "source root is not accessible"; got != want {
		t.Fatalf("NewPathError() = %q, want %q", got, want)
	}

	err = NewPathError("source root is not accessible", "/tmp/private/source: permission denied", PathErrorOptions{Verbose: true})
	if got, want := err.Error(), "/tmp/private/source: permission denied"; got != want {
		t.Fatalf("NewPathError(verbose) = %q, want %q", got, want)
	}
}
