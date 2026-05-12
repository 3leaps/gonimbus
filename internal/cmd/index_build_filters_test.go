package cmd

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/match"
)

func TestComputeFiltersHashFromConfig_StableForEquivalentInputs(t *testing.T) {
	cfg1 := &match.FilterConfig{
		Size: &match.SizeFilterConfig{Min: "1KB"},
		Modified: &match.DateFilterConfig{
			After:  "2025-12-01",
			Before: "2026-01-01",
		},
		KeyRegex: "\\.xml$",
	}

	cfg2 := &match.FilterConfig{
		Size: &match.SizeFilterConfig{Min: "1000B"},
		Modified: &match.DateFilterConfig{
			After:  "2025-12-01T00:00:00Z",
			Before: "2026-01-01T00:00:00Z",
		},
		KeyRegex: "  \\.xml$  ",
	}

	h1, err := computeFiltersHashFromConfig(cfg1)
	require.NoError(t, err)
	require.NotEmpty(t, h1)

	h2, err := computeFiltersHashFromConfig(cfg2)
	require.NoError(t, err)
	require.Equal(t, h1, h2)
}

func TestComputeFiltersHashFromConfig_ChangesWhenValueChanges(t *testing.T) {
	cfg := &match.FilterConfig{
		Size: &match.SizeFilterConfig{Min: "1KB"},
		Modified: &match.DateFilterConfig{
			After:  "2025-12-01",
			Before: "2026-01-01",
		},
		KeyRegex: "\\.xml$",
	}

	h1, err := computeFiltersHashFromConfig(cfg)
	require.NoError(t, err)
	require.NotEmpty(t, h1)

	cfg.KeyRegex = "\\.json$"
	h2, err := computeFiltersHashFromConfig(cfg)
	require.NoError(t, err)
	require.NotEmpty(t, h2)
	require.NotEqual(t, h1, h2)
}

func TestComputeFiltersHashFromConfig_RejectsInvalidBounds(t *testing.T) {
	cfg := &match.FilterConfig{
		Size: &match.SizeFilterConfig{Min: "2KB", Max: "1KB"},
	}
	_, err := computeFiltersHashFromConfig(cfg)
	require.Error(t, err)
}

func TestComputeFiltersHashFromConfig_NormalizesDatesToUTC(t *testing.T) {
	cfg := &match.FilterConfig{Modified: &match.DateFilterConfig{After: "2025-12-01T00:00:00-05:00"}}
	h, err := computeFiltersHashFromConfig(cfg)
	require.NoError(t, err)
	require.NotEmpty(t, h)

	cfg2 := &match.FilterConfig{Modified: &match.DateFilterConfig{After: time.Date(2025, 12, 1, 5, 0, 0, 0, time.UTC).Format(time.RFC3339)}}
	h2, err := computeFiltersHashFromConfig(cfg2)
	require.NoError(t, err)
	require.Equal(t, h, h2)
}

func TestIsAWSSSOExpiredError_PositiveSignals(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "invalid grant exception",
			err:  errors.New("create provider: operation error SSO: GetRoleCredentials, InvalidGrantException: "),
		},
		{
			name: "token expired",
			err:  errors.New("scope compile: provider list: token has expired and refresh failed"),
		},
		{
			name: "expired sso session",
			err:  errors.New("aws sdk: SSO session is expired or invalid"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.True(t, isAWSSSOExpiredError(tt.err))
		})
	}
}

func TestIsAWSSSOExpiredError_NegativeSignals(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "access denied", err: errors.New("provider list: AccessDenied: access denied")},
		{name: "missing profile", err: errors.New("failed to get shared config profile, demo-profile")},
		{name: "imds", err: errors.New("failed to refresh cached credentials, no EC2 IMDS role found")},
		{name: "static credentials", err: errors.New("SignatureDoesNotMatch: request signature mismatch")},
		{name: "nil", err: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.False(t, isAWSSSOExpiredError(tt.err))
		})
	}
}

func TestAWSSSOLoginHint_Profile(t *testing.T) {
	hint := awsSSOLoginHint("demo-profile")
	require.Contains(t, hint, "aws sso login --profile demo-profile")
}

func TestAWSSSOLoginHint_Generic(t *testing.T) {
	hint := awsSSOLoginHint("")
	require.Contains(t, hint, "aws sso login")
	require.NotContains(t, hint, "--profile")
}

func TestWriteScopePlanError_IncludesHintBeforeRawError(t *testing.T) {
	err := errors.New("create provider: operation error SSO: GetRoleCredentials, InvalidGrantException: ")
	var out bytes.Buffer

	writeScopePlanError(&out, err, "demo-profile")

	text := out.String()
	require.Contains(t, text, "hint: AWS SSO token appears expired")
	require.Contains(t, text, "aws sso login --profile demo-profile")
	require.Contains(t, text, "error: create provider: operation error SSO")
	require.Less(t, bytes.Index(out.Bytes(), []byte("hint:")), bytes.Index(out.Bytes(), []byte("error:")))
}

func TestWriteScopePlanError_NonSSOOnlyRawError(t *testing.T) {
	err := errors.New("provider list: AccessDenied: access denied")
	var out bytes.Buffer

	writeScopePlanError(&out, err, "demo-profile")

	text := out.String()
	require.NotContains(t, text, "hint:")
	require.Contains(t, text, "error: provider list: AccessDenied")
}
