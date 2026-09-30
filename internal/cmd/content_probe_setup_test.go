package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
	"github.com/stretchr/testify/require"
)

func TestProbeSetupDiagnosticsDoNotPublishConfigurationValues(t *testing.T) {
	const marker = "SYNTHETIC-PRIVATE-LITERAL"
	for _, cfg := range []string{
		"extract:\n  - name: value\n    type: regex\n    required: " + marker + "\n",
		"extract:\n  - name: value\n    type: " + marker + "\n",
		"extract:\n  - name: value\n    type: regex\n    pattern: '([" + marker + "'\n",
		"extract:\n  - name: value\n    type: regex\n    required: 'https://user:pass@example.test?token=synthetic-secret'\n",
	} {
		cmd, buf := probeSummaryTestCommand(t, "", nil)
		require.NoError(t, os.WriteFile(contentProbeConfigPath, []byte(cfg), 0600))
		err := runContentProbe(cmd, nil)
		require.Error(t, err)
		require.NotContains(t, err.Error(), marker)
		require.NotContains(t, buf.String(), marker)
		require.NotContains(t, err.Error(), "synthetic-secret")
		require.NotContains(t, buf.String(), "synthetic-secret")
		s := terminalProbeSummary(t, buf.String())
		require.Equal(t, 40, s.ExitCode)
		require.Equal(t, "failed", s.Termination)
	}
}

func TestProbeProviderSetupKeepsCodeWithoutPublishingBody(t *testing.T) {
	cmd, buf := probeSummaryTestCommand(t, "s3://bucket/key", nil)
	newContentProbeProvider = func(context.Context, *uri.ObjectURI, int) (contentProbeProvider, error) {
		return nil, fmt.Errorf("%w: SYNTHETIC-PRIVATE-LITERAL token=synthetic-secret", provider.ErrAccessDenied)
	}
	err := runContentProbe(cmd, nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "SYNTHETIC-PRIVATE-LITERAL")
	require.NotContains(t, buf.String(), "SYNTHETIC-PRIVATE-LITERAL")
	require.NotContains(t, buf.String(), "synthetic-secret")
	s := terminalProbeSummary(t, buf.String())
	require.Equal(t, 32, s.ExitCode)
	require.Equal(t, map[string]int64{"ACCESS_DENIED": 1}, s.ErrorsByCode)
}

func TestProbeTypedDispatchDoesNotParseOtherCommandMessages(t *testing.T) {
	for _, code := range []int{1, 32, 40, 60, 130} {
		actual, ok := ProbeExitCode(fmt.Errorf("wrapped: %w", probeExit(code, "probe outcome", errors.New("bounded"))))
		require.True(t, ok)
		require.Equal(t, code, int(actual))
	}
	_, ok := ProbeExitCode(errors.New("another command (exit code 60)"))
	require.False(t, ok)
}
