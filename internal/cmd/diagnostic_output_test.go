package cmd

import (
	"bytes"
	"os"
	"os/exec"
	"regexp"
	"testing"

	"github.com/fulmenhq/gofulmen/appidentity"
	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/observability"
)

var structuredLogPrefixPattern = regexp.MustCompile(`(?m)^\d{4}-\d{2}-\d{2}T[^\t]*\t(?:DEBUG|INFO|WARN|ERROR)\t`)

func testCommandBuffers() (*cobra.Command, *bytes.Buffer, *bytes.Buffer) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := &cobra.Command{Use: "test"}
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	return cmd, &stdout, &stderr
}

func requirePlainDiagnosticOutput(t *testing.T, output string) {
	t.Helper()
	require.NotRegexp(t, structuredLogPrefixPattern, output)
	require.NotContains(t, output, `{"service":`)
	require.NotContains(t, output, `{"environment":`)
}

func withDiagnosticTestState(t *testing.T) {
	t.Helper()

	oldIdentity := appIdentity
	oldVersion := versionInfo
	oldEnvInfoLogFormat := envInfoLogFormat
	oldDoctorProvider := doctorProvider
	oldDoctorProfile := doctorProfile
	oldDoctorLogFormat := doctorLogFormat
	oldDoctorEndpoint := doctorEndpoint
	oldDoctorRegion := doctorRegion
	oldDoctorProbeURI := doctorProbeURI
	oldHealthLogFormat := healthLogFormat
	oldLogger := observability.CLILogger

	appIdentity = &appidentity.Identity{
		BinaryName: "gonimbus",
		ConfigName: "gonimbus",
	}
	SetVersionInfo("0.2.0-test", "abc123", "2026-05-11T12:00:00Z")
	envInfoLogFormat = diagnosticLogFormatPlain
	doctorProvider = ""
	doctorProfile = ""
	doctorLogFormat = diagnosticLogFormatPlain
	doctorEndpoint = ""
	doctorRegion = ""
	doctorProbeURI = ""
	healthLogFormat = diagnosticLogFormatPlain
	observability.InitCLILogger("gonimbus-test", false)
	setDefaults()

	t.Cleanup(func() {
		appIdentity = oldIdentity
		versionInfo = oldVersion
		envInfoLogFormat = oldEnvInfoLogFormat
		doctorProvider = oldDoctorProvider
		doctorProfile = oldDoctorProfile
		doctorLogFormat = oldDoctorLogFormat
		doctorEndpoint = oldDoctorEndpoint
		doctorRegion = oldDoctorRegion
		doctorProbeURI = oldDoctorProbeURI
		healthLogFormat = oldHealthLogFormat
		observability.CLILogger = oldLogger
	})
}

func TestEnvInfoDefaultOutputIsPlain(t *testing.T) {
	withDiagnosticTestState(t)

	cmd, stdout, stderr := testCommandBuffers()
	envInfoCmd.Run(cmd, nil)

	require.Empty(t, stderr.String())
	require.Contains(t, stdout.String(), "=== Gonimbus Environment Information ===")
	require.Contains(t, stdout.String(), "Application:")
	require.Contains(t, stdout.String(), "  Version:    0.2.0-test")
	requirePlainDiagnosticOutput(t, stdout.String())
}

func TestDoctorDefaultOutputIsPlain(t *testing.T) {
	withDiagnosticTestState(t)

	cmd, stdout, stderr := testCommandBuffers()
	runDoctor(cmd, nil)

	require.Empty(t, stderr.String())
	require.Contains(t, stdout.String(), "=== gonimbus doctor ===")
	require.Contains(t, stdout.String(), "Running diagnostic checks...")
	require.Contains(t, stdout.String(), "Checking Go version")
	require.Contains(t, stdout.String(), "=== End Diagnostics ===")
	require.NotContains(t, stdout.String(), "Probing S3 target")
	requirePlainDiagnosticOutput(t, stdout.String())
}

func TestHealthDefaultOutputIsPlain(t *testing.T) {
	withDiagnosticTestState(t)

	cmd, stdout, stderr := testCommandBuffers()
	healthCmd.Run(cmd, nil)

	require.Empty(t, stderr.String())
	require.Contains(t, stdout.String(), "Running health check...")
	require.Contains(t, stdout.String(), "Version information available")
	require.Contains(t, stdout.String(), "All health checks passed")
	requirePlainDiagnosticOutput(t, stdout.String())
}

func TestDiagnosticStructuredOptInUsesLoggerPath(t *testing.T) {
	withDiagnosticTestState(t)

	cmd, stdout, stderr := testCommandBuffers()
	out, err := newDiagnosticPrinter(cmd, diagnosticLogFormatStructured)
	require.NoError(t, err)
	require.True(t, out.structured)

	out.Info("structured opt-in test")
	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())
}

func TestDoctorInvalidProfileErrorIsPlain(t *testing.T) {
	if os.Getenv("GONIMBUS_TEST_DOCTOR_INVALID_PROFILE") == "1" {
		withDiagnosticTestState(t)
		doctorProfile = "demo"
		doctorProvider = ""
		doctorLogFormat = diagnosticLogFormatPlain
		runDoctor(&cobra.Command{Use: "doctor"}, nil)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestDoctorInvalidProfileErrorIsPlain")
	cmd.Env = append(os.Environ(), "GONIMBUS_TEST_DOCTOR_INVALID_PROFILE=1")
	output, err := cmd.CombinedOutput()
	require.Error(t, err)

	exitErr, ok := err.(*exec.ExitError)
	require.True(t, ok)
	require.Equal(t, int(foundry.ExitInvalidArgument), exitErr.ExitCode())

	text := string(output)
	require.Contains(t, text, "FATAL: Invalid flags")
	require.Contains(t, text, "--profile requires --provider s3")
	require.NotContains(t, text, "Running diagnostic checks")
	requirePlainDiagnosticOutput(t, text)
}
