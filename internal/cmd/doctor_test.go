package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/3leaps/gonimbus/internal/observability"
)

func TestMaskAccessKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "standard 20 char key",
			input: "AKIAIOSFODNN7EXAMPLE",
			want:  "****MPLE",
		},
		{
			name:  "short key 4 chars",
			input: "ABCD",
			want:  "****",
		},
		{
			name:  "short key 3 chars",
			input: "ABC",
			want:  "****",
		},
		{
			name:  "empty key",
			input: "",
			want:  "****",
		},
		{
			name:  "5 char key shows last 4",
			input: "ABCDE",
			want:  "****BCDE",
		},
		{
			name:  "8 char key",
			input: "12345678",
			want:  "****5678",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskAccessKey(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPrintAWSCredentialsHelp(t *testing.T) {
	// Initialize CLI logger to avoid nil pointer
	observability.InitCLILogger("test", false)

	// This test verifies the function doesn't panic
	// It logs help text for configuring AWS credentials
	t.Run("does not panic without profile", func(t *testing.T) {
		assert.NotPanics(t, func() {
			printAWSCredentialsHelp("")
		})
	})

	t.Run("does not panic with profile", func(t *testing.T) {
		assert.NotPanics(t, func() {
			printAWSCredentialsHelp("my-profile")
		})
	})
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{
			name:     "hours and minutes",
			duration: 5*time.Hour + 30*time.Minute,
			want:     "5h 30m",
		},
		{
			name:     "just minutes",
			duration: 45 * time.Minute,
			want:     "45m",
		},
		{
			name:     "zero",
			duration: 0,
			want:     "0m",
		},
		{
			name:     "negative (expired)",
			duration: -1 * time.Hour,
			want:     "expired",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDuration(tt.duration)
			assert.Equal(t, tt.want, got)
		})
	}
}
