//go:build linux

package reflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMemInfoTotalBytes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int64
	}{
		{
			name: "well-formed meminfo",
			raw:  "MemTotal:       65536000 kB\nMemFree:        1024 kB\n",
			want: 65536000 * 1024,
		},
		{
			name: "MemTotal not first line",
			raw:  "MemFree:        1024 kB\nMemTotal:       2048 kB\n",
			want: 2048 * 1024,
		},
		{
			name: "missing MemTotal",
			raw:  "MemFree:        1024 kB\n",
			want: 0,
		},
		{
			name: "malformed number",
			raw:  "MemTotal:       lots kB\n",
			want: 0,
		},
		{
			name: "unexpected unit",
			raw:  "MemTotal:       2048 MB\n",
			want: 0,
		},
		{
			name: "zero value",
			raw:  "MemTotal:       0 kB\n",
			want: 0,
		},
		{
			name: "absurd value",
			raw:  "MemTotal:       1152921504606846976 kB\n",
			want: 0,
		},
		{
			name: "empty content",
			raw:  "",
			want: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, parseMemInfoTotalBytes([]byte(tc.raw)))
		})
	}
}
