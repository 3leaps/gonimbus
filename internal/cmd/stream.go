package cmd

import "github.com/spf13/cobra"

var streamCmd = &cobra.Command{
	Use:   "stream",
	Short: "Streaming helpers (mixed-framing JSONL + raw bytes)",
	Long: `Streaming helpers emit a mixed-framing stream:

- JSONL control-plane records on stdout (one JSON object per line)
- raw byte blocks immediately following chunk header records

Important: in streaming mode, errors are emitted to stdout as gonimbus.error.v1 records
so downstream consumers can handle failures without scraping stderr.
`,
}

func init() {
	rootCmd.AddCommand(streamCmd)
}
