package cmd

import "github.com/spf13/cobra"

var contentCmd = &cobra.Command{
	Use:   "content",
	Short: "Content inspection operations",
	Long: `Content commands perform safe reads (HEAD, range reads, streaming) and emit
machine-readable JSONL output suitable for pipeline integration.

Unlike the stream commands, content commands do not emit mixed-framing raw bytes.
They emit JSONL-only records.
`,
}

func init() {
	rootCmd.AddCommand(contentCmd)
}
