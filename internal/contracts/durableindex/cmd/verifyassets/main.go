// Command verifyassets checks a disposable contract fixture tree against the
// embedded durable-index/v0 inventory before an upstream integrity battery runs.
package main

import (
	"fmt"
	"os"

	contractv0 "github.com/3leaps/gonimbus/internal/contracts/durableindex/v0"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: verifyassets <fixture-root>")
		os.Exit(2)
	}
	if err := contractv0.VerifyFilesystem(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
