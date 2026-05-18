# Using Gonimbus as a Library

Gonimbus exposes selected packages under `pkg/` for Go applications that need
the same object-storage parsing and data-plane helpers used by the CLI. For URI
parsing, use `github.com/3leaps/gonimbus/pkg/uri`.

```go
package main

import (
	"fmt"

	"github.com/3leaps/gonimbus/pkg/uri"
)

func main() {
	u, err := uri.ParseURI("s3://bucket/data/2026/**/*.xml")
	if err != nil {
		panic(err)
	}

	fmt.Println(u.Provider)    // s3
	fmt.Println(u.Bucket)      // bucket
	fmt.Println(u.Key)         // data/2026/
	fmt.Println(u.Pattern)     // data/2026/**/*.xml
	fmt.Println(u.IsPattern()) // true
}
```

`pkg/uri` currently supports S3-style URIs. Glob patterns preserve the original
pattern in `ObjectURI.Pattern` and expose the strongest listing prefix in
`ObjectURI.Key`. Escaped glob metacharacters are treated as literal key
characters.

Gonimbus is pre-v1.0. Public packages are intended for library use, but breaking
changes may still happen across minor versions. Pin applications to specific
release tags.

For the full supported embedded-use contract, including credential injection,
environment-read behavior, endpoint hermeticity, and dependency boundaries, see
[`docs/library-consumers.md`](../library-consumers.md).
