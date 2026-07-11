package jobregistry

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestJobControlSourcesUseRegistryReadPrimitives is a lightweight architecture
// gate: registry records/logs must be opened through the platform-specific,
// directory-bound primitives rather than reintroducing path-based reads.
func TestJobControlSourcesUseRegistryReadPrimitives(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	patterns := []string{
		filepath.Join(repoRoot, "pkg", "jobregistry", "*.go"),
		filepath.Join(repoRoot, "internal", "cmd", "index_jobs*.go"),
	}
	banned := map[string]bool{
		"Open": true, "OpenFile": true, "ReadFile": true,
		"CreateTemp": true, "Rename": true, "WriteFile": true,
	}
	for _, pattern := range patterns {
		files, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range files {
			base := filepath.Base(path)
			if strings.HasSuffix(base, "_test.go") || base == "invocation.go" || strings.HasPrefix(base, "lock_") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !banned[selector.Sel.Name] {
					return true
				}
				pkg, ok := selector.X.(*ast.Ident)
				if ok && pkg.Name == "os" {
					t.Errorf("%s uses os.%s; registry record/log I/O must use directory-bound store primitives", path, selector.Sel.Name)
				}
				return true
			})
		}
	}
}
