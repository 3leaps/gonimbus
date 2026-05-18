package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type depBoundary struct {
	RecommendedPackages []string `json:"recommended_packages"`
	DeniedImports       []string `json:"denied_imports"`
	DeniedEnvCalls      []string `json:"denied_env_calls"`
}

func TestImportAndConfigLiteralAreEnvFree(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=TestEmbeddingImportHelper")
	cmd.Env = []string{"GONIMBUS_EMBEDDING_IMPORT_HELPER=1"}

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("embed import helper failed: %v\n%s", err, out)
	}
}

func TestEmbeddingImportHelper(t *testing.T) {
	if os.Getenv("GONIMBUS_EMBEDDING_IMPORT_HELPER") != "1" {
		return
	}
	if err := constructLibrarySurface(); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}

func TestRecommendedPackagesAvoidDeniedEnvReadsAndImports(t *testing.T) {
	root := repoRoot(t)
	boundary := readDepBoundary(t, root)

	for _, pkg := range boundary.RecommendedPackages {
		pkgDir := filepath.Join(root, strings.TrimPrefix(pkg, "./"))
		entries, err := os.ReadDir(pkgDir)
		if err != nil {
			t.Fatalf("read package dir %s: %v", pkg, err)
		}

		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			checkGoFileForDeniedContract(t, filepath.Join(pkgDir, entry.Name()), boundary)
		}
	}
}

func TestRecommendedPackageDependencyBoundary(t *testing.T) {
	root := repoRoot(t)
	boundary := readDepBoundary(t, root)

	cmd := exec.Command("go", "list", "-deps", "./internal/embeddingtest")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list dependency boundary failed: %v\n%s", err, out)
	}

	deps := strings.Fields(string(out))
	for _, denied := range boundary.DeniedImports {
		for _, dep := range deps {
			if importDenied(dep, denied) {
				t.Fatalf("recommended embed package dependency graph includes denied dependency %q via %q", denied, dep)
			}
		}
	}
}

func checkGoFileForDeniedContract(t *testing.T, path string, boundary depBoundary) {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse imports %s: %v", path, err)
	}

	importNames := map[string]string{}
	for _, spec := range file.Imports {
		importPath := strings.Trim(spec.Path.Value, `"`)
		for _, denied := range boundary.DeniedImports {
			if importDenied(importPath, denied) {
				t.Fatalf("%s imports denied library-consumer dependency %q", path, importPath)
			}
		}

		name := filepath.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		importNames[name] = importPath
	}

	file, err = parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse file %s: %v", path, err)
	}

	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		if importNames[ident.Name] == "os" {
			callName := fmt.Sprintf("os.%s", sel.Sel.Name)
			for _, denied := range boundary.DeniedEnvCalls {
				if callName == denied {
					t.Fatalf("%s calls denied library-consumer env function %s", path, callName)
				}
			}
		}
		return true
	})
}

func readDepBoundary(t *testing.T, root string) depBoundary {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(root, "testdata/library-consumers/dep-boundary.json"))
	if err != nil {
		t.Fatalf("read dep boundary: %v", err)
	}

	var boundary depBoundary
	if err := json.Unmarshal(data, &boundary); err != nil {
		t.Fatalf("parse dep boundary: %v", err)
	}
	return boundary
}

func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repo root")
		}
		dir = parent
	}
}

func importDenied(importPath, deniedPrefix string) bool {
	return importPath == deniedPrefix || strings.HasPrefix(importPath, deniedPrefix+"/")
}
