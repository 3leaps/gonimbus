package main

import (
	"archive/tar"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var stablePackages = []string{
	"github.com/3leaps/gonimbus/pkg/match",
	"github.com/3leaps/gonimbus/pkg/provider",
	"github.com/3leaps/gonimbus/pkg/provider/file",
	"github.com/3leaps/gonimbus/pkg/provider/s3",
	"github.com/3leaps/gonimbus/pkg/uri",
}

var releaseTagPattern = regexp.MustCompile(`^v[0-9]+(?:\.[0-9]+){1,2}(?:[-+][0-9A-Za-z.-]+)?$`)

func main() {
	baseTag := flag.String("base-tag", "", "release tag to compare against; defaults to latest v* tag")
	mode := flag.String("mode", "all", "check mode: all, diff, manifest")
	flag.Parse()

	if err := run(*mode, *baseTag); err != nil {
		fmt.Fprintln(os.Stderr, "api-stability:", err)
		os.Exit(1)
	}
}

func run(mode, baseTag string) error {
	switch mode {
	case "all":
		if err := checkManifest(); err != nil {
			return err
		}
		return checkDiff(baseTag)
	case "diff":
		return checkDiff(baseTag)
	case "manifest":
		return checkManifest()
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
}

func checkManifest() error {
	pkgs, err := goListPackages()
	if err != nil {
		return err
	}
	tiers, err := parseManifest()
	if err != nil {
		return err
	}

	var problems []string
	for _, pkg := range pkgs {
		if _, ok := tiers[pkg]; !ok {
			problems = append(problems, "missing tier for "+pkg)
		}
	}
	for pkg := range tiers {
		if !contains(pkgs, pkg) {
			problems = append(problems, "manifest entry is not returned by go list: "+pkg)
		}
	}
	for _, pkg := range stablePackages {
		if tiers[pkg] != "Stable" {
			problems = append(problems, "stable package must be marked Stable: "+pkg)
		}
	}
	for pkg, tier := range tiers {
		if tier == "Stable" && !contains(stablePackages, pkg) {
			problems = append(problems, "unexpected Stable package not covered by diff gate: "+pkg)
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("api stability manifest mismatch:\n%s", strings.Join(problems, "\n"))
	}
	return nil
}

func goListPackages() ([]string, error) {
	cmd := exec.Command("go", "list", "./pkg/...")
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("go list failed: %s", strings.TrimSpace(string(exitErr.Stderr)))
		}
		return nil, err
	}
	lines := strings.Fields(string(out))
	sort.Strings(lines)
	return lines, nil
}

func parseManifest() (map[string]string, error) {
	const filename = "docs/api-stability.md"
	data, err := readRepoFile(filename)
	if err != nil {
		return nil, err
	}
	return parseManifestContent(filename, string(data))
}

func parseManifestContent(filename, content string) (map[string]string, error) {
	tiers := make(map[string]string)
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "| `github.com/3leaps/gonimbus/pkg/") {
			continue
		}
		cells := splitMarkdownTableRow(line)
		if len(cells) < 2 {
			return nil, fmt.Errorf("invalid manifest row: %s", line)
		}
		pkg := strings.Trim(cells[0], "` ")
		tier := strings.TrimSpace(cells[1])
		if tier != "Stable" && tier != "Experimental" {
			return nil, fmt.Errorf("invalid tier %q for %s", tier, pkg)
		}
		if _, exists := tiers[pkg]; exists {
			return nil, fmt.Errorf("duplicate manifest entry for %s", pkg)
		}
		tiers[pkg] = tier
	}
	if len(tiers) == 0 {
		return nil, fmt.Errorf("no package rows found in %s", filename)
	}
	return tiers, nil
}

func splitMarkdownTableRow(line string) []string {
	line = strings.Trim(line, "|")
	parts := strings.Split(line, "|")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func checkDiff(baseTag string) error {
	if baseTag == "" {
		var err error
		baseTag, err = latestReleaseTag()
		if err != nil {
			return err
		}
	}
	base, err := snapshotFromGitArchive(baseTag)
	if err != nil {
		return err
	}
	current, err := snapshotFromWorkingTree()
	if err != nil {
		return err
	}

	changes := diffSnapshots(base, current)
	if len(changes) == 0 {
		return nil
	}
	if hasLibraryAPIChangelogEntry() {
		return nil
	}

	sort.Strings(changes)
	return fmt.Errorf("stable exported API changed relative to %s without an Unreleased Library API changelog entry:\n%s", baseTag, strings.Join(changes, "\n"))
}

func latestReleaseTag() (string, error) {
	cmd := exec.Command("git", "tag", "--list", "v*", "--sort=-v:refname")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	for _, tag := range strings.Fields(string(out)) {
		return tag, nil
	}
	return "", fmt.Errorf("no v* release tags found")
}

func snapshotFromGitArchive(tag string) (map[string]map[string]string, error) {
	if !releaseTagPattern.MatchString(tag) {
		return nil, fmt.Errorf("invalid release tag %q", tag)
	}
	// #nosec G204 -- tag is validated above and exec.Command does not invoke a shell.
	cmd := exec.Command("git", "archive", "--format=tar", tag)
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("git archive %s failed: %s", tag, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return nil, err
	}
	files := make(map[string][]byte)
	tr := tar.NewReader(bytes.NewReader(out))
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg || !strings.HasSuffix(hdr.Name, ".go") {
			continue
		}
		if !isStablePackageFile(hdr.Name) {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		files[hdr.Name] = data
	}
	return exportedSnapshot(files)
}

func snapshotFromWorkingTree() (map[string]map[string]string, error) {
	files := make(map[string][]byte)
	for _, pkg := range stablePackages {
		dir := importPathToDir(pkg)
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			name := filepath.ToSlash(filepath.Join(dir, entry.Name()))
			// #nosec G304 -- name is built from the fixed Stable package list and os.ReadDir entries.
			data, err := os.ReadFile(name)
			if err != nil {
				return nil, err
			}
			files[name] = data
		}
	}
	return exportedSnapshot(files)
}

func isStablePackageFile(name string) bool {
	if strings.HasSuffix(name, "_test.go") {
		return false
	}
	dir := path.Dir(name)
	for _, pkg := range stablePackages {
		if dir == importPathToDir(pkg) {
			return true
		}
	}
	return false
}

func importPathToDir(importPath string) string {
	return strings.TrimPrefix(importPath, "github.com/3leaps/gonimbus/")
}

func exportedSnapshot(files map[string][]byte) (map[string]map[string]string, error) {
	out := make(map[string]map[string]string)
	fset := token.NewFileSet()
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		file, err := parser.ParseFile(fset, name, files[name], parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		pkgPath := "github.com/3leaps/gonimbus/" + path.Dir(name)
		if out[pkgPath] == nil {
			out[pkgPath] = make(map[string]string)
		}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && ast.IsExported(d.Name.Name) {
					out[pkgPath]["func "+d.Name.Name] = signature(fset, d.Type)
				}
				if d.Recv != nil && ast.IsExported(d.Name.Name) {
					out[pkgPath]["method "+recvName(d.Recv)+"."+d.Name.Name] = signature(fset, d.Type)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if ast.IsExported(s.Name.Name) {
							out[pkgPath]["type "+s.Name.Name] = signature(fset, publicTypeNode(s.Type))
						}
					case *ast.ValueSpec:
						for _, ident := range s.Names {
							if ast.IsExported(ident.Name) {
								out[pkgPath][valueKind(d.Tok)+" "+ident.Name] = valueSignature(fset, s)
							}
						}
					}
				}
			}
		}
	}
	for _, pkg := range stablePackages {
		if out[pkg] == nil {
			out[pkg] = map[string]string{}
		}
	}
	return out, nil
}

func signature(fset *token.FileSet, node ast.Node) string {
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, fset, node); err != nil {
		return ""
	}
	return compact(buf.String())
}

func valueSignature(fset *token.FileSet, spec *ast.ValueSpec) string {
	if spec.Type != nil {
		return signature(fset, publicTypeNode(spec.Type))
	}
	if len(spec.Values) > 0 {
		return signature(fset, spec.Values[0])
	}
	return ""
}

func publicTypeNode(expr ast.Expr) ast.Expr {
	switch t := expr.(type) {
	case *ast.StructType:
		return publicStructType(t)
	default:
		return expr
	}
}

func publicStructType(t *ast.StructType) *ast.StructType {
	if t.Fields == nil {
		return t
	}
	fields := make([]*ast.Field, 0, len(t.Fields.List))
	for _, field := range t.Fields.List {
		if len(field.Names) == 0 {
			if exportedEmbeddedField(field.Type) {
				fields = append(fields, field)
			}
			continue
		}
		names := make([]*ast.Ident, 0, len(field.Names))
		for _, name := range field.Names {
			if ast.IsExported(name.Name) {
				names = append(names, name)
			}
		}
		if len(names) == 0 {
			continue
		}
		copyField := *field
		copyField.Names = names
		fields = append(fields, &copyField)
	}
	return &ast.StructType{
		Struct: t.Struct,
		Fields: &ast.FieldList{
			Opening: t.Fields.Opening,
			List:    fields,
			Closing: t.Fields.Closing,
		},
		Incomplete: t.Incomplete,
	}
}

func exportedEmbeddedField(expr ast.Expr) bool {
	for {
		if star, ok := expr.(*ast.StarExpr); ok {
			expr = star.X
			continue
		}
		break
	}
	switch v := expr.(type) {
	case *ast.Ident:
		return ast.IsExported(v.Name)
	case *ast.SelectorExpr:
		return ast.IsExported(v.Sel.Name)
	default:
		return false
	}
}

func compact(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func recvName(fields *ast.FieldList) string {
	if fields == nil || len(fields.List) == 0 {
		return ""
	}
	t := fields.List[0].Type
	for {
		if star, ok := t.(*ast.StarExpr); ok {
			t = star.X
			continue
		}
		break
	}
	switch v := t.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.IndexExpr:
		if id, ok := v.X.(*ast.Ident); ok {
			return id.Name
		}
	case *ast.IndexListExpr:
		if id, ok := v.X.(*ast.Ident); ok {
			return id.Name
		}
	}
	return compact(fmt.Sprint(t))
}

func valueKind(tok token.Token) string {
	if tok == token.CONST {
		return "const"
	}
	return "var"
}

func diffSnapshots(base, current map[string]map[string]string) []string {
	var changes []string
	for _, pkg := range stablePackages {
		baseSymbols := base[pkg]
		currentSymbols := current[pkg]
		for name, baseSig := range baseSymbols {
			currentSig, ok := currentSymbols[name]
			if !ok {
				changes = append(changes, fmt.Sprintf("removed %s %s", pkg, name))
				continue
			}
			if currentSig != baseSig {
				changes = append(changes, fmt.Sprintf("changed %s %s", pkg, name))
			}
		}
		for name := range currentSymbols {
			if _, ok := baseSymbols[name]; !ok {
				changes = append(changes, fmt.Sprintf("added %s %s", pkg, name))
			}
		}
	}
	sort.Strings(changes)
	return changes
}

func hasLibraryAPIChangelogEntry() bool {
	data, err := readRepoFile("CHANGELOG.md")
	if err != nil {
		return false
	}
	return hasLibraryAPIChangelogText(string(data))
}

func hasLibraryAPIChangelogText(changelog string) bool {
	section := unreleasedSection(changelog)
	if section == "" {
		return false
	}
	lines := strings.Split(section, "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) != "### Library API" {
			continue
		}
		var body []string
		for _, bodyLine := range lines[i+1:] {
			if strings.HasPrefix(bodyLine, "### ") || strings.HasPrefix(bodyLine, "## ") {
				break
			}
			body = append(body, strings.TrimSpace(bodyLine))
		}
		if strings.TrimSpace(strings.Join(body, "\n")) != "" {
			return true
		}
	}
	return false
}

func unreleasedSection(changelog string) string {
	lines := strings.Split(changelog, "\n")
	start := -1
	for i, line := range lines {
		if strings.HasPrefix(line, "## [Unreleased]") {
			start = i + 1
			break
		}
	}
	if start == -1 {
		return ""
	}
	end := len(lines)
	for i := start; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "## [") {
			end = i
			break
		}
	}
	return strings.Join(lines[start:end], "\n")
}

func contains(items []string, value string) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}

func readRepoFile(name string) ([]byte, error) {
	clean := filepath.ToSlash(filepath.Clean(name))
	if clean == "." || strings.HasPrefix(clean, "../") || path.IsAbs(clean) {
		return nil, fmt.Errorf("invalid repo file path %q", name)
	}
	// #nosec G304 -- callers pass fixed repository-relative documentation paths.
	return os.ReadFile(clean)
}
