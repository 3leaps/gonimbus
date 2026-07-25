package ownedproc

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// The ownership rule is a property of the source, not of any single run: a
// second wait added anywhere in this package would be a real defect that no
// behavioral assertion is guaranteed to observe, because whether it corrupts
// anything depends on scheduling and on whether an id has been reused. These
// controls read the implementation and reject the shapes the rule forbids.
//
// The contract is package-wide, so the controls are too: they load every
// non-test file in the package and type-check them together. A guard scoped to
// one file could be defeated by moving the forbidden shape into a second file,
// and a guard that matches a spelling could be defeated by choosing another
// spelling of the same capability. Both are decided here against types.
//
// Scope, stated exactly so the controls are not read as broader than they are.
// A "wait" is a function or method named Wait, or Wait followed by digits — the
// spellings the standard library and the system call interface use to reap a
// child (Wait, Wait4). Uses are counted at the identifier and resolved by the
// type checker, across every non-test file, so the count does not depend on the
// expression shape a wait happens to be written in: a method call, a method
// value, a method expression, a qualified package function, and a bare call to
// a package-level or dot-imported function are all one rule.
//
// The limits that remain, named rather than left implied. A reaping path under
// some other name passes. A route that never mentions the function by
// identifier — reflection by string, linkname, assembly — passes; each would be
// conspicuous in a package this size, but none is caught here. And a legitimate
// future sync.WaitGroup.Wait would fail, which in a package this small is the
// deliberation the rule is for.
//
// What the name rule does not decide is which types carry the capability. That
// is settled by method sets, so a handle stays caught however its type is
// spelled at the point it is handed out — including a wrapper that embeds a
// command and inherits the method by promotion.

// loadPackage type-checks every non-test file in this package. Type information
// is what makes the controls resistant to renaming: a capability is recognised
// by its method set rather than by how a result happens to be spelled.
var loadPackage = sync.OnceValues(func() (*packageUnderTest, error) {
	fset := token.NewFileSet()

	entries, err := os.ReadDir(".")
	if err != nil {
		return nil, err
	}
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Clean(name), nil, 0)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		return nil, errNoImplementationFiles
	}

	// Every non-test file is checked, including any that a build constraint would
	// exclude on this host. If a constraint-split file is ever added the check
	// below fails rather than skipping the file, which keeps the failure visible
	// instead of quietly narrowing the controls' reach.
	info := &types.Info{
		Uses:       make(map[*ast.Ident]types.Object),
		Selections: make(map[*ast.SelectorExpr]*types.Selection),
		Defs:       make(map[*ast.Ident]types.Object),
	}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	pkg, err := conf.Check("ownedproc", fset, files, info)
	if err != nil {
		return nil, err
	}
	return &packageUnderTest{fset: fset, files: files, pkg: pkg, info: info}, nil
})

type packageUnderTest struct {
	fset  *token.FileSet
	files []*ast.File
	pkg   *types.Package
	info  *types.Info
}

type errString string

func (e errString) Error() string { return string(e) }

const errNoImplementationFiles = errString("no non-test files found; the controls would pass vacuously")

// waitSite is one use of a wait capability, located for a readable failure.
type waitSite struct {
	fn   string
	recv string
	pos  string
}

// waitSites reports every use of a wait function or method in the package's
// non-test files.
//
// Sites are counted at the identifier, not at any particular expression shape.
// The type checker resolves each identifier to the object it denotes, so one
// rule covers a method call, a method value, a method expression, a qualified
// package function, and a bare call to a function in this package or a
// dot-imported one. Matching on an expression shape instead would leave
// whichever shapes were not enumerated silently uncounted.
func (p *packageUnderTest) waitSites() []waitSite {
	var sites []waitSite
	for _, file := range p.files {
		// A selector records the receiver for the diagnostic. ast.Inspect is
		// pre-order, so a selector is always seen before its own identifier.
		recvByIdent := map[*ast.Ident]string{}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				if selection, ok := p.info.Selections[node]; ok {
					recvByIdent[node.Sel] = selection.Recv().String()
				}
			case *ast.Ident:
				if !isWaitName(node.Name) {
					return true
				}
				// Uses holds references, not declarations, so declaring a wait helper
				// is not itself a site — calling it is. A type reference such as
				// sync.WaitGroup resolves to a TypeName and is not a wait capability.
				obj, ok := p.info.Uses[node]
				if !ok {
					return true
				}
				if _, isFunc := obj.(*types.Func); !isFunc {
					return true
				}
				recv, ok := recvByIdent[node]
				if !ok {
					recv = "package-level function " + obj.(*types.Func).FullName()
				}
				sites = append(sites, waitSite{
					fn:   enclosingFunc(file, node.Pos()),
					recv: recv,
					pos:  p.fset.Position(node.Pos()).String(),
				})
			}
			return true
		})
	}
	return sites
}

// TestExactlyOneWaitSite pins that the package waits on the child in one place,
// and that the place is the owner established in Start. A wait added to
// teardown, to a helper, or to a second file in the package fails here —
// including one that bypasses the counted wrapper by calling the process or the
// command directly, and one that is taken as a method value rather than called.
func TestExactlyOneWaitSite(t *testing.T) {
	pkg := mustLoad(t)

	sites := pkg.waitSites()
	if len(sites) != 1 {
		for _, site := range sites {
			t.Logf("wait on %s in %s at %s", site.recv, site.fn, site.pos)
		}
		t.Fatalf("found %d wait sites across the package, want exactly one", len(sites))
	}
	if sites[0].fn != "Start" {
		t.Fatalf("the sole wait belongs to the owner established in Start, found it in %s", sites[0].fn)
	}
	if got := sites[0].recv; !strings.HasSuffix(got, "exec.Cmd") {
		t.Fatalf("the sole wait is on %s, want the retained *exec.Cmd", got)
	}
}

// TestResultIsStoredBeforeCompletionIsPublished pins the publication order, and
// pins it to the routine that owns the wait. Closing the completion channel
// before storing the result would let an observer see an exit while the result
// is still absent, and teardown decide the child is still running. Swapping the
// two statements fails here, and so does moving the pair out of the owner, where
// the order would no longer be established alongside the wait it describes.
func TestResultIsStoredBeforeCompletionIsPublished(t *testing.T) {
	pkg := mustLoad(t)

	sites := pkg.waitSites()
	if len(sites) != 1 {
		t.Fatalf("found %d wait sites, want exactly one to anchor the publication order to", len(sites))
	}
	owner := pkg.funcDecl(sites[0].fn)
	if owner == nil {
		t.Fatalf("no declaration found for the wait owner %s", sites[0].fn)
	}

	var stores, closes []token.Pos
	ast.Inspect(owner, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			for _, lhs := range node.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "err" {
					stores = append(stores, node.Pos())
				}
			}
		case *ast.CallExpr:
			if ident, ok := node.Fun.(*ast.Ident); ok && ident.Name == "close" {
				closes = append(closes, node.Pos())
			}
		}
		return true
	})

	if len(stores) != 1 {
		t.Fatalf("found %d result stores in %s, want exactly one", len(stores), sites[0].fn)
	}
	if len(closes) != 1 {
		t.Fatalf("found %d completion publications in %s, want exactly one", len(closes), sites[0].fn)
	}
	if stores[0] > closes[0] {
		t.Fatalf("the result is stored at %s, after completion is published at %s; it must come first",
			pkg.fset.Position(stores[0]), pkg.fset.Position(closes[0]))
	}
}

// TestNoExportedWaitCapableHandle pins that the package hands out no way to wait
// on the child, since a second waiter obtained by a caller is the same defect
// arriving from outside. The rule is about capability, not spelling: any
// exported result or field whose method set carries a wait fails here, whether
// it is written as *os.Process, *exec.Cmd, or an interface that happens to
// include one.
func TestNoExportedWaitCapableHandle(t *testing.T) {
	pkg := mustLoad(t)

	scope := pkg.pkg.Scope()
	for _, name := range scope.Names() {
		obj := scope.Lookup(name)
		if !obj.Exported() {
			continue
		}
		switch typed := obj.(type) {
		case *types.Func:
			checkNoWaitInResults(t, typed.Name(), typed.Type().(*types.Signature))
		case *types.Var:
			if capability := waitCapability(typed.Type()); capability != "" {
				t.Errorf("exported variable %s is a %s, which exposes %s", name, typed.Type(), capability)
			}
		case *types.TypeName:
			named, ok := typed.Type().(*types.Named)
			if !ok {
				continue
			}
			for i := 0; i < named.NumMethods(); i++ {
				method := named.Method(i)
				if method.Exported() {
					checkNoWaitInResults(t, name+"."+method.Name(), method.Type().(*types.Signature))
				}
			}
			structType, ok := named.Underlying().(*types.Struct)
			if !ok {
				continue
			}
			for i := 0; i < structType.NumFields(); i++ {
				field := structType.Field(i)
				if !field.Exported() {
					continue
				}
				if capability := waitCapability(field.Type()); capability != "" {
					t.Errorf("exported field %s.%s is a %s, which exposes %s", name, field.Name(), field.Type(), capability)
				}
			}
		}
	}
}

func checkNoWaitInResults(t *testing.T, what string, sig *types.Signature) {
	t.Helper()
	results := sig.Results()
	for i := 0; i < results.Len(); i++ {
		if capability := waitCapability(results.At(i).Type()); capability != "" {
			t.Errorf("%s returns a %s, which exposes %s; expose only what a caller needs",
				what, results.At(i).Type(), capability)
		}
	}
}

// waitCapability names the wait method a caller could reach through t, or "" if
// there is none. Both t and *t are considered, since a caller holding an
// addressable value reaches the pointer method set too.
func waitCapability(t types.Type) string {
	for _, candidate := range []types.Type{t, types.NewPointer(t)} {
		methods := types.NewMethodSet(candidate)
		for i := 0; i < methods.Len(); i++ {
			method := methods.At(i).Obj()
			if isWaitName(method.Name()) {
				return method.Name()
			}
		}
	}
	return ""
}

// isWaitName reports whether name is one of the spellings used to reap a child:
// Wait, or Wait followed by digits such as Wait4. Names that merely begin with
// Wait — the WaitCalls counter on Child, for one — are not wait capabilities and
// must not be treated as if they were.
func isWaitName(name string) bool {
	if !strings.HasPrefix(name, "Wait") {
		return false
	}
	for _, r := range name[len("Wait"):] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func mustLoad(t *testing.T) *packageUnderTest {
	t.Helper()
	pkg, err := loadPackage()
	if err != nil {
		t.Fatalf("load package under test: %v", err)
	}
	return pkg
}

// funcDecl finds a top-level declaration by name across the package's files.
func (p *packageUnderTest) funcDecl(name string) *ast.FuncDecl {
	for _, file := range p.files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
				return fn
			}
		}
	}
	return nil
}

// enclosingFunc names the function a position falls inside, for a readable
// failure.
func enclosingFunc(file *ast.File, pos token.Pos) string {
	name := "<file scope>"
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Pos() <= pos && pos <= fn.End() {
			name = fn.Name.Name
		}
	}
	return name
}
