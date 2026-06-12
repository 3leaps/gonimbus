package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHasLibraryAPIChangelogEntryRequiresUnreleasedBody(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{
			name: "convention only",
			body: `# Changelog

## Library API Section Convention

Use a Library API subsection when needed.

## [Unreleased]

## [0.2.2] - 2026-05-26
`,
			want: false,
		},
		{
			name: "empty subsection",
			body: `# Changelog

## [Unreleased]

### Library API

## [0.2.2] - 2026-05-26
`,
			want: false,
		},
		{
			name: "unreleased acknowledgement",
			body: `# Changelog

## [Unreleased]

### Library API

- Breaking: renamed Foo to Bar; migrate by calling Bar.

## [0.2.2] - 2026-05-26
`,
			want: true,
		},
		{
			name: "released entry does not waive",
			body: `# Changelog

## [Unreleased]

## [0.2.2] - 2026-05-26

### Library API

- Historical Stable API change.
`,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasLibraryAPIChangelogText(tt.body); got != tt.want {
				t.Fatalf("hasLibraryAPIChangelogEntry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasLibraryAPIChangelogEntryAcceptsCurrentReleaseSection(t *testing.T) {
	body := `# Changelog

## [Unreleased]

## [0.3.0] - 2026-06-12

### Library API

- Added: provider credential-refresh sentinel.

## [0.2.3] - 2026-05-31
`

	if !hasLibraryAPIChangelogTextForVersion(body, "0.3.0") {
		t.Fatal("hasLibraryAPIChangelogTextForVersion() = false, want true for current release section")
	}
	if hasLibraryAPIChangelogTextForVersion(body, "0.3.1") {
		t.Fatal("hasLibraryAPIChangelogTextForVersion() = true, want false for a different release section")
	}
}

func TestDiffSnapshotsReportsStableSymbolChanges(t *testing.T) {
	base := map[string]map[string]string{
		"github.com/3leaps/gonimbus/pkg/uri": {
			"func ParseURI":  "func(raw string) (*ObjectURI, error)",
			"type ObjectURI": "struct{ Provider string }",
		},
	}
	current := map[string]map[string]string{
		"github.com/3leaps/gonimbus/pkg/uri": {
			"func ParseURI": "func(raw string) (*ObjectURI, error)",
			"func NewURI":   "func(provider string) *ObjectURI",
		},
	}

	changes := diffSnapshots(base, current)
	want := []string{
		"added github.com/3leaps/gonimbus/pkg/uri func NewURI",
		"removed github.com/3leaps/gonimbus/pkg/uri type ObjectURI",
	}
	if len(changes) != len(want) {
		t.Fatalf("len(changes) = %d, want %d: %v", len(changes), len(want), changes)
	}
	for i := range want {
		if changes[i] != want[i] {
			t.Fatalf("changes[%d] = %q, want %q", i, changes[i], want[i])
		}
	}
}

func TestExportedSnapshotIgnoresUnexportedStructFields(t *testing.T) {
	base, err := exportedSnapshot(map[string][]byte{
		"pkg/provider/s3/provider.go": []byte(`package s3

type Provider struct {
	client string
	Bucket string
}
`),
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := exportedSnapshot(map[string][]byte{
		"pkg/provider/s3/provider.go": []byte(`package s3

type Provider struct {
	client string
	cache map[string]string
	Bucket string
}
`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if changes := diffSnapshots(base, current); len(changes) != 0 {
		t.Fatalf("diffSnapshots() = %v, want no public API changes", changes)
	}

	current["github.com/3leaps/gonimbus/pkg/provider/s3"]["type Provider"] = "struct{ Bucket int }"
	changes := diffSnapshots(base, current)
	if len(changes) != 1 || changes[0] != "changed github.com/3leaps/gonimbus/pkg/provider/s3 type Provider" {
		t.Fatalf("diffSnapshots() = %v, want exported field type change", changes)
	}
}

func TestParseManifestRejectsDuplicateRows(t *testing.T) {
	dir := t.TempDir()
	manifest := filepath.Join(dir, "api-stability.md")
	if err := os.WriteFile(manifest, []byte(`| Import path | Tier | Notes |
| --- | --- | --- |
| `+"`github.com/3leaps/gonimbus/pkg/uri`"+` | Stable | URI parsing. |
| `+"`github.com/3leaps/gonimbus/pkg/uri`"+` | Stable | Duplicate. |
`), 0o600); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseManifestContent(manifest, string(data)); err == nil {
		t.Fatal("parseManifest() error = nil, want duplicate entry error")
	}
}

func TestCheckManifestRejectsUnexpectedStablePackage(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module github.com/3leaps/gonimbus\n\ngo 1.25\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, pkg := range []string{
		"pkg/atlas",
		"pkg/match",
		"pkg/provider",
		"pkg/provider/file",
		"pkg/provider/s3",
		"pkg/uri",
	} {
		if err := os.MkdirAll(filepath.Join(dir, pkg), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, pkg, "doc.go"), []byte("package "+filepath.Base(pkg)+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest := `# Library API Stability

| Import path | Tier | Notes |
| --- | --- | --- |
| ` + "`github.com/3leaps/gonimbus/pkg/atlas`" + ` | Stable | Wrongly promoted. |
| ` + "`github.com/3leaps/gonimbus/pkg/match`" + ` | Stable | Matching. |
| ` + "`github.com/3leaps/gonimbus/pkg/provider`" + ` | Stable | Provider. |
| ` + "`github.com/3leaps/gonimbus/pkg/provider/file`" + ` | Stable | File provider. |
| ` + "`github.com/3leaps/gonimbus/pkg/provider/s3`" + ` | Stable | S3 provider. |
| ` + "`github.com/3leaps/gonimbus/pkg/uri`" + ` | Stable | URI parsing. |
`
	if err := os.WriteFile(filepath.Join(dir, "docs-api-stability.md"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}

	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(oldwd); err != nil {
			t.Fatalf("restore wd: %v", err)
		}
	})
	if err := os.MkdirAll("docs", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename("docs-api-stability.md", "docs/api-stability.md"); err != nil {
		t.Fatal(err)
	}

	err = checkManifest()
	if err == nil {
		t.Fatal("checkManifest() error = nil, want unexpected Stable package error")
	}
	if !strings.Contains(err.Error(), "unexpected Stable package not covered by diff gate: github.com/3leaps/gonimbus/pkg/atlas") {
		t.Fatalf("checkManifest() error = %v", err)
	}
}
