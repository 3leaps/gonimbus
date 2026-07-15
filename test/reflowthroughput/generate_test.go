package reflowthroughput

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateDeterministicManifest(t *testing.T) {
	t.Parallel()
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	r := DefaultSmokeRecipe()
	r.ObjectCount = 8

	c1, err := Generate(GenerateOptions{Recipe: r, RunRoot: dir1})
	if err != nil {
		t.Fatal(err)
	}
	c2, err := Generate(GenerateOptions{Recipe: r, RunRoot: dir2})
	if err != nil {
		t.Fatal(err)
	}
	if c1.Manifest.Digest != c2.Manifest.Digest {
		t.Fatalf("digest mismatch for same recipe: %s vs %s", c1.Manifest.Digest, c2.Manifest.Digest)
	}
	if c1.Manifest.ObjectCount != 8 {
		t.Fatalf("count=%d", c1.Manifest.ObjectCount)
	}
	// Changed seed changes digest.
	r2 := r
	r2.Seed = 99
	dir3 := t.TempDir()
	c3, err := Generate(GenerateOptions{Recipe: r2, RunRoot: dir3})
	if err != nil {
		t.Fatal(err)
	}
	if c3.Manifest.Digest == c1.Manifest.Digest {
		t.Fatal("expected different digest for different seed")
	}
	// Files exist and reflow input has N lines.
	b, err := os.ReadFile(c1.ReflowInputPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := 0
	for _, line := range splitLines(string(b)) {
		if line != "" {
			lines++
		}
	}
	if lines != 8 {
		t.Fatalf("reflow input lines=%d", lines)
	}
	// Probe config present.
	if _, err := os.Stat(c1.ProbeConfigPath); err != nil {
		t.Fatal(err)
	}
	// Verify unchanged helper.
	if err := VerifyManifestUnchanged(c1.ManifestPath, c1.Manifest.Digest); err != nil {
		t.Fatal(err)
	}
	// Sample object content has Marker.
	sample := filepath.Join(c1.Root, filepath.FromSlash(r.RelativeKey(0)))
	body, err := os.ReadFile(sample)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(string(body), "<Marker>") {
		t.Fatalf("missing Marker in content: %s", body[:min(80, len(body))])
	}
}

func TestEnsureEmptyAndAbsent(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dest := filepath.Join(root, "d")
	if err := EnsureEmptyDir(dest); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dest, "x"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := EnsureEmptyDir(dest); err == nil {
		t.Fatal("expected nonempty dest error")
	}
	ck := filepath.Join(root, "c.db")
	if err := EnsureAbsent(ck); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ck, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := EnsureAbsent(ck); err == nil {
		t.Fatal("expected existing checkpoint error")
	}
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		(func() bool {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		})())
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
