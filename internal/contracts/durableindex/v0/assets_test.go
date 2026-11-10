package v0

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func TestPinnedUpstreamClosure(t *testing.T) {
	if err := Verify(); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if err := VerifyFilesystem(assetRoot); err != nil {
		t.Fatalf("VerifyFilesystem(%q) error = %v", assetRoot, err)
	}

	got := Inventory()
	if got.SourceTag != expectedSourceTag ||
		got.SourceTagObject != expectedSourceTagObject ||
		got.SourceCommit != expectedSourceCommit ||
		got.ExportSHA256 != expectedExportSHA256 ||
		got.InventorySHA256 != expectedInventorySHA256 ||
		got.FileCount != expectedInventoryFiles ||
		got.TotalBytes != expectedInventoryBytes {
		t.Fatalf("Inventory() = %#v", got)
	}

	upstreamFS, err := FS()
	if err != nil {
		t.Fatalf("FS() error = %v", err)
	}
	if _, err := fs.Stat(upstreamFS, contractPath); err != nil {
		t.Fatalf("FS().Stat(%q) error = %v", contractPath, err)
	}
}

func TestVerifyFilesystemRejectsClosureMutations(t *testing.T) {
	const target = IndexPublicationSchemaPath

	cases := []struct {
		name string
		mut  func(t *testing.T, root string)
		want string
	}{
		{
			name: "missing",
			mut: func(t *testing.T, root string) {
				t.Helper()
				if err := os.Remove(filepath.Join(root, filepath.FromSlash(target))); err != nil {
					t.Fatal(err)
				}
			},
			want: "missing",
		},
		{
			name: "same-size changed",
			mut: func(t *testing.T, root string) {
				t.Helper()
				name := filepath.Join(root, filepath.FromSlash(target))
				data, err := os.ReadFile(name) // #nosec G304 -- test mutates disposable temp copy
				if err != nil {
					t.Fatal(err)
				}
				data[0] ^= 1
				if err := os.WriteFile(name, data, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "digest mismatch",
		},
		{
			name: "extra",
			mut: func(t *testing.T, root string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, "unexpected.json"), []byte("{}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "unexpected",
		},
		{
			name: "renamed",
			mut: func(t *testing.T, root string) {
				t.Helper()
				if err := os.Rename(filepath.Join(root, filepath.FromSlash(target)), filepath.Join(root, "renamed.json")); err != nil {
					t.Fatal(err)
				}
			},
			want: "unexpected",
		},
		{
			name: "symlink leaf",
			mut: func(t *testing.T, root string) {
				t.Helper()
				name := filepath.Join(root, filepath.FromSlash(target))
				data, err := os.ReadFile(name) // #nosec G304 -- test reads disposable temp copy
				if err != nil {
					t.Fatal(err)
				}
				outside := filepath.Join(t.TempDir(), "identical.json")
				if err := os.WriteFile(outside, data, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(name); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, name); err != nil {
					t.Skipf("symlink unavailable: %v", err)
				}
			},
			want: "non-regular",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := copyPinnedUpstream(t)
			tc.mut(t, root)
			err := VerifyFilesystem(root)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("VerifyFilesystem() error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestVerifyFilesystemRejectsRootSymlinkSpellings(t *testing.T) {
	intact := copyPinnedUpstream(t)
	holder := t.TempDir()
	alias := filepath.Join(holder, "alias")
	if err := os.Symlink(intact, alias); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	parentTarget := filepath.Join(holder, "parent-real")
	if err := os.MkdirAll(parentTarget, 0o750); err != nil {
		t.Fatal(err)
	}
	nestedRoot := filepath.Join(parentTarget, "upstream")
	if err := os.Rename(intact, nestedRoot); err != nil {
		t.Fatal(err)
	}
	intact = nestedRoot
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(intact, alias); err != nil {
		t.Fatal(err)
	}
	parentAlias := filepath.Join(holder, "parent-alias")
	if err := os.Symlink(parentTarget, parentAlias); err != nil {
		t.Fatal(err)
	}
	sep := string(os.PathSeparator)
	for _, spelling := range []string{
		alias,
		alias + sep,
		alias + sep + ".",
		alias + sep + ".." + sep + "parent-real" + sep + "upstream",
		parentAlias + sep + "upstream",
		intact + sep + "LICENSE" + sep + "..",
	} {
		if err := VerifyFilesystem(spelling); err == nil || !strings.Contains(err.Error(), "not a directory") {
			t.Fatalf("VerifyFilesystem(%q) error = %v, want symlink or non-directory refusal", spelling, err)
		}
	}
	missingParent := holder + sep + "definitely-missing" + sep + ".." + sep + "parent-real" + sep + "upstream"
	if err := VerifyFilesystem(missingParent); err == nil {
		t.Fatalf("VerifyFilesystem(%q) unexpectedly succeeded on missing intermediate component", missingParent)
	}

	origWD, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(origWD) })
	if err := os.Chdir(holder); err != nil {
		t.Fatal(err)
	}
	for _, relSpelling := range []string{
		"alias",
		"alias/",
		"alias/.",
		"alias/../parent-real/upstream",
		"parent-alias/upstream",
		"parent-real/upstream/LICENSE/..",
	} {
		if err := VerifyFilesystem(relSpelling); err == nil || !strings.Contains(err.Error(), "not a directory") {
			t.Fatalf("VerifyFilesystem(%q) error = %v, want relative symlink or non-directory refusal", relSpelling, err)
		}
	}
	if err := VerifyFilesystem("definitely-missing/../parent-real/upstream"); err == nil {
		t.Fatal("VerifyFilesystem(definitely-missing/../parent-real/upstream) unexpectedly succeeded")
	}

	realNested := filepath.Join(holder, "real", "nested")
	emptyPin := filepath.Join(holder, "real", "pin")
	if err := os.MkdirAll(realNested, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(emptyPin, 0o750); err != nil {
		t.Fatal(err)
	}
	deepAlias := filepath.Join(holder, "deep-alias")
	if err := os.Symlink(realNested, deepAlias); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", deepAlias)
	for _, tricky := range []string{
		deepAlias + sep + ".." + sep + "parent-real" + sep + "upstream",
		"deep-alias/../parent-real/upstream",
	} {
		if err := VerifyFilesystem(tricky); err == nil || !strings.Contains(err.Error(), "not a directory") {
			t.Fatalf("VerifyFilesystem(%q) with TMPDIR=deep-alias error = %v, want symlink traversal refusal", tricky, err)
		}
	}

	if os.PathSeparator == '/' {
		decoyDir := filepath.Join(holder, "bs-alias", "name", "upstream")
		if err := os.MkdirAll(decoyDir, 0o750); err != nil {
			t.Fatal(err)
		}
		bsAlias := filepath.Join(holder, `bs-alias\name`)
		if err := os.Symlink(parentTarget, bsAlias); err != nil {
			t.Fatal(err)
		}
		for _, bsSpelling := range []string{
			filepath.Join(bsAlias, "upstream"),
			`bs-alias\name/upstream`,
		} {
			if err := VerifyFilesystem(bsSpelling); err == nil || !strings.Contains(err.Error(), "not a directory") {
				t.Fatalf("VerifyFilesystem(%q) error = %v, want POSIX backslash symlink ancestor refusal", bsSpelling, err)
			}
		}

		realBS := filepath.Join(holder, `real\bs`)
		if err := os.MkdirAll(realBS, 0o750); err != nil {
			t.Fatal(err)
		}
		bsIntact := filepath.Join(realBS, "upstream")
		if err := os.Rename(intact, bsIntact); err != nil {
			t.Fatal(err)
		}
		if err := VerifyFilesystem(bsIntact); err != nil {
			t.Fatalf("VerifyFilesystem(%q) unexpected error on real backslash directory = %v", bsIntact, err)
		}
		if err := VerifyFilesystem(`real\bs/upstream`); err != nil {
			t.Fatalf("VerifyFilesystem(real\\bs/upstream) unexpected error = %v", err)
		}
	}
}

func TestSplitVolumeComponentsWindowsAndUNC(t *testing.T) {
	candidates := func(steps []volumeComponentStep) string {
		out := make([]string, 0, len(steps))
		for _, s := range steps {
			out = append(out, s.candidate)
		}
		return strings.Join(out, "|")
	}
	if got := candidates(walkVolumeComponentPrefixes(`C:\fixtures\upstream`, `C:`, '\\')); got != `C:\fixtures|C:\fixtures\upstream` {
		t.Fatalf("drive rooted steps = %q, want %q", got, `C:\fixtures|C:\fixtures\upstream`)
	}
	if got := candidates(walkVolumeComponentPrefixes(`C:fixtures\upstream`, `C:`, '\\')); got != `C:fixtures|C:fixtures\upstream` {
		t.Fatalf("drive relative steps = %q, want %q", got, `C:fixtures|C:fixtures\upstream`)
	}
	if got := candidates(walkVolumeComponentPrefixes(`\\server\share\fixtures\upstream`, `\\server\share`, '\\')); got != `\\server\share\fixtures|\\server\share\fixtures\upstream` {
		t.Fatalf("unc steps = %q, want %q", got, `\\server\share\fixtures|\\server\share\fixtures\upstream`)
	}
	if got := candidates(walkVolumeComponentPrefixes(`/tmp/a\b/upstream`, "", '/')); got != `/tmp|/tmp/a\b|/tmp/a\b/upstream` {
		t.Fatalf("posix backslash steps = %q, want %q", got, `/tmp|/tmp/a\b|/tmp/a\b/upstream`)
	}
}

func TestPinRejectsCoherentInventoryRepin(t *testing.T) {
	const target = IndexPublicationSchemaPath
	files := pinnedClosureMap(t)
	manifest, err := loadPin(files)
	if err != nil {
		t.Fatal(err)
	}
	asset := assetRoot + "/" + target
	data := append([]byte(nil), files[asset].Data...)
	data[0] ^= 1
	files[asset] = &fstest.MapFile{Data: data, Mode: 0o644}

	for i := range manifest.Inventory.Files {
		if manifest.Inventory.Files[i].Path == target {
			digest := sha256.Sum256(data)
			manifest.Inventory.Files[i].SHA256 = hex.EncodeToString(digest[:])
		}
	}
	h := sha256.New()
	for _, file := range manifest.Inventory.Files {
		itemData := files[assetRoot+"/"+file.Path].Data
		_, _ = h.Write([]byte(file.Path))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(itemData)
	}
	manifest.Inventory.SHA256 = hex.EncodeToString(h.Sum(nil))
	pin, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	files["pin.json"] = &fstest.MapFile{Data: pin, Mode: 0o644}

	if _, err := loadPin(files); err == nil || !strings.Contains(err.Error(), "invalid inventory metadata") {
		t.Fatalf("loadPin(coherently repinned closure) error = %v", err)
	}
}

func TestValidateUpstreamShapeCasesOffline(t *testing.T) {
	upstreamFS, err := FS()
	if err != nil {
		t.Fatal(err)
	}
	rawCases, err := fs.ReadFile(upstreamFS, "schemas/durable-index/v0/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var inventory struct {
		Shape []struct {
			File   string `json:"file"`
			Schema string `json:"schema"`
			Valid  bool   `json:"valid"`
		} `json:"shape"`
	}
	if err := json.Unmarshal(rawCases, &inventory); err != nil {
		t.Fatal(err)
	}
	if len(inventory.Shape) == 0 {
		t.Fatal("cases.json has no shape entries")
	}

	for _, entry := range inventory.Shape {
		t.Run(entry.File, func(t *testing.T) {
			schemaPath := path.Join("schemas/durable-index/v0", entry.Schema)
			docPath := path.Join("schemas/durable-index/v0", entry.File)
			data, err := fs.ReadFile(upstreamFS, docPath)
			if err != nil {
				t.Fatal(err)
			}
			err = ValidateJSON(schemaPath, data)
			if entry.Valid && err != nil {
				t.Fatalf("ValidateJSON(%q, %q) unexpected error = %v", schemaPath, docPath, err)
			}
			if !entry.Valid && err == nil {
				t.Fatalf("ValidateJSON(%q, %q) succeeded on invalid shape fixture", schemaPath, docPath)
			}
		})
	}
}

func TestValidateJSONStrictParserControls(t *testing.T) {
	upstreamFS, err := FS()
	if err != nil {
		t.Fatal(err)
	}
	validPlan, err := fs.ReadFile(upstreamFS, "schemas/durable-index/v0/examples/index-plan.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateJSON(IndexPlanSchemaPath, validPlan); err != nil {
		t.Fatalf("ValidateJSON(valid index-plan) error = %v", err)
	}

	deepOK := strings.Repeat("[", maxJSONContainerDepth) + "0" + strings.Repeat("]", maxJSONContainerDepth)
	if err := checkSingleJSONValue([]byte(deepOK), maxJSONContainerDepth); err != nil {
		t.Fatalf("depth %d rejected: %v", maxJSONContainerDepth, err)
	}
	deepTooFar := strings.Repeat("[", maxJSONContainerDepth+1) + "0" + strings.Repeat("]", maxJSONContainerDepth+1)
	if err := checkSingleJSONValue([]byte(deepTooFar), maxJSONContainerDepth); err == nil {
		t.Fatalf("depth %d accepted", maxJSONContainerDepth+1)
	}

	for name, payload := range map[string][]byte{
		"duplicate key":              []byte(`{"capabilities":["contract: durable-index/v0"],"capabilities":["contract: durable-index/v0"]}`),
		"trailing value":             append(append([]byte(nil), validPlan...), []byte("{}")...),
		"invalid utf8":               {0x7b, 0x22, 0xff, 0x22, 0x3a, 0x31, 0x7d},
		"lone high surrogate prefix": []byte(`{"capabilities":["contract: durable-index/v0"],"kind":"index_plan","set_id":"set-example","scope_profile":"unicode-prefix/v0","scopes":[{"namespace":"objects","prefix":"\ud800"}]}`),
		"lone low surrogate prefix":  []byte(`{"capabilities":["contract: durable-index/v0"],"kind":"index_plan","set_id":"set-example","scope_profile":"unicode-prefix/v0","scopes":[{"namespace":"objects","prefix":"\udfff"}]}`),
		"lone surrogate object key":  []byte(`{"\ud800":1}`),
		"non-finite number":          []byte(`{"a":1e400}`),
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateJSON(IndexPlanSchemaPath, payload); err == nil {
				t.Fatalf("ValidateJSON(%s) unexpectedly succeeded", name)
			}
		})
	}

	for name, payload := range map[string][]byte{
		"valid surrogate pair": []byte(`{"capabilities":["contract: durable-index/v0"],"kind":"index_plan","set_id":"set-example","scope_profile":"unicode-prefix/v0","scopes":[{"namespace":"objects","prefix":"emoji/\ud83d\ude00/"}]}`),
		"escaped backslash":    []byte(`{"capabilities":["contract: durable-index/v0"],"kind":"index_plan","set_id":"set-example","scope_profile":"unicode-prefix/v0","scopes":[{"namespace":"objects","prefix":"literal\\ud800/"}]}`),
		"escaped ufffd":        []byte(`{"capabilities":["contract: durable-index/v0"],"kind":"index_plan","set_id":"set-example","scope_profile":"unicode-prefix/v0","scopes":[{"namespace":"objects","prefix":"repl/\ufffd/"}]}`),
		"literal utf8 ufffd":   []byte("{\"capabilities\":[\"contract: durable-index/v0\"],\"kind\":\"index_plan\",\"set_id\":\"set-example\",\"scope_profile\":\"unicode-prefix/v0\",\"scopes\":[{\"namespace\":\"objects\",\"prefix\":\"repl/\xef\xbf\xbd/\"}]}"),
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateJSON(IndexPlanSchemaPath, payload); err != nil {
				t.Fatalf("ValidateJSON(%s) unexpected error = %v", name, err)
			}
		})
	}

	parsingEntries, err := fs.ReadDir(upstreamFS, "schemas/durable-index/v0/vectors/parsing")
	if err != nil {
		t.Fatal(err)
	}
	if len(parsingEntries) == 0 {
		t.Fatal("missing upstream parsing vectors")
	}
	for _, entry := range parsingEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		t.Run("upstream-parsing/"+entry.Name(), func(t *testing.T) {
			raw, readErr := fs.ReadFile(upstreamFS, path.Join("schemas/durable-index/v0/vectors/parsing", entry.Name()))
			if readErr != nil {
				t.Fatal(readErr)
			}
			var vec struct {
				Input struct {
					Hex string `json:"hex"`
				} `json:"input"`
				Expected struct {
					Refusal string `json:"refusal"`
				} `json:"expected"`
			}
			if unmarshalErr := json.Unmarshal(raw, &vec); unmarshalErr != nil {
				t.Fatal(unmarshalErr)
			}
			inputBytes, decErr := hex.DecodeString(vec.Input.Hex)
			if decErr != nil {
				t.Fatal(decErr)
			}
			if _, parseErr := StrictParseJSON(inputBytes); parseErr == nil && vec.Expected.Refusal != "" {
				t.Fatalf("StrictParseJSON(%s) accepted input expecting refusal %q", entry.Name(), vec.Expected.Refusal)
			}
		})
	}
}

func pinnedClosureMap(t *testing.T) fstest.MapFS {
	t.Helper()
	files := fstest.MapFS{}
	if err := fs.WalkDir(embedded, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := fs.ReadFile(embedded, name)
		if err != nil {
			return err
		}
		files[name] = &fstest.MapFile{Data: data, Mode: 0o644}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return files
}

func copyPinnedUpstream(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := fs.WalkDir(embedded, assetRoot, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if name == assetRoot || entry.IsDir() {
			return nil
		}
		rel := strings.TrimPrefix(name, assetRoot+"/")
		destination := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
			t.Fatal(err)
		}
		data, err := fs.ReadFile(embedded, name)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, data, 0o600)
	}); err != nil {
		t.Fatal(err)
	}
	return root
}
