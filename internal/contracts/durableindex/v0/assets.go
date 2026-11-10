// Package v0 embeds and verifies the content-pinned durable-index/v0 contract
// closure and its composed segmented-snapshot/v0, coverage-attestation/v0, and
// data-artifact/v0 schema families.
package v0

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// Schema paths within the pinned upstream closure.
const (
	IndexCommonSchemaPath         = "schemas/durable-index/v0/common.schema.json"
	IndexPublicationSchemaPath    = "schemas/durable-index/v0/index-publication.schema.json"
	IndexPlanSchemaPath           = "schemas/durable-index/v0/index-plan.schema.json"
	IndexRowSchemaPath            = "schemas/durable-index/v0/index-row.schema.json"
	IndexMetadataSchemaPath       = "schemas/durable-index/v0/index-metadata.schema.json"
	IndexCutoverSchemaPath        = "schemas/durable-index/v0/index-cutover.schema.json"
	SnapshotCommonSchemaPath      = "schemas/segmented-snapshot/v0/common.schema.json"
	SnapshotManifestSchemaPath    = "schemas/segmented-snapshot/v0/snapshot-manifest.schema.json"
	SnapshotProfileSchemaPath     = "schemas/segmented-snapshot/v0/snapshot-profile.schema.json"
	SnapshotPublicationSchemaPath = "schemas/segmented-snapshot/v0/snapshot-publication.schema.json"
	VerificationResultSchemaPath  = "schemas/segmented-snapshot/v0/verification-result.schema.json"
	CoverageAttestationSchemaPath = "schemas/coverage-attestation/v0/coverage-attestation.schema.json"
	ArtifactDescriptorSchemaPath  = "schemas/data-artifact/v0/artifact-descriptor.schema.json"
)

const (
	assetRoot               = "upstream"
	contractPath            = "schemas/durable-index/v0/contract.json"
	maxValidatedJSONBytes   = 16 << 20
	maxJSONContainerDepth   = 256
	expectedPinVersion      = "gonimbus/durable-index-pin/v1"
	expectedSourceRepo      = "3leaps/crucible"
	expectedSourceTag       = "v0.2.9"
	expectedSourceTagObject = "fdd8b222a67a8f34fdabab9d641a35e7b6a0f36d"
	expectedSourceCommit    = "0e1e5f9b1f2e262a72ff166086d9543199b59300"
	expectedExportSHA256    = "3626bf0b2fd620d5cacaad6a6bda0edbcc64cb1a211b9006360fbf14c98585a2"
	expectedInventorySHA256 = "7f745f62845077139a633870f8bee989b181e7279dcffc60ed1b6f7f5e5d807f"
	expectedInventoryFiles  = 256
	expectedInventoryBytes  = 1278178
)

var allNormativeSchemas = []string{
	IndexCommonSchemaPath,
	IndexPublicationSchemaPath,
	IndexPlanSchemaPath,
	IndexRowSchemaPath,
	IndexMetadataSchemaPath,
	IndexCutoverSchemaPath,
	SnapshotCommonSchemaPath,
	SnapshotManifestSchemaPath,
	SnapshotProfileSchemaPath,
	SnapshotPublicationSchemaPath,
	VerificationResultSchemaPath,
	CoverageAttestationSchemaPath,
	ArtifactDescriptorSchemaPath,
}

//go:embed all:upstream pin.json
var embedded embed.FS

type pinManifest struct {
	Version   string            `json:"version"`
	Source    pinSource         `json:"source"`
	Licenses  pinLicenses       `json:"licenses"`
	Contract  pinContract       `json:"contract"`
	Trees     map[string]string `json:"trees"`
	Inventory pinInventory      `json:"inventory"`
}

type pinSource struct {
	Repository         string   `json:"repository"`
	Tag                string   `json:"tag"`
	AnnotatedTagObject string   `json:"annotated_tag_object"`
	ResolvedCommit     string   `json:"resolved_commit"`
	ExportPaths        []string `json:"export_paths"`
	ExportSHA256       string   `json:"export_sha256"`
	ReleaseAssets      []string `json:"release_assets"`
}

type pinLicenses struct {
	Code         pinLicenseFile `json:"code"`
	DataAndSpecs pinLicenseFile `json:"data_and_specs"`
	Notice       pinNoticeFile  `json:"notice"`
}

type pinLicenseFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	SPDX   string `json:"spdx"`
}

type pinNoticeFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type pinContract struct {
	Path              string                         `json:"path"`
	Capability        string                         `json:"capability"`
	EntrySchema       string                         `json:"entry_schema"`
	CommonSchema      string                         `json:"common_schema"`
	ObjectSchemas     map[string]string              `json:"object_schemas"`
	ComposedContracts map[string]pinComposedContract `json:"composed_contracts"`
}

type pinComposedContract struct {
	Path    string   `json:"path"`
	Schemas []string `json:"schemas"`
}

type pinInventory struct {
	Algorithm  string    `json:"algorithm"`
	SHA256     string    `json:"sha256"`
	FileCount  int       `json:"file_count"`
	TotalBytes int64     `json:"total_bytes"`
	Files      []pinFile `json:"files"`
}

type pinFile struct {
	Path    string `json:"path"`
	SHA256  string `json:"sha256"`
	GitBlob string `json:"git_blob"`
	Bytes   int64  `json:"bytes"`
}

// InventoryMetadata identifies the upstream revision and complete embedded closure.
type InventoryMetadata struct {
	SourceTag       string
	SourceTagObject string
	SourceCommit    string
	ExportSHA256    string
	InventorySHA256 string
	FileCount       int
	TotalBytes      int64
}

var (
	verifiedOnce sync.Once
	verifiedErr  error
	compiledMu   sync.Mutex
	compiledMap  = make(map[string]*jsonschema.Schema)
)

// Inventory returns immutable metadata for the embedded pin. A malformed
// compiled-in manifest returns zero metadata; callers that admit a contract
// object must call Verify or ValidateJSON.
func Inventory() InventoryMetadata {
	manifest, err := loadPin(embedded)
	if err != nil {
		return InventoryMetadata{}
	}
	return inventoryMetadata(manifest)
}

// Verify checks the entire embedded closure, its provenance manifest, and all
// normative durable-index/v0 + composed schemas without network access.
func Verify() error {
	manifest, err := loadPin(embedded)
	if err != nil {
		return err
	}
	if err := verifyAssets(embedded, assetRoot, manifest); err != nil {
		return err
	}
	return compileAllSchemas(embedded, assetRoot)
}

// VerifyFilesystem checks an extracted copy of the pinned upstream closure. It
// rejects missing, extra, changed, non-regular, and symlinked assets before
// compiling all normative schemas offline.
func VerifyFilesystem(root string) error {
	manifest, err := loadPin(embedded)
	if err != nil {
		return err
	}
	cleanRoot, err := verifyFilesystem(root, manifest)
	if err != nil {
		return err
	}
	return compileAllSchemas(os.DirFS(cleanRoot), ".")
}

// FS verifies the embedded closure once and returns a read-only filesystem
// rooted at the upstream directory.
func FS() (fs.FS, error) {
	verifiedOnce.Do(func() {
		verifiedErr = Verify()
	})
	if verifiedErr != nil {
		return nil, verifiedErr
	}
	return fs.Sub(embedded, assetRoot)
}

// CompileSchema verifies the embedded closure once and returns the compiled
// offline JSON Schema for one of the pinned normative schema paths.
func CompileSchema(schemaPath string) (*jsonschema.Schema, error) {
	if _, err := FS(); err != nil {
		return nil, err
	}
	compiledMu.Lock()
	defer compiledMu.Unlock()
	if s, ok := compiledMap[schemaPath]; ok {
		return s, nil
	}
	s, err := compileSchema(embedded, assetRoot, schemaPath)
	if err != nil {
		return nil, err
	}
	compiledMap[schemaPath] = s
	return s, nil
}

// ValidateJSON strictly parses data (UTF-8 valid, no lone surrogates, finite
// numbers, single JSON value, no duplicate keys, max container depth 256) and
// validates it offline against the pinned schema at schemaPath.
func ValidateJSON(schemaPath string, data []byte) error {
	schema, err := CompileSchema(schemaPath)
	if err != nil {
		return err
	}
	value, err := StrictParseJSON(data)
	if err != nil {
		return fmt.Errorf("durable-index JSON (%s): %w", schemaPath, err)
	}
	if err := schema.Validate(value); err != nil {
		return fmt.Errorf("durable-index schema validation (%s): %w", schemaPath, err)
	}
	return nil
}

// StrictParseJSON parses a single UTF-8 JSON value while enforcing the
// durable-index/v0 strict parser controls: no invalid UTF-8, no unpaired
// Unicode surrogate escapes, no duplicate object member names, no non-finite
// numbers, no trailing tokens, and max container depth 256.
func StrictParseJSON(data []byte) (any, error) {
	return decodeValidatedJSON(data)
}

func inventoryMetadata(manifest pinManifest) InventoryMetadata {
	return InventoryMetadata{
		SourceTag:       manifest.Source.Tag,
		SourceTagObject: manifest.Source.AnnotatedTagObject,
		SourceCommit:    manifest.Source.ResolvedCommit,
		ExportSHA256:    manifest.Source.ExportSHA256,
		InventorySHA256: manifest.Inventory.SHA256,
		FileCount:       manifest.Inventory.FileCount,
		TotalBytes:      manifest.Inventory.TotalBytes,
	}
}

func loadPin(fsys fs.FS) (pinManifest, error) {
	data, err := fs.ReadFile(fsys, "pin.json")
	if err != nil {
		return pinManifest{}, fmt.Errorf("durable-index pin manifest: %w", err)
	}
	var manifest pinManifest
	if err := decodeExactJSON(data, &manifest); err != nil {
		return pinManifest{}, fmt.Errorf("durable-index pin manifest: %w", err)
	}
	if err := validatePin(manifest); err != nil {
		return pinManifest{}, fmt.Errorf("durable-index pin manifest: %w", err)
	}
	return manifest, nil
}

func validatePin(manifest pinManifest) error {
	if manifest.Version != expectedPinVersion {
		return errors.New("unsupported version")
	}
	if manifest.Source.Repository != expectedSourceRepo ||
		manifest.Source.Tag != expectedSourceTag ||
		manifest.Source.AnnotatedTagObject != expectedSourceTagObject ||
		manifest.Source.ResolvedCommit != expectedSourceCommit ||
		manifest.Source.ExportSHA256 != expectedExportSHA256 ||
		len(manifest.Source.ExportPaths) == 0 ||
		!slices.IsSorted(manifest.Source.ExportPaths) ||
		len(manifest.Source.ReleaseAssets) != 0 {
		return errors.New("unexpected source provenance")
	}
	for _, p := range manifest.Source.ExportPaths {
		if !validAssetPath(p) {
			return errors.New("invalid export path")
		}
	}
	if manifest.Licenses.Code.Path != "LICENSE" || manifest.Licenses.Code.SPDX != "MIT" || !isLowerHex(manifest.Licenses.Code.SHA256, sha256.Size*2) ||
		manifest.Licenses.DataAndSpecs.Path != "LICENSE-CC0" || manifest.Licenses.DataAndSpecs.SPDX != "CC0-1.0" || !isLowerHex(manifest.Licenses.DataAndSpecs.SHA256, sha256.Size*2) ||
		manifest.Licenses.Notice.Path != "NOTICE" || !isLowerHex(manifest.Licenses.Notice.SHA256, sha256.Size*2) {
		return errors.New("invalid license pin")
	}
	if err := validateContractLinks(manifest.Contract, manifest.Trees); err != nil {
		return err
	}
	if manifest.Inventory.Algorithm != "sha256(path + NUL + bytes)" ||
		manifest.Inventory.FileCount != expectedInventoryFiles ||
		manifest.Inventory.TotalBytes != expectedInventoryBytes ||
		manifest.Inventory.SHA256 != expectedInventorySHA256 ||
		len(manifest.Inventory.Files) != manifest.Inventory.FileCount {
		return errors.New("invalid inventory metadata")
	}
	var total int64
	previous := ""
	seen := make(map[string]struct{}, len(manifest.Inventory.Files))
	for _, file := range manifest.Inventory.Files {
		if !validAssetPath(file.Path) || file.Path <= previous ||
			!isLowerHex(file.SHA256, sha256.Size*2) ||
			!isLowerHex(file.GitBlob, 40) ||
			file.Bytes < 0 {
			return errors.New("invalid inventory file entry")
		}
		if _, exists := seen[file.Path]; exists {
			return errors.New("duplicate inventory file")
		}
		seen[file.Path] = struct{}{}
		previous = file.Path
		if total > int64(^uint64(0)>>1)-file.Bytes {
			return errors.New("inventory byte count overflows")
		}
		total += file.Bytes
	}
	if total != manifest.Inventory.TotalBytes {
		return errors.New("inventory byte count does not match files")
	}
	return nil
}

func validateContractLinks(c pinContract, trees map[string]string) error {
	if c.Path != contractPath ||
		c.Capability != "contract: durable-index/v0" ||
		c.EntrySchema != IndexPublicationSchemaPath ||
		c.CommonSchema != IndexCommonSchemaPath ||
		len(c.ObjectSchemas) != 4 ||
		c.ObjectSchemas["index_cutover"] != IndexCutoverSchemaPath ||
		c.ObjectSchemas["index_metadata"] != IndexMetadataSchemaPath ||
		c.ObjectSchemas["index_plan"] != IndexPlanSchemaPath ||
		c.ObjectSchemas["index_row"] != IndexRowSchemaPath {
		return errors.New("invalid durable-index contract links")
	}
	if len(c.ComposedContracts) != 3 {
		return errors.New("unexpected composed contract count")
	}
	ca, okCA := c.ComposedContracts["coverage-attestation/v0"]
	da, okDA := c.ComposedContracts["data-artifact/v0"]
	ss, okSS := c.ComposedContracts["segmented-snapshot/v0"]
	if !okCA || ca.Path != "schemas/coverage-attestation/v0/contract.json" || !slices.Equal(ca.Schemas, []string{CoverageAttestationSchemaPath}) ||
		!okDA || da.Path != "schemas/data-artifact/v0/contract.json" || !slices.Equal(da.Schemas, []string{ArtifactDescriptorSchemaPath}) ||
		!okSS || ss.Path != "schemas/segmented-snapshot/v0/contract.json" || !slices.Equal(ss.Schemas, []string{
		SnapshotCommonSchemaPath,
		SnapshotManifestSchemaPath,
		SnapshotProfileSchemaPath,
		SnapshotPublicationSchemaPath,
		VerificationResultSchemaPath,
	}) {
		return errors.New("invalid composed contract links")
	}
	expectedTrees := map[string]string{
		"schemas/coverage-attestation/v0": "525b0cfd235b0035dc7c6bb589081c396e8ef2df",
		"schemas/data-artifact/v0":        "6b9a0422bd64844964a0ff05f4f9efa944003338",
		"schemas/durable-index/v0":        "11913b981567b633a7040c3f82a41bded221dae9",
		"schemas/segmented-snapshot/v0":   "ef95ffb12f5288224dffd5eecc71885ff3089e97",
	}
	if len(trees) != len(expectedTrees) {
		return errors.New("unexpected schema tree pin count")
	}
	for k, want := range expectedTrees {
		if trees[k] != want {
			return fmt.Errorf("unexpected schema tree pin for %q", k)
		}
	}
	return nil
}

func verifyAssets(fsys fs.FS, root string, manifest pinManifest) error {
	expected := make(map[string]pinFile, len(manifest.Inventory.Files))
	for _, file := range manifest.Inventory.Files {
		expected[file.Path] = file
	}
	actual := make(map[string]struct{}, len(expected))
	h := sha256.New()
	var total int64
	for _, file := range manifest.Inventory.Files {
		assetPath := path.Join(root, file.Path)
		info, err := fs.Stat(fsys, assetPath)
		if err != nil {
			return fmt.Errorf("durable-index asset closure missing %q: %w", file.Path, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("durable-index asset closure has non-regular %q", file.Path)
		}
		data, err := fs.ReadFile(fsys, assetPath)
		if err != nil {
			return fmt.Errorf("durable-index asset closure reads %q: %w", file.Path, err)
		}
		if int64(len(data)) != file.Bytes {
			return fmt.Errorf("durable-index asset closure size mismatch for %q", file.Path)
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != file.SHA256 {
			return fmt.Errorf("durable-index asset closure digest mismatch for %q", file.Path)
		}
		_, _ = io.WriteString(h, file.Path)
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(data)
		total += int64(len(data))
		actual[file.Path] = struct{}{}
	}
	if total != manifest.Inventory.TotalBytes || hex.EncodeToString(h.Sum(nil)) != manifest.Inventory.SHA256 {
		return errors.New("durable-index asset closure inventory mismatch")
	}
	if err := fs.WalkDir(fsys, root, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if name == root || entry.IsDir() {
			return nil
		}
		rel := name
		if root != "." {
			rel = strings.TrimPrefix(name, root+"/")
			if rel == name || rel == "" {
				return fmt.Errorf("durable-index asset closure has invalid path %q", name)
			}
		}
		if _, ok := expected[rel]; !ok {
			return fmt.Errorf("durable-index asset closure has unexpected %q", rel)
		}
		actual[rel] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	if len(actual) != len(expected) {
		return errors.New("durable-index asset closure has missing files")
	}
	return nil
}

func verifyFilesystem(root string, manifest pinManifest) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", errors.New("durable-index asset closure root is empty")
	}
	if err := rejectRootSymlinkComponents(root); err != nil {
		return "", err
	}
	cleanRoot := filepath.Clean(root)
	rootInfo, err := os.Lstat(cleanRoot)
	if err != nil {
		return "", fmt.Errorf("durable-index asset closure root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return "", errors.New("durable-index asset closure root is not a directory")
	}
	expected := make(map[string]pinFile, len(manifest.Inventory.Files))
	for _, file := range manifest.Inventory.Files {
		expected[file.Path] = file
	}
	count := 0
	if err := filepath.WalkDir(cleanRoot, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if name == cleanRoot {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("durable-index asset closure has non-regular %q", name)
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("durable-index asset closure has non-regular %q", name)
		}
		rel, relErr := filepath.Rel(cleanRoot, name)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if _, ok := expected[rel]; !ok {
			return fmt.Errorf("durable-index asset closure has unexpected %q", rel)
		}
		count++
		return nil
	}); err != nil {
		return "", err
	}
	for _, file := range manifest.Inventory.Files {
		assetPath := filepath.Join(cleanRoot, filepath.FromSlash(file.Path))
		info, lstatErr := os.Lstat(assetPath)
		if lstatErr != nil {
			return "", fmt.Errorf("durable-index asset closure missing %q: %w", file.Path, lstatErr)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return "", fmt.Errorf("durable-index asset closure has non-regular %q", file.Path)
		}
	}
	if count != len(expected) {
		return "", errors.New("durable-index asset closure has missing files")
	}
	if err := verifyAssets(os.DirFS(cleanRoot), ".", manifest); err != nil {
		return "", err
	}
	return cleanRoot, nil
}

func rejectRootSymlinkComponents(root string) error {
	cleanRoot := filepath.Clean(root)
	vol := filepath.VolumeName(root)
	steps := walkVolumeComponentPrefixes(root, vol, os.PathSeparator)
	for _, step := range steps {
		if err := lstatRootDirComponent(step.candidate, cleanRoot, step.disallowAncestorSymlink); err != nil {
			return err
		}
	}
	if err := lstatRootDirComponent(cleanRoot, cleanRoot, true); err != nil {
		return err
	}
	rawInfo, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("durable-index asset closure root: %w", err)
	}
	cleanInfo, err := os.Lstat(cleanRoot)
	if err != nil {
		return fmt.Errorf("durable-index asset closure root: %w", err)
	}
	if !os.SameFile(rawInfo, cleanInfo) {
		return errors.New("durable-index asset closure root is not a directory")
	}
	return nil
}

type volumeComponentStep struct {
	candidate               string
	disallowAncestorSymlink bool
}

func walkVolumeComponentPrefixes(root, vol string, sep rune) []volumeComponentStep {
	baseRoot, parts := splitVolumeComponents(root, vol, sep)
	cur := baseRoot
	steps := make([]volumeComponentStep, 0, len(parts))
	for i, part := range parts {
		hasLaterDotDot := hasParentTraversalComponent(parts[i+1:])
		if part == "" || part == "." {
			if cur != "" && cur != baseRoot {
				steps = append(steps, volumeComponentStep{
					candidate:               cur,
					disallowAncestorSymlink: hasLaterDotDot,
				})
			}
			continue
		}
		if cur == "" || cur == vol || strings.HasSuffix(cur, string(sep)) {
			cur += part
		} else {
			cur += string(sep) + part
		}
		steps = append(steps, volumeComponentStep{
			candidate:               cur,
			disallowAncestorSymlink: hasLaterDotDot || part == "..",
		})
	}
	return steps
}

func splitVolumeComponents(root, vol string, sep rune) (string, []string) {
	rest := root
	if vol != "" && strings.HasPrefix(root, vol) {
		rest = root[len(vol):]
	}
	normalized := rest
	if sep != '/' {
		normalized = strings.ReplaceAll(normalized, string(sep), "/")
	}
	baseRoot := ""
	if strings.HasPrefix(normalized, "/") {
		baseRoot = vol + string(sep)
	} else if vol != "" {
		baseRoot = vol
	}
	return baseRoot, strings.Split(normalized, "/")
}

func hasParentTraversalComponent(parts []string) bool {
	for _, part := range parts {
		if part == ".." {
			return true
		}
	}
	return false
}

func lstatRootDirComponent(candidate, cleanRoot string, disallowAncestorSymlink bool) error {
	info, err := os.Lstat(candidate)
	if err != nil {
		return fmt.Errorf("durable-index asset closure root: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		cleanCandidate := filepath.Clean(candidate)
		if !disallowAncestorSymlink && isPlatformRootSymlink(cleanCandidate) && isStrictPathPrefix(cleanCandidate, cleanRoot) {
			targetInfo, statErr := os.Stat(candidate)
			if statErr == nil && targetInfo.IsDir() {
				return nil
			}
		}
		return errors.New("durable-index asset closure root is not a directory")
	}
	if !info.IsDir() {
		return errors.New("durable-index asset closure root is not a directory")
	}
	return nil
}

func isPlatformRootSymlink(cleanPath string) bool {
	switch filepath.ToSlash(cleanPath) {
	case "/var", "/tmp", "/etc", "/private/var", "/private/tmp", "/private/etc":
		return true
	default:
		return false
	}
}

func isStrictPathPrefix(ancestor, target string) bool {
	if ancestor == "" || ancestor == target {
		return false
	}
	prefix := ancestor
	if !strings.HasSuffix(prefix, string(os.PathSeparator)) {
		prefix += string(os.PathSeparator)
	}
	return strings.HasPrefix(target, prefix)
}

func compileAllSchemas(fsys fs.FS, root string) error {
	for _, schemaPath := range allNormativeSchemas {
		if _, err := compileSchema(fsys, root, schemaPath); err != nil {
			return err
		}
	}
	return nil
}

func compileSchema(fsys fs.FS, root, wanted string) (*jsonschema.Schema, error) {
	if !slices.Contains(allNormativeSchemas, wanted) {
		return nil, fmt.Errorf("unknown durable-index schema path %q", wanted)
	}
	compiler := jsonschema.NewCompiler()
	compiler.LoadURL = func(url string) (io.ReadCloser, error) {
		return nil, fmt.Errorf("offline durable-index schema compiler refuses %q", url)
	}
	var wantedID string
	for _, schemaPath := range allNormativeSchemas {
		data, err := fs.ReadFile(fsys, path.Join(root, schemaPath))
		if err != nil {
			return nil, fmt.Errorf("durable-index schema %q: %w", schemaPath, err)
		}
		id, err := schemaID(data)
		if err != nil {
			return nil, fmt.Errorf("durable-index schema %q: %w", schemaPath, err)
		}
		if err := compiler.AddResource(id, bytes.NewReader(data)); err != nil {
			return nil, fmt.Errorf("durable-index schema %q: %w", schemaPath, err)
		}
		if schemaPath == wanted {
			wantedID = id
		}
	}
	schema, err := compiler.Compile(wantedID)
	if err != nil {
		return nil, fmt.Errorf("durable-index schema %q does not compile offline: %w", wanted, err)
	}
	return schema, nil
}

func schemaID(data []byte) (string, error) {
	var header struct {
		ID string `json:"$id"`
	}
	if err := decodeFlexibleJSON(data, &header); err != nil {
		return "", err
	}
	if !strings.HasPrefix(header.ID, "contract:") {
		return "", errors.New("missing contract URI")
	}
	return header.ID, nil
}

func decodeValidatedJSON(data []byte) (any, error) {
	if len(data) == 0 || len(data) > maxValidatedJSONBytes {
		return nil, errors.New("size is outside bounds")
	}
	if !utf8.Valid(data) {
		return nil, errors.New("invalid UTF-8")
	}
	if err := rejectLoneSurrogateEscapes(data); err != nil {
		return nil, err
	}
	if err := checkSingleJSONValue(data, maxJSONContainerDepth); err != nil {
		return nil, err
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeFlexibleJSON(data []byte, out any) error {
	if len(data) == 0 || !utf8.Valid(data) {
		return errors.New("invalid JSON input")
	}
	if err := rejectLoneSurrogateEscapes(data); err != nil {
		return err
	}
	if err := checkSingleJSONValue(data, maxJSONContainerDepth); err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func decodeExactJSON(data []byte, out any) error {
	if len(data) == 0 || !utf8.Valid(data) {
		return errors.New("invalid JSON input")
	}
	if err := rejectLoneSurrogateEscapes(data); err != nil {
		return err
	}
	if err := checkSingleJSONValue(data, maxJSONContainerDepth); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(out)
}

func checkSingleJSONValue(data []byte, maxDepth int) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder, 0, maxDepth); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, depth, maxDepth int) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if num, ok := token.(json.Number); ok {
		return validateFiniteJSONNumber(num)
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if depth >= maxDepth {
		return errors.New("JSON nesting exceeds limit")
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder, depth+1, maxDepth); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return errors.New("unterminated object")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder, depth+1, maxDepth); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return errors.New("unterminated array")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}

func validateFiniteJSONNumber(num json.Number) error {
	raw := num.String()
	if _, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
		return errors.New("non-finite JSON number")
	}
	return nil
}

// rejectLoneSurrogateEscapes walks JSON string literals and refuses unpaired
// UTF-16 surrogate escapes (\uD800..\uDBFF without an immediately following
// \uDC00..\uDFFF, or an unpaired \uDC00..\uDFFF) before encoding/json can
// silently coerce them to U+FFFD.
func rejectLoneSurrogateEscapes(data []byte) error {
	inString := false
	for i := 0; i < len(data); {
		b := data[i]
		if !inString {
			if b == '"' {
				inString = true
			}
			i++
			continue
		}
		if b == '"' {
			inString = false
			i++
			continue
		}
		if b != '\\' {
			i++
			continue
		}
		if i+1 >= len(data) {
			return errors.New("unterminated JSON string escape")
		}
		if data[i+1] != 'u' {
			i += 2
			continue
		}
		if i+6 > len(data) {
			return errors.New("truncated Unicode escape")
		}
		u1, ok := parseHexUint16(data[i+2 : i+6])
		if !ok {
			return errors.New("invalid Unicode escape")
		}
		if u1 >= 0xDC00 && u1 <= 0xDFFF {
			return errors.New("lone low Unicode surrogate escape")
		}
		if u1 >= 0xD800 && u1 <= 0xDBFF {
			if i+12 > len(data) || data[i+6] != '\\' || data[i+7] != 'u' {
				return errors.New("lone high Unicode surrogate escape")
			}
			u2, ok2 := parseHexUint16(data[i+8 : i+12])
			if !ok2 || u2 < 0xDC00 || u2 > 0xDFFF {
				return errors.New("lone high Unicode surrogate escape")
			}
			i += 12
			continue
		}
		i += 6
	}
	return nil
}

func parseHexUint16(raw []byte) (uint16, bool) {
	if len(raw) != 4 {
		return 0, false
	}
	var out uint16
	for _, b := range raw {
		out <<= 4
		switch {
		case b >= '0' && b <= '9':
			out |= uint16(b - '0')
		case b >= 'a' && b <= 'f':
			out |= uint16(b - 'a' + 10)
		case b >= 'A' && b <= 'F':
			out |= uint16(b - 'A' + 10)
		default:
			return 0, false
		}
	}
	return out, true
}

func validAssetPath(value string) bool {
	return value != "" &&
		path.Clean(value) == value &&
		!strings.HasPrefix(value, "/") &&
		!strings.HasPrefix(value, "../") &&
		value != ".." &&
		value != "." &&
		!strings.Contains(value, "\\")
}

func isLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}
