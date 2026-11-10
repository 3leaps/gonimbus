package durableindex

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"path"
	"reflect"
	"strings"
	"testing"

	durableindexv0 "github.com/3leaps/gonimbus/internal/contracts/durableindex/v0"
)

type corpusInventory struct {
	Shape []struct {
		File   string `json:"file"`
		Schema string `json:"schema"`
		Valid  bool   `json:"valid"`
	} `json:"shape"`
	Plans       []string `json:"plans"`
	Transitions []string `json:"transitions"`
	Scenarios   []string `json:"scenarios"`
}

func loadUpstreamCasesInventory(t *testing.T) (fs.FS, corpusInventory) {
	t.Helper()
	upstreamFS, err := durableindexv0.FS()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := fs.ReadFile(upstreamFS, "schemas/durable-index/v0/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var inv corpusInventory
	if err := json.Unmarshal(raw, &inv); err != nil {
		t.Fatal(err)
	}
	return upstreamFS, inv
}

func TestUpstreamShapeVectors(t *testing.T) {
	upstreamFS, inv := loadUpstreamCasesInventory(t)
	if len(inv.Shape) == 0 {
		t.Fatal("cases.json has no shape vectors")
	}
	for _, sc := range inv.Shape {
		t.Run(sc.File, func(t *testing.T) {
			raw, err := fs.ReadFile(upstreamFS, path.Join("schemas/durable-index/v0", sc.File))
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := StrictParseContractJSON(raw)
			if err != nil {
				if sc.Valid {
					t.Fatalf("StrictParseContractJSON(%q) unexpected error = %v", sc.File, err)
				}
				return
			}
			obj, ok := asMap(parsed)
			if !ok {
				if sc.Valid {
					t.Fatalf("expected object in %q", sc.File)
				}
				return
			}
			schemaRel := path.Clean(path.Join("schemas/durable-index/v0", sc.Schema))
			err = validateShapeSchema(obj, schemaRel)
			if sc.Valid && err != nil {
				t.Fatalf("validateShapeSchema(%q, %q) unexpected error = %v", sc.File, schemaRel, err)
			}
			if !sc.Valid && err == nil {
				t.Fatalf("validateShapeSchema(%q, %q) succeeded, want rejection", sc.File, schemaRel)
			}
		})
	}
}

func TestUpstreamPlanVectors(t *testing.T) {
	upstreamFS, inv := loadUpstreamCasesInventory(t)
	if len(inv.Plans) == 0 {
		t.Fatal("cases.json has no plan vectors")
	}
	for _, relFile := range inv.Plans {
		t.Run(relFile, func(t *testing.T) {
			raw, err := fs.ReadFile(upstreamFS, path.Join("schemas/durable-index/v0", relFile))
			if err != nil {
				t.Fatal(err)
			}
			var vec struct {
				SetID       string  `json:"set_id"`
				Input       []Scope `json:"input"`
				Canonical   []Scope `json:"canonical"`
				BytesHex    string  `json:"bytes_hex"`
				SHA256      string  `json:"sha256"`
				Comparisons []struct {
					Next []Scope `json:"next"`
					Loss bool    `json:"loss"`
				} `json:"comparisons"`
				Membership []struct {
					Scope     Scope  `json:"scope"`
					Namespace string `json:"namespace"`
					Key       string `json:"key"`
					Contains  bool   `json:"contains"`
				} `json:"membership"`
			}
			if err := json.Unmarshal(raw, &vec); err != nil {
				t.Fatal(err)
			}
			canon, err := CanonicalizeScopes(vec.Input)
			if err != nil {
				t.Fatalf("CanonicalizeScopes error = %v", err)
			}
			if !ScopesEqual(canon, vec.Canonical) {
				t.Fatalf("CanonicalizeScopes = %+v, want %+v", canon, vec.Canonical)
			}
			framed, err := PlanFramedBytes(vec.SetID, canon)
			if err != nil {
				t.Fatalf("PlanFramedBytes error = %v", err)
			}
			if gotHex := hex.EncodeToString(framed); gotHex != vec.BytesHex {
				t.Fatalf("PlanFramedBytes hex = %s, want %s", gotHex, vec.BytesHex)
			}
			digest, err := PlanDigest(vec.SetID, canon)
			if err != nil {
				t.Fatalf("PlanDigest error = %v", err)
			}
			if digest != vec.SHA256 {
				t.Fatalf("PlanDigest = %s, want %s", digest, vec.SHA256)
			}
			for i, cmp := range vec.Comparisons {
				lost, err := HasScopeLoss(canon, cmp.Next)
				if err != nil {
					t.Fatalf("comparison[%d] HasScopeLoss error = %v", i, err)
				}
				if lost != cmp.Loss {
					t.Fatalf("comparison[%d] HasScopeLoss = %v, want %v", i, lost, cmp.Loss)
				}
			}
			for i, mem := range vec.Membership {
				got := ScopeContains(mem.Scope, mem.Namespace, mem.Key)
				if got != mem.Contains {
					t.Fatalf("membership[%d] ScopeContains = %v, want %v", i, got, mem.Contains)
				}
			}
		})
	}
}

func TestUpstreamTransitionAndScenarioVectors(t *testing.T) {
	_, inv := loadUpstreamCasesInventory(t)
	if len(inv.Transitions) == 0 {
		t.Fatal("cases.json has no transition vectors")
	}
	if len(inv.Scenarios) == 0 {
		t.Fatal("cases.json has no scenario vectors")
	}
	all := make([]string, 0, len(inv.Transitions)+len(inv.Scenarios))
	all = append(all, inv.Transitions...)
	all = append(all, inv.Scenarios...)
	for _, relFile := range all {
		t.Run(relFile, func(t *testing.T) {
			caseMap, err := LoadCorpusCase(relFile)
			if err != nil {
				t.Fatalf("LoadCorpusCase(%q) error = %v", relFile, err)
			}
			got, err := EvaluateCorpusCase(caseMap)
			if err != nil {
				t.Fatalf("EvaluateCorpusCase(%q) error = %v", relFile, err)
			}
			want, _ := asMap(caseMap["expected"])
			if !reflect.DeepEqual(got, want) {
				gotJSON, _ := json.MarshalIndent(got, "", "  ")
				wantJSON, _ := json.MarshalIndent(want, "", "  ")
				t.Fatalf("EvaluateCorpusCase(%q) mismatch:\ngot:\n%s\nwant:\n%s", relFile, gotJSON, wantJSON)
			}
		})
	}
}

func TestBuildIndexPlanAndCutoverReceiptHelpers(t *testing.T) {
	plan, raw, digest, err := BuildIndexPlan("set-example", []Scope{
		{Namespace: "objects", Prefix: "beta/"},
		{Namespace: "objects", Prefix: "alpha/deep/"},
		{Namespace: "objects", Prefix: "alpha/"},
	})
	if err != nil {
		t.Fatalf("BuildIndexPlan error = %v", err)
	}
	if len(plan.Scopes) != 2 || plan.Scopes[0].Prefix != "alpha/" || plan.Scopes[1].Prefix != "beta/" {
		t.Fatalf("unexpected canonical scopes: %+v", plan.Scopes)
	}
	if digest != "78df561c81e1c50a5744a7e91d5172320f772c6fce93166cca1cfa5e8c3df4e5" {
		t.Fatalf("digest = %s", digest)
	}
	if err := durableindexv0.ValidateJSON(durableindexv0.IndexPlanSchemaPath, raw); err != nil {
		t.Fatalf("ValidateJSON(BuildIndexPlan) error = %v", err)
	}

	oldEp := ParentBinding{
		Selection: Selection{NamespaceID: "set-old", SnapshotID: "snap-1", ArtifactID: "artifact:data"},
		Publication: Binding{
			ID:         "pub-old",
			Ref:        "artifact:old",
			SHA256:     strings.Repeat("a", 64),
			ByteLength: 100,
		},
	}
	newEp := ParentBinding{
		Selection: Selection{NamespaceID: "set-new", SnapshotID: "snap-2", ArtifactID: "artifact:data"},
		Publication: Binding{
			ID:         "pub-new",
			Ref:        "artifact:new",
			SHA256:     strings.Repeat("b", 64),
			ByteLength: 200,
		},
	}
	rec, recRaw, err := BuildCutoverReceipt(oldEp, newEp, "operator-1", "2026-10-18T02:00:00.000000000Z")
	if err != nil {
		t.Fatalf("BuildCutoverReceipt error = %v", err)
	}
	if rec.Rollback != oldEp.Publication {
		t.Fatalf("Rollback = %+v, want %+v", rec.Rollback, oldEp.Publication)
	}
	if len(recRaw) == 0 {
		t.Fatal("empty cutover receipt bytes")
	}
}

func TestSecurityAndSchemaHardeningControls(t *testing.T) {
	// D3: Invalid namespace identifiers in CanonicalizeScopes / BuildIndexPlan.
	for _, badNS := range []string{"", "non ascii é", "bad/slash"} {
		_, _, _, err := BuildIndexPlan("set-example", []Scope{{Namespace: badNS, Prefix: "a/"}})
		if reason, ok := RefusalReason(err); !ok || reason != "invalid_shape" {
			t.Fatalf("BuildIndexPlan(namespace=%q) err = %v, want invalid_shape", badNS, err)
		}
	}

	// D2: Field catalog validation rejects invalid sensitivity and unexpected properties.
	baseGraph, err := LoadCorpusCase("vectors/graphs/baseline.json")
	if err != nil {
		t.Fatal(err)
	}
	inputMap, _ := asMap(baseGraph["input"])
	storeMap, _ := asMap(inputMap["store"])
	var catObj map[string]any
	if err := json.Unmarshal([]byte(storeMap["artifact:catalog"].(string)), &catObj); err != nil {
		t.Fatal(err)
	}
	fields, _ := asSlice(catObj["fields"])
	f0, _ := asMap(fields[0])
	f0["sensitivity"] = "not-a-valid-sensitivity"
	f0["unexpected_prop"] = true
	if err := validateFieldCatalogShape(catObj); err == nil {
		t.Fatal("validateFieldCatalogShape accepted invalid sensitivity and unexpected field property")
	}

	// S1: Multibyte UTF-8 byte ceilings on revision, storage_class, and enrichment value.
	threeByteChar := "é"                  // 2 bytes in UTF-8; "€" is 3 bytes in UTF-8
	euro4098 := strings.Repeat("€", 1366) // 4098 bytes (1366 code points <= 4096 maxLength)
	baseRow := LogicalRow{
		Namespace:   "objects",
		Key:         "alpha/one",
		Revision:    &Revision{Kind: "opaque-etag", Value: "r1"},
		SizeBytes:   12,
		FirstSeen:   Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
		LastSeen:    Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
		LastChanged: Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
		Enrichments: []EnrichmentAttribute{},
	}
	_ = threeByteChar
	rowBadRev := baseRow
	rowBadRev.Revision = &Revision{Kind: "opaque-etag", Value: euro4098}
	if reason, ok := RefusalReason(ValidateRows([]LogicalRow{rowBadRev}, "")); !ok || reason != "capacity" {
		t.Fatalf("ValidateRows(multibyte revision > 4096 bytes) reason = %q, want capacity", reason)
	}
	rowBadSC := baseRow
	scVal := euro4098
	rowBadSC.StorageClass = &scVal
	if reason, ok := RefusalReason(ValidateRows([]LogicalRow{rowBadSC}, "")); !ok || reason != "capacity" {
		t.Fatalf("ValidateRows(multibyte storage_class > 4096 bytes) reason = %q, want capacity", reason)
	}
	rowBadEnrich := baseRow
	enrichVal := strings.Repeat("€", 21846) // 65538 bytes (21846 code points <= 65536 maxLength)
	rowBadEnrich.Enrichments = []EnrichmentAttribute{{
		Name:            "attr",
		Value:           &enrichVal,
		Operation:       "head-1",
		ObservedAt:      "2026-10-18T00:00:00.000000000Z",
		AgainstRevision: Revision{Kind: "opaque-etag", Value: "r1"},
	}}
	if reason, ok := RefusalReason(ValidateRows([]LogicalRow{rowBadEnrich}, "")); !ok || reason != "capacity" {
		t.Fatalf("ValidateRows(multibyte enrichment > 65536 bytes) reason = %q, want capacity", reason)
	}

	// S2: Linear canonicalization and binary-search HasScopeLoss on 65,536 disjoint scopes.
	manyScopes := make([]Scope, MaxEffectiveScopes)
	for i := range manyScopes {
		manyScopes[i] = Scope{
			Namespace: "objects",
			Prefix:    hex.EncodeToString([]byte{byte(i >> 8), byte(i)}) + "/",
		}
	}
	canonMany, err := CanonicalizeScopes(manyScopes)
	if err != nil || len(canonMany) != MaxEffectiveScopes {
		t.Fatalf("CanonicalizeScopes(65536) len=%d err=%v", len(canonMany), err)
	}
	lost, err := HasScopeLoss(canonMany, canonMany)
	if err != nil || lost {
		t.Fatalf("HasScopeLoss(65536) lost=%v err=%v", lost, err)
	}

	// S4: Standalone FoldTransition rejects generation overflow and out-of-range generations.
	obsAt := "2026-10-18T00:00:00.000000000Z"
	pubSHA := strings.Repeat("a", 64)
	_, err = FoldTransition(TransitionInput{
		Mode:                 ModeObserve,
		SetID:                "set-example",
		SnapshotID:           "snapshot-1",
		Generation:           MaxExactInteger + 1,
		ObservationAt:        &obsAt,
		DiscoveryComplete:    true,
		AdmittedParentSHA256: &pubSHA,
		Parent: &TransitionParentInput{
			PublicationSHA256: pubSHA,
			SetID:             "set-example",
			SnapshotID:        "snapshot-0",
			Generation:        MaxExactInteger,
			Scopes:            []Scope{{Namespace: "objects", Prefix: "a/"}},
		},
		Scopes: []Scope{{Namespace: "objects", Prefix: "a/"}},
	})
	if _, ok := RefusalReason(err); !ok {
		t.Fatalf("FoldTransition(MaxExactInteger -> MaxExactInteger+1) expected refusal, got %v", err)
	}

	// S3: Profile max_metadata_bytes ceiling enforced immediately after profile load before parsing subsequent oversized metadata.
	s3Graph, err := LoadCorpusCase("vectors/graphs/baseline.json")
	if err != nil {
		t.Fatal(err)
	}
	s3In, _ := asMap(s3Graph["input"])
	s3Store, _ := asMap(s3In["store"])
	// Make artifact:catalog oversized (> 1 MiB profile max_metadata_bytes) AND malformed JSON.
	// If profile max_metadata_bytes is enforced before parsing, VerifyLogicalGraph returns "capacity" instead of "invalid_json".
	oversizedMalformedCatalog := strings.Repeat(" ", 2*1024*1024) + "{malformed"
	catSum := sha256.Sum256([]byte(oversizedMalformedCatalog))
	s3Store["artifact:catalog"] = oversizedMalformedCatalog
	var manObj map[string]any
	if err := json.Unmarshal([]byte(s3Store["artifact:g0-manifest"].(string)), &manObj); err != nil {
		t.Fatal(err)
	}
	manObj["field_catalog"] = map[string]any{
		"id":          "artifact:index-catalog",
		"ref":         "artifact:catalog",
		"sha256":      hex.EncodeToString(catSum[:]),
		"byte_length": int64(len(oversizedMalformedCatalog)),
	}
	manBytes, _ := json.Marshal(manObj)
	manSum := sha256.Sum256(manBytes)
	s3Store["artifact:g0-manifest"] = string(manBytes)
	var pubObj map[string]any
	if err := json.Unmarshal([]byte(s3Store["artifact:g0-publication"].(string)), &pubObj); err != nil {
		t.Fatal(err)
	}
	pubObj["manifest"] = map[string]any{
		"id":          "snapshot-0",
		"ref":         "artifact:g0-manifest",
		"sha256":      hex.EncodeToString(manSum[:]),
		"byte_length": int64(len(manBytes)),
	}
	pubBytes, _ := json.Marshal(pubObj)
	pubSum := sha256.Sum256(pubBytes)
	s3Store["artifact:g0-publication"] = string(pubBytes)
	s3In["publication"] = map[string]any{
		"id":          "publication-0",
		"ref":         "artifact:g0-publication",
		"sha256":      hex.EncodeToString(pubSum[:]),
		"byte_length": int64(len(pubBytes)),
	}
	_, s3Err := VerifyLogicalGraph(s3In)
	if reason, ok := RefusalReason(s3Err); !ok || reason != "capacity" {
		t.Fatalf("VerifyLogicalGraph(oversized malformed catalog) reason = %q (err=%v), want capacity before JSON parse", reason, s3Err)
	}
}
