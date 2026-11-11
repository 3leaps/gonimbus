package durableindex

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
	"reflect"
	"strings"

	durableindexv0 "github.com/3leaps/gonimbus/internal/contracts/durableindex/v0"
)

// VerifyBoundTransition validates a bound_transitions vector or reject case (I1-I6).
func VerifyBoundTransition(caseMap map[string]any) (TransitionOutcome, error) {
	evMap, ok1 := asMap(caseMap["evidence"])
	trMap, ok2 := asMap(caseMap["transition"])
	if !ok1 || !ok2 {
		return TransitionOutcome{}, NewRefusal("invalid_shape", "evidence and transition must be objects")
	}
	authority, err := VerifyBindings(evMap, nil)
	if err != nil {
		return TransitionOutcome{}, err
	}
	metaMap, _ := asMap(evMap["metadata"])
	store, err := parseStoreMap(evMap["store"])
	if err != nil {
		return TransitionOutcome{}, err
	}
	planPin, err := remountJSON[Binding](metaMap["plan"])
	if err != nil {
		return TransitionOutcome{}, err
	}
	planObj, _, err := resolvePinnedStoreObject(planPin, store)
	if err != nil {
		return TransitionOutcome{}, err
	}
	sel, err := remountJSON[Selection](metaMap["selection"])
	if err != nil {
		return TransitionOutcome{}, err
	}
	trSetID, _ := asString(trMap["set_id"])
	trSnapID, _ := asString(trMap["snapshot_id"])
	trMode, _ := asString(trMap["mode"])
	metaMode, _ := asString(metaMap["mode"])
	trGen, _ := asInt64Exact(trMap["generation"])
	metaGen, _ := asInt64Exact(metaMap["generation"])

	if trSetID != sel.NamespaceID ||
		trSnapID != sel.SnapshotID ||
		trMode != metaMode ||
		trGen != metaGen ||
		!reflect.DeepEqual(trMap["observation_at"], metaMap["observation_at"]) ||
		!reflect.DeepEqual(trMap["scopes"], planObj["scopes"]) ||
		!reflect.DeepEqual(trMap["observations"], evMap["observations"]) {
		return TransitionOutcome{}, NewRefusal("metadata_transition", "transition identity, mode, plan, or observations disagree with metadata")
	}

	if trMap["parent"] == nil {
		if metaMap["parent"] != nil {
			return TransitionOutcome{}, NewRefusal("metadata_transition", "metadata parent disagrees with transition parent")
		}
	} else {
		if metaMap["parent"] == nil {
			return TransitionOutcome{}, NewRefusal("metadata_transition", "metadata parent is null for non-baseline transition")
		}
		metaParent, err := remountJSON[ParentBinding](metaMap["parent"])
		if err != nil {
			return TransitionOutcome{}, err
		}
		trParentMap, _ := asMap(trMap["parent"])
		trPSetID, _ := asString(trParentMap["set_id"])
		trPSnapID, _ := asString(trParentMap["snapshot_id"])
		trPPubSHA, _ := asString(trParentMap["publication_sha256"])
		if metaParent.Selection.NamespaceID != trPSetID ||
			metaParent.Selection.SnapshotID != trPSnapID ||
			metaParent.Publication.SHA256 != trPPubSHA {
			return TransitionOutcome{}, NewRefusal("metadata_transition", "metadata parent disagrees with transition parent")
		}
	}

	var expectedAck any
	if ackRaw := metaMap["loss_acknowledgment"]; ackRaw != nil {
		metaAck, err := remountJSON[LossAcknowledgment](ackRaw)
		if err != nil {
			return TransitionOutcome{}, err
		}
		metaParent, err := remountJSON[ParentBinding](metaMap["parent"])
		if err != nil || metaAck.Parent != metaParent {
			return TransitionOutcome{}, NewRefusal("metadata_transition", "loss_acknowledgment parent disagrees with metadata parent")
		}
		expectedAck = map[string]any{
			"parent_sha256":   metaAck.Parent.Publication.SHA256,
			"plan_digest":     metaAck.PlanSHA256,
			"acknowledged_by": metaAck.AcknowledgedBy,
			"acknowledged_at": metaAck.AcknowledgedAt,
		}
	}
	if !reflect.DeepEqual(trMap["acknowledgment"], expectedAck) {
		return TransitionOutcome{}, NewRefusal("metadata_transition", "transition acknowledgment disagrees with metadata loss_acknowledgment")
	}

	trInput, err := parseTransitionMap(trMap)
	if err != nil {
		return TransitionOutcome{}, err
	}
	trInput.AbsenceAuthority = authority
	outcome, err := FoldTransition(trInput)
	if err != nil {
		return TransitionOutcome{}, err
	}
	metaAdm, _ := asString(metaMap["admission"])
	metaRetained, _ := asInt64Exact(metaMap["retained_unobserved_rows"])
	if outcome.Admission != metaAdm || outcome.RetainedUnobservedRows != metaRetained {
		return TransitionOutcome{}, NewRefusal("metadata_transition", "metadata admission or retained_unobserved_rows disagrees with transition")
	}
	return outcome, nil
}

// VerifyCutover validates an explicit baseline cutover receipt (index-cutover.schema.json).
func VerifyCutover(caseMap map[string]any) (map[string]any, error) {
	recRaw, hasRec := caseMap["record"]
	if !hasRec || recRaw == nil {
		return nil, NewRefusal("cutover_required", "cutover assertion requires a receipt")
	}
	if err := validateShapeSchema(recRaw, durableindexv0.IndexCutoverSchemaPath); err != nil {
		return nil, err
	}
	rec, err := remountJSON[IndexCutoverReceipt](recRaw)
	if err != nil {
		return nil, err
	}
	if err := ValidateInstant(rec.AcceptedAt); err != nil {
		return nil, err
	}
	actorsSlice, _ := asSlice(caseMap["authorized_actors"])
	actorAuthorized := false
	for _, aRaw := range actorsSlice {
		if aStr, ok := asString(aRaw); ok && aStr == rec.AuthorizedBy {
			actorAuthorized = true
			break
		}
	}
	if !actorAuthorized {
		return nil, NewRefusal("cutover_actor", "cutover actor is absent from authorized_actors")
	}
	store, err := parseStoreMap(caseMap["store"])
	if err != nil {
		return nil, err
	}
	var newPubObj map[string]any
	for idx, ep := range []ParentBinding{rec.Old, rec.New} {
		pubObj, _, err := resolvePinnedStoreObject(ep.Publication, store)
		if err != nil {
			return nil, err
		}
		if err := validateShapeSchema(pubObj, durableindexv0.IndexPublicationSchemaPath); err != nil {
			return nil, err
		}
		pubSel, err := remountJSON[Selection](pubObj["selection"])
		if err != nil {
			return nil, err
		}
		pubID, _ := asString(pubObj["publication_id"])
		if pubSel != ep.Selection || pubID != ep.Publication.ID {
			return nil, NewRefusal("cutover_selection", "endpoint selection or publication_id mismatch")
		}
		if idx == 1 {
			newPubObj = pubObj
		}
	}
	if rec.Old.Publication == rec.New.Publication || rec.Old.Selection.SnapshotID == rec.New.Selection.SnapshotID {
		return nil, NewRefusal("cutover_selection", "old and new endpoints must differ")
	}
	if rec.Rollback != rec.Old.Publication {
		return nil, NewRefusal("cutover_rollback", "rollback does not match old publication pin")
	}
	if _, _, err := resolvePinnedStoreObject(rec.Rollback, store); err != nil {
		return nil, err
	}
	manifestPin, err := remountJSON[Binding](newPubObj["manifest"])
	if err != nil {
		return nil, err
	}
	manifestObj, _, err := resolvePinnedStoreObject(manifestPin, store)
	if err != nil {
		return nil, err
	}
	manSel, err := remountJSON[Selection](manifestObj["selection"])
	if err != nil {
		return nil, err
	}
	profMetaPin, err := remountJSON[Binding](manifestObj["profile_metadata"])
	if err != nil {
		return nil, err
	}
	if manSel != rec.New.Selection || profMetaPin.ID != rec.New.Selection.SnapshotID {
		return nil, NewRefusal("cutover_selection", "new manifest selection or profile_metadata id mismatch")
	}
	metaObj, _, err := resolvePinnedStoreObject(profMetaPin, store)
	if err != nil {
		return nil, err
	}
	if err := validateShapeSchema(metaObj, durableindexv0.IndexMetadataSchemaPath); err != nil {
		return nil, err
	}
	metaSel, err := remountJSON[Selection](metaObj["selection"])
	if err != nil {
		return nil, err
	}
	if metaSel != rec.New.Selection {
		return nil, NewRefusal("cutover_selection", "new metadata selection mismatch")
	}
	mode, _ := asString(metaObj["mode"])
	gen, _ := asInt64Exact(metaObj["generation"])
	if mode != ModeBaseline || gen != 0 || metaObj["parent"] != nil {
		return nil, NewRefusal("cutover_baseline", "new cutover endpoint is not an explicit baseline")
	}
	obsAt, _ := asString(metaObj["observation_at"])
	if err := ValidateInstant(obsAt); err != nil {
		return nil, err
	}
	if rec.AcceptedAt < obsAt {
		return nil, NewRefusal("cutover_time", "cutover accepted_at precedes new baseline observation_at")
	}
	return map[string]any{"cutover": "explicit_baseline"}, nil
}

// VerifyRowMembers validates cross-member logical rows in manifest order.
func VerifyRowMembers(caseMap map[string]any) (map[string]any, error) {
	membersSlice, ok := asSlice(caseMap["members"])
	if !ok {
		return nil, NewRefusal("invalid_shape", "members must be an array")
	}
	var allRows []LogicalRow
	for _, mRaw := range membersSlice {
		mSlice, ok := asSlice(mRaw)
		if !ok {
			return nil, NewRefusal("invalid_shape", "member must be an array of rows")
		}
		for _, rRaw := range mSlice {
			r, err := parseRowMap(rRaw)
			if err != nil {
				return nil, err
			}
			allRows = append(allRows, r)
		}
	}
	if err := ValidateRows(allRows, ""); err != nil {
		return nil, err
	}
	return map[string]any{"row_count": int64(len(allRows))}, nil
}

func validateFieldCatalogShape(catalogObj map[string]any) error {
	wrapper := map[string]any{
		"capabilities": []any{"contract: data-artifact/v0"},
		"artifact_id":  "artifact:catalog-check",
		"lifecycle":    "complete",
		"producer": map[string]any{
			"name":    "verifier",
			"version": "1.0.0",
			"profile": LogicalProfileID,
		},
		"grains": []any{
			map[string]any{
				"id":          "objects",
				"kind":        "record_stream",
				"record_kind": RowProfileObjectInventoryV0,
			},
		},
		"representations": []any{
			map[string]any{
				"id":     "logical-rows",
				"role":   "audit_stream",
				"format": "ndjson",
				"uri":    "artifact:manifest",
				"read_path": map[string]any{
					"appendable": false,
				},
				"protection_enforceable_granularity": "artifact",
			},
		},
		"field_catalogs": []any{catalogObj},
		"protection": map[string]any{
			"default_action": "block_export",
		},
	}
	return validateShapeSchema(wrapper, artifactDescriptorSchemaRelPath)
}

type graphSnapshotResult struct {
	selection       Selection
	metadata        map[string]any
	scopes          []Scope
	rows            []LogicalRow
	evidence        map[string]any
	protectionClass string
}

// VerifyLogicalGraph validates a complete byte-bound logical projection graph (vectors/graphs/).
func VerifyLogicalGraph(caseMap map[string]any) (map[string]any, error) {
	proj, _ := asString(caseMap["projection"])
	if proj != LogicalProjectionMode {
		return nil, NewRefusal("graph_profile", "unsupported projection mode")
	}
	store, err := parseStoreMap(caseMap["store"])
	if err != nil {
		return nil, err
	}
	upstreamFS, err := durableindexv0.FS()
	if err != nil {
		return nil, err
	}
	profileSpecBytes, err := fs.ReadFile(upstreamFS, logicalProfileSpecRelPath)
	if err != nil {
		return nil, err
	}
	indexRowSchemaBytes, err := fs.ReadFile(upstreamFS, durableindexv0.IndexRowSchemaPath)
	if err != nil {
		return nil, err
	}

	visited := make(map[string]struct{})
	active := make(map[string]struct{})
	allMetadataRefs := make(map[string]struct{})
	memberRefs := make(map[string]struct{})

	fetchBinding := func(pin Binding, metadataRefs map[string]struct{}, maxMetaCeiling, maxMemberCeiling int64, schemaRel string, isMember bool) (map[string]any, []byte, error) {
		rawStr, ok := store[pin.Ref]
		if !ok {
			return nil, nil, NewRefusal("binding_unavailable", fmt.Sprintf("missing store ref %q", pin.Ref))
		}
		if pin.ByteLength < 0 || int64(len(rawStr)) != pin.ByteLength {
			return nil, nil, NewRefusal("binding_integrity", fmt.Sprintf("digest or length mismatch for %q", pin.Ref))
		}
		if isMember {
			memberRefs[pin.Ref] = struct{}{}
		} else {
			metadataRefs[pin.Ref] = struct{}{}
		}
		for mr := range metadataRefs {
			allMetadataRefs[mr] = struct{}{}
		}
		for mr := range allMetadataRefs {
			if _, conflict := memberRefs[mr]; conflict {
				return nil, nil, NewRefusal("binding_alias", "metadata and member bindings alias a ref")
			}
		}
		metaLimit := int64(MaxMetadataBytes)
		if maxMetaCeiling > 0 && maxMetaCeiling < metaLimit {
			metaLimit = maxMetaCeiling
		}
		var metaBudget int64
		for mr := range metadataRefs {
			metaBudget += int64(len(store[mr]))
		}
		if metaBudget > metaLimit {
			return nil, nil, NewRefusal("capacity", "snapshot metadata budget exceeds limit")
		}
		if isMember {
			memberLimit := int64(MaxDecompressedMemberBytes)
			if maxMemberCeiling > 0 && maxMemberCeiling < memberLimit {
				memberLimit = maxMemberCeiling
			}
			if int64(len(rawStr)) > memberLimit {
				return nil, nil, NewRefusal("capacity", "member byte length exceeds profile max_member_bytes")
			}
		}
		raw := []byte(rawStr)
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != pin.SHA256 {
			return nil, nil, NewRefusal("binding_integrity", fmt.Sprintf("digest or length mismatch for %q", pin.Ref))
		}
		if schemaRel == "" {
			return nil, raw, nil
		}
		parsed, err := StrictParseContractJSON(raw)
		if err != nil {
			return nil, nil, err
		}
		obj, ok := asMap(parsed)
		if !ok {
			return nil, nil, NewRefusal("invalid_shape", "expected JSON object")
		}
		if schemaRel == "fieldCatalog" {
			if err := validateFieldCatalogShape(obj); err != nil {
				return nil, nil, err
			}
		} else if err := validateShapeSchema(obj, schemaRel); err != nil {
			return nil, nil, err
		}
		return obj, raw, nil
	}

	var verifySnapshot func(pin Binding) (graphSnapshotResult, error)
	verifySnapshot = func(pin Binding) (graphSnapshotResult, error) {
		metadataRefs := make(map[string]struct{})
		var maxMetaBytes int64
		var maxMemberBytes int64
		fetch := func(b Binding, schemaRel string, isMember bool) (map[string]any, []byte, error) {
			return fetchBinding(b, metadataRefs, maxMetaBytes, maxMemberBytes, schemaRel, isMember)
		}
		if _, cyc := active[pin.SHA256]; cyc {
			return graphSnapshotResult{}, NewRefusal("parent_cycle", "cycle in parent publication chain")
		}
		if len(active) >= MaxAncestryDepth {
			return graphSnapshotResult{}, NewRefusal("capacity", "ancestry depth limit exceeded")
		}
		active[pin.SHA256] = struct{}{}
		defer delete(active, pin.SHA256)

		pub, _, err := fetch(pin, durableindexv0.IndexPublicationSchemaPath, false)
		if err != nil {
			return graphSnapshotResult{}, err
		}
		sel, err := remountJSON[Selection](pub["selection"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		pubID, _ := asString(pub["publication_id"])
		if pubID != pin.ID {
			return graphSnapshotResult{}, NewRefusal("graph_identity", "publication_id differs from binding id")
		}
		if _, reused := visited[sel.SnapshotID]; reused {
			return graphSnapshotResult{}, NewRefusal("snapshot_reuse", "snapshot_id reused across graph")
		}
		visited[sel.SnapshotID] = struct{}{}

		manPin, err := remountJSON[Binding](pub["manifest"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		profPin, err := remountJSON[Binding](pub["profile"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		descPin, err := remountJSON[Binding](pub["descriptor"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		profile, _, err := fetch(profPin, snapshotProfileSchemaRelPath, false)
		if err != nil {
			return graphSnapshotResult{}, err
		}
		limitsMap, _ := asMap(profile["limits"])
		maxMembers, _ := asInt64Exact(limitsMap["max_members"])
		maxMemberBytes, _ = asInt64Exact(limitsMap["max_member_bytes"])
		maxMetaBytes, _ = asInt64Exact(limitsMap["max_metadata_bytes"])
		metaLimit := int64(MaxMetadataBytes)
		if maxMetaBytes > 0 && maxMetaBytes < metaLimit {
			metaLimit = maxMetaBytes
		}
		var initialMetaBudget int64
		for mr := range metadataRefs {
			initialMetaBudget += int64(len(store[mr]))
		}
		if initialMetaBudget > metaLimit {
			return graphSnapshotResult{}, NewRefusal("capacity", "metadata bytes exceed profile max_metadata_bytes")
		}
		manifest, _, err := fetch(manPin, snapshotManifestSchemaRelPath, false)
		if err != nil {
			return graphSnapshotResult{}, err
		}
		descriptor, _, err := fetch(descPin, artifactDescriptorSchemaRelPath, false)
		if err != nil {
			return graphSnapshotResult{}, err
		}

		pubState, _ := asString(pub["state"])
		descLifecycle, _ := asString(descriptor["lifecycle"])
		if pubState != "committed" || (descLifecycle != "complete" && descLifecycle != "partial") {
			return graphSnapshotResult{}, NewRefusal("graph_publication", "publication or descriptor is not readable committed state")
		}
		manSel, err := remountJSON[Selection](manifest["selection"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		descArtID, _ := asString(descriptor["artifact_id"])
		if manSel != sel || manPin.ID != sel.SnapshotID || descPin.ID != descArtID || descArtID != sel.ArtifactID {
			return graphSnapshotResult{}, NewRefusal("graph_identity", "manifest or descriptor identity mismatch")
		}

		manCaps, _ := asSlice(manifest["capabilities"])
		hasIndexCap := false
		for _, c := range manCaps {
			if cs, ok := asString(c); ok && cs == ContractCapability {
				hasIndexCap = true
				break
			}
		}
		if !hasIndexCap {
			return graphSnapshotResult{}, NewRefusal("graph_profile", "manifest lacks durable-index/v0 capability")
		}
		profID, _ := asString(profile["profile_id"])
		if !reflect.DeepEqual(manifest["profile"], pub["profile"]) || profID != profPin.ID || profID != LogicalProfileID {
			return graphSnapshotResult{}, NewRefusal("graph_profile", "profile binding or profile_id mismatch")
		}
		semSpecPin, err := remountJSON[Binding](profile["semantic_spec"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		_, semSpecRaw, err := fetch(semSpecPin, "", false)
		if err != nil {
			return graphSnapshotResult{}, err
		}
		if semSpecPin.ID != LogicalProfileID || string(semSpecRaw) != string(profileSpecBytes) {
			return graphSnapshotResult{}, NewRefusal("graph_profile", "semantic_spec mismatch")
		}
		if !reflect.DeepEqual(profile["operations"], []any{"full_verify"}) ||
			!reflect.DeepEqual(profile["count_relations"], []any{"equal"}) ||
			!reflect.DeepEqual(profile["physical_orders"], []any{"manifest_order"}) ||
			profile["retention"] != "unsupported" ||
			profile["durability_claim"] != "none" ||
			profile["streaming"] != "unsupported" {
			return graphSnapshotResult{}, NewRefusal("graph_profile", "profile capability flags mismatch")
		}
		if manifest["count_relation"] != "equal" || manifest["physical_order"] != "manifest_order" {
			return graphSnapshotResult{}, NewRefusal("graph_profile", "manifest count_relation or physical_order mismatch")
		}
		pmRaw, hasPM := manifest["profile_metadata"]
		if !hasPM || pmRaw == nil {
			return graphSnapshotResult{}, NewRefusal("graph_dependency", "manifest missing profile_metadata")
		}
		pmPin, err := remountJSON[Binding](pmRaw)
		if err != nil {
			return graphSnapshotResult{}, err
		}
		meta, _, err := fetch(pmPin, durableindexv0.IndexMetadataSchemaPath, false)
		if err != nil {
			return graphSnapshotResult{}, err
		}
		metaSel, err := remountJSON[Selection](meta["selection"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		if metaSel != sel || pmPin.ID != sel.SnapshotID {
			return graphSnapshotResult{}, NewRefusal("graph_identity", "metadata selection or binding id mismatch")
		}
		covSubjURI, _ := asString(manifest["coverage_subject_uri"])
		if covSubjURI != SubjectURI(sel) {
			return graphSnapshotResult{}, NewRefusal("coverage_subject", "manifest coverage_subject_uri mismatch")
		}

		rowSchemaMap, _ := asMap(manifest["row_schema"])
		rsRef, _ := asString(rowSchemaMap["ref"])
		rsSHA, _ := asString(rowSchemaMap["sha256"])
		rsLen, _ := asInt64Exact(rowSchemaMap["byte_length"])
		rowSchemaPin := Binding{Ref: rsRef, SHA256: rsSHA, ByteLength: rsLen}
		_, rowSchemaRaw, err := fetch(rowSchemaPin, "", false)
		if err != nil {
			return graphSnapshotResult{}, err
		}
		schemaID, _ := asString(rowSchemaMap["schema_id"])
		if string(rowSchemaRaw) != string(indexRowSchemaBytes) || schemaID != "contract:durable-index/v0/index-row.schema.json" {
			return graphSnapshotResult{}, NewRefusal("graph_row_schema", "bound row_schema mismatch")
		}

		fcPin, err := remountJSON[Binding](manifest["field_catalog"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		catalog, _, err := fetch(fcPin, "fieldCatalog", false)
		if err != nil {
			return graphSnapshotResult{}, err
		}
		catID, _ := asString(catalog["id"])
		catGrain, _ := asString(catalog["grain"])
		manGrainID, _ := asString(manifest["grain_id"])
		if catID != fcPin.ID || catGrain != manGrainID {
			return graphSnapshotResult{}, NewRefusal("graph_catalog", "catalog id or grain mismatch")
		}

		grainsSlice, _ := asSlice(descriptor["grains"])
		repsSlice, _ := asSlice(descriptor["representations"])
		if len(grainsSlice) != 1 || len(repsSlice) != 1 {
			return graphSnapshotResult{}, NewRefusal("graph_descriptor", "descriptor must have 1 grain and 1 representation")
		}
		grainMap, _ := asMap(grainsSlice[0])
		repMap, _ := asMap(repsSlice[0])
		manRepID, _ := asString(manifest["representation_id"])
		if grainMap["id"] != manGrainID || repMap["grain"] != manGrainID ||
			repMap["id"] != manRepID ||
			grainMap["field_catalog_ref"] != catID || repMap["field_catalog_ref"] != catID {
			return graphSnapshotResult{}, NewRefusal("graph_descriptor", "descriptor grain/representation/catalog references disagree")
		}
		prodMap, _ := asMap(descriptor["producer"])
		readPathMap, _ := asMap(repMap["read_path"])
		if repMap["profile"] != LogicalProfileID || prodMap["profile"] != LogicalProfileID ||
			repMap["uri"] != manPin.Ref || repMap["format"] != "ndjson" ||
			readPathMap["physical_ordering"] != "manifest_order" || readPathMap["appendable"] != false {
			return graphSnapshotResult{}, NewRefusal("graph_descriptor", "descriptor representation profile/uri/format/read_path mismatch")
		}
		if fcListRaw, hasFCList := descriptor["field_catalogs"]; hasFCList {
			if !reflect.DeepEqual(fcListRaw, []any{catalog}) {
				return graphSnapshotResult{}, NewRefusal("graph_catalog", "embedded descriptor field_catalogs disagrees with catalog")
			}
		}
		expectedFieldTypes := map[string]string{
			"namespace":     "string",
			"key":           "string",
			"revision":      "object",
			"size_bytes":    "integer",
			"last_modified": "datetime",
			"storage_class": "string",
			"first_seen":    "object",
			"last_seen":     "object",
			"last_changed":  "object",
			"deleted":       "object",
			"enrichments":   "array",
		}
		catFieldsSlice, _ := asSlice(catalog["fields"])
		seenFields := make(map[string]map[string]any, len(catFieldsSlice))
		for _, fRaw := range catFieldsSlice {
			fm, _ := asMap(fRaw)
			fname, _ := asString(fm["name"])
			seenFields[fname] = fm
		}
		if len(seenFields) != len(catFieldsSlice) || len(seenFields) != len(expectedFieldTypes) {
			return graphSnapshotResult{}, NewRefusal("graph_catalog", "catalog field inventory mismatch")
		}
		for fname, wantType := range expectedFieldTypes {
			fm, ok := seenFields[fname]
			if !ok || fm["type"] != wantType || fm["required"] != true {
				return graphSnapshotResult{}, NewRefusal("graph_catalog", fmt.Sprintf("catalog field %q type or required mismatch", fname))
			}
		}
		if wf, hasWF := catalog["withheld_fields"]; hasWF {
			if wfs, ok := asSlice(wf); !ok || len(wfs) > 0 {
				return graphSnapshotResult{}, NewRefusal("graph_catalog", "withheld_fields must be empty")
			}
		}
		if wfc, hasWFC := catalog["withheld_field_count"]; hasWFC {
			if n, ok := asInt64Exact(wfc); !ok || n != 0 {
				return graphSnapshotResult{}, NewRefusal("graph_catalog", "withheld_field_count must be 0")
			}
		}

		protMap, _ := asMap(descriptor["protection"])
		rank := map[string]int{
			"public":     0,
			"internal":   1,
			"controlled": 2,
			"restricted": 3,
			"unknown":    3,
		}
		defClass := "unknown"
		if dc, ok := asString(protMap["default_export_class"]); ok && dc != "" {
			defClass = dc
		}
		effRank := rank[defClass]
		for _, fm := range seenFields {
			sens, _ := asString(fm["sensitivity"])
			if r := rank[sens]; r > effRank {
				effRank = r
			}
		}
		rankNames := [...]string{"public", "internal", "controlled", "restricted"}
		effClass := rankNames[effRank]
		if reqExp, _ := caseMap["request_export"].(bool); reqExp {
			return graphSnapshotResult{}, NewRefusal("protected_export", "export blocked by protection policy")
		}

		membersSlice, _ := asSlice(manifest["members"])
		memberIDs := make(map[string]struct{}, len(membersSlice))
		memberRefSet := make(map[string]struct{}, len(membersSlice))
		for _, mRaw := range membersSlice {
			mm, _ := asMap(mRaw)
			id, _ := asString(mm["id"])
			ref, _ := asString(mm["ref"])
			memberIDs[id] = struct{}{}
			memberRefSet[ref] = struct{}{}
		}
		if len(memberIDs) != len(membersSlice) || len(memberRefSet) != len(membersSlice) {
			return graphSnapshotResult{}, NewRefusal("graph_members", "duplicate member id or ref")
		}
		emptyAllowed, _ := profile["empty_snapshots"].(bool)
		if len(membersSlice) == 0 && !emptyAllowed {
			return graphSnapshotResult{}, NewRefusal("graph_members", "empty member set not permitted by profile")
		}
		if int64(len(membersSlice)) > maxMembers {
			return graphSnapshotResult{}, NewRefusal("capacity", "member count exceeds profile max_members")
		}

		var rows []LogicalRow
		for _, mRaw := range membersSlice {
			mm, _ := asMap(mRaw)
			mID, _ := asString(mm["id"])
			mRef, _ := asString(mm["ref"])
			mSHA, _ := asString(mm["sha256"])
			mLen, _ := asInt64Exact(mm["byte_length"])
			mPin := Binding{ID: mID, Ref: mRef, SHA256: mSHA, ByteLength: mLen}
			_, mBytes, err := fetch(mPin, "", true)
			if err != nil {
				return graphSnapshotResult{}, err
			}
			trimmed := strings.TrimSuffix(string(mBytes), "\n")
			var lines []string
			if trimmed != "" {
				lines = strings.Split(trimmed, "\n")
			}
			mRowCount, _ := asInt64Exact(mm["row_count"])
			if int64(len(lines)) != mRowCount {
				return graphSnapshotResult{}, NewRefusal("graph_count", "member decoded row count disagrees with member.row_count")
			}
			for _, line := range lines {
				parsedLine, err := StrictParseContractJSON([]byte(line))
				if err != nil {
					return graphSnapshotResult{}, err
				}
				r, err := parseRowMap(parsedLine)
				if err != nil {
					return graphSnapshotResult{}, err
				}
				rows = append(rows, r)
			}
		}
		if err := ValidateRows(rows, ""); err != nil {
			return graphSnapshotResult{}, err
		}
		physCount, _ := asInt64Exact(manifest["physical_row_count"])
		grainCount, _ := asInt64Exact(grainMap["row_count"])
		repCount, _ := asInt64Exact(repMap["row_count"])
		if int64(len(rows)) != physCount || physCount != grainCount || grainCount != repCount {
			return graphSnapshotResult{}, NewRefusal("graph_count", "manifest, grain, or representation row_count mismatch")
		}

		var parentMap any
		var inheritedEvidence map[string]any
		if meta["parent"] != nil {
			pBind, err := remountJSON[ParentBinding](meta["parent"])
			if err != nil {
				return graphSnapshotResult{}, err
			}
			prior, err := verifySnapshot(pBind.Publication)
			if err != nil {
				return graphSnapshotResult{}, err
			}
			if prior.selection != pBind.Selection {
				return graphSnapshotResult{}, NewRefusal("graph_identity", "parent selection mismatch")
			}
			if prior.selection.NamespaceID != sel.NamespaceID {
				return graphSnapshotResult{}, NewRefusal("set_mismatch", "cross-set parent edge")
			}
			pGen, _ := asInt64Exact(prior.metadata["generation"])
			parentMap = map[string]any{
				"publication_sha256": pBind.Publication.SHA256,
				"set_id":             prior.selection.NamespaceID,
				"snapshot_id":        prior.selection.SnapshotID,
				"generation":         pGen,
				"scopes":             scopesToAnySlice(prior.scopes),
				"rows":               rowsToAnySlice(prior.rows),
			}
			inheritedEvidence = prior.evidence
			if rank[prior.protectionClass] > rank[effClass] {
				effClass = prior.protectionClass
			}
		}

		srcEvAll, _ := asMap(caseMap["source_evidence"])
		srcRaw, hasSrc := srcEvAll[sel.SnapshotID]
		if !hasSrc || srcRaw == nil {
			return graphSnapshotResult{}, NewRefusal("graph_evidence", "missing source_evidence for snapshot")
		}
		srcMap, _ := asMap(srcRaw)
		evidence := map[string]any{
			"metadata":     meta,
			"store":        caseMap["store"],
			"grain_id":     manGrainID,
			"observations": srcMap["observations"],
			"enumerated":   srcMap["enumerated"],
		}
		if inheritedEvidence != nil {
			evidence["inherited_from"] = inheritedEvidence
		}

		planPin, err := remountJSON[Binding](meta["plan"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		if _, _, err := fetch(planPin, "", false); err != nil {
			return graphSnapshotResult{}, err
		}
		covPins, err := remountJSON[[]Binding](meta["coverage"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		for _, cp := range covPins {
			if _, _, err := fetch(cp, "", false); err != nil {
				return graphSnapshotResult{}, err
			}
		}
		planObj, _, err := resolvePinnedStoreObject(planPin, store)
		if err != nil {
			return graphSnapshotResult{}, err
		}
		scopes, err := parseScopeList(planObj["scopes"])
		if err != nil {
			return graphSnapshotResult{}, err
		}

		var ackAny any
		if meta["loss_acknowledgment"] != nil {
			la, err := remountJSON[LossAcknowledgment](meta["loss_acknowledgment"])
			if err != nil {
				return graphSnapshotResult{}, err
			}
			ackAny = map[string]any{
				"parent_sha256":   la.Parent.Publication.SHA256,
				"plan_digest":     la.PlanSHA256,
				"acknowledged_by": la.AcknowledgedBy,
				"acknowledged_at": la.AcknowledgedAt,
			}
		}
		transitionMap := map[string]any{
			"mode":                   meta["mode"],
			"set_id":                 sel.NamespaceID,
			"snapshot_id":            sel.SnapshotID,
			"generation":             meta["generation"],
			"observation_at":         meta["observation_at"],
			"discovery_complete":     srcMap["discovery_complete"],
			"parent":                 parentMap,
			"admitted_parent_sha256": srcMap["admitted_parent_sha256"],
			"scopes":                 planObj["scopes"],
			"acknowledgment":         ackAny,
			"observations":           srcMap["observations"],
			"absence_authority":      []any{},
			"enrichments":            srcMap["enrichments"],
		}
		expected, err := VerifyBoundTransition(map[string]any{
			"evidence":   evidence,
			"transition": transitionMap,
		})
		if err != nil {
			return graphSnapshotResult{}, err
		}
		if !reflect.DeepEqual(rows, expected.Rows) {
			return graphSnapshotResult{}, NewRefusal("graph_transition", "published rows differ from checked transition output")
		}

		var totalMetaBytes int64
		for mr := range metadataRefs {
			totalMetaBytes += int64(len(store[mr]))
		}
		budgetCeiling := int64(MaxMetadataBytes)
		if maxMetaBytes < budgetCeiling {
			budgetCeiling = maxMetaBytes
		}
		if totalMetaBytes > budgetCeiling {
			return graphSnapshotResult{}, NewRefusal("capacity", "metadata bytes exceed profile max_metadata_bytes")
		}

		return graphSnapshotResult{
			selection:       sel,
			metadata:        meta,
			scopes:          scopes,
			rows:            rows,
			evidence:        evidence,
			protectionClass: effClass,
		}, nil
	}

	rootPin, err := remountJSON[Binding](caseMap["publication"])
	if err != nil {
		return nil, err
	}
	res, err := verifySnapshot(rootPin)
	if err != nil {
		return nil, err
	}
	gen, _ := asInt64Exact(res.metadata["generation"])
	ret, _ := asInt64Exact(res.metadata["retained_unobserved_rows"])
	return map[string]any{
		"rows":                     rowsToAnySlice(res.rows),
		"retained_unobserved_rows": ret,
		"generation":               gen,
		"protection": map[string]any{
			"action": "block_export",
			"class":  res.protectionClass,
		},
		"evidence_scope": LogicalProjectionToken,
	}, nil
}

// VerifyCutoverGraphs validates a complete cutover_graphs scenario.
func VerifyCutoverGraphs(caseMap map[string]any) (map[string]any, error) {
	res, err := VerifyCutover(caseMap)
	if err != nil {
		return nil, err
	}
	recMap, _ := asMap(caseMap["record"])
	oldEp, _ := asMap(recMap["old"])
	newEp, _ := asMap(recMap["new"])
	oldGraph, _ := asMap(caseMap["old_graph"])
	newGraph, _ := asMap(caseMap["new_graph"])
	if !reflect.DeepEqual(oldEp["publication"], oldGraph["publication"]) ||
		!reflect.DeepEqual(newEp["publication"], newGraph["publication"]) {
		return nil, NewRefusal("cutover_selection", "cutover endpoint publication disagrees with graph publication")
	}
	topStore, err := parseStoreMap(caseMap["store"])
	if err != nil {
		return nil, err
	}
	for _, sideGraph := range []map[string]any{oldGraph, newGraph} {
		sideStore, err := parseStoreMap(sideGraph["store"])
		if err != nil {
			return nil, err
		}
		for ref, raw := range sideStore {
			if topRaw, ok := topStore[ref]; !ok || topRaw != raw {
				return nil, NewRefusal("binding_integrity", "graph store disagrees with top-level cutover store")
			}
		}
		if _, err := VerifyLogicalGraph(sideGraph); err != nil {
			return nil, err
		}
	}
	res["evidence_scope"] = LogicalProjectionToken
	return res, nil
}

func scopesToAnySlice(scopes []Scope) []any {
	out := make([]any, len(scopes))
	for i, s := range scopes {
		out[i] = map[string]any{
			"namespace": s.Namespace,
			"prefix":    s.Prefix,
		}
	}
	return out
}

func rowsToAnySlice(rows []LogicalRow) []any {
	raw, _ := json.Marshal(rows)
	val, _ := StrictParseContractJSON(raw)
	if s, ok := asSlice(val); ok {
		return s
	}
	return []any{}
}

// LoadCorpusCase loads a positive or overlay reject case from schemas/durable-index/v0/<relPath>.
func LoadCorpusCase(relPath string) (map[string]any, error) {
	upstreamFS, err := durableindexv0.FS()
	if err != nil {
		return nil, err
	}
	fullPath := path.Join("schemas/durable-index/v0", relPath)
	raw, err := fs.ReadFile(upstreamFS, fullPath)
	if err != nil {
		return nil, err
	}
	parsed, err := StrictParseContractJSON(raw)
	if err != nil {
		return nil, err
	}
	caseMap, ok := asMap(parsed)
	if !ok {
		return nil, fmt.Errorf("%s is not a JSON object", relPath)
	}
	if baseRel, hasBase := asString(caseMap["baseline"]); hasBase && baseRel != "" {
		baseFull := path.Join("schemas/durable-index/v0", baseRel)
		baseRaw, err := fs.ReadFile(upstreamFS, baseFull)
		if err != nil {
			return nil, err
		}
		baseParsed, err := StrictParseContractJSON(baseRaw)
		if err != nil {
			return nil, err
		}
		baseMap, _ := asMap(baseParsed)
		inputCopyRaw, _ := json.Marshal(baseMap["input"])
		inputParsed, err := StrictParseContractJSON(inputCopyRaw)
		if err != nil {
			return nil, err
		}
		inputMap, _ := asMap(inputParsed)
		if repMap, ok := asMap(caseMap["replace"]); ok {
			for k, v := range repMap {
				inputMap[k] = v
			}
		}
		if overlayMap, ok := asMap(caseMap["store_overlay"]); ok {
			storeMap, _ := asMap(inputMap["store"])
			for k, v := range overlayMap {
				storeMap[k] = v
			}
		}
		if omitSlice, ok := asSlice(caseMap["omit_refs"]); ok {
			storeMap, _ := asMap(inputMap["store"])
			for _, r := range omitSlice {
				if rs, ok := asString(r); ok {
					delete(storeMap, rs)
				}
			}
		}
		caseMap["input"] = inputMap
	}
	return caseMap, nil
}

// EvaluateCorpusCase evaluates a loaded corpus case and returns either the expected positive result map or {"refusal": reason}.
func EvaluateCorpusCase(caseMap map[string]any) (map[string]any, error) {
	runner, _ := asString(caseMap["runner"])
	inputMap, _ := asMap(caseMap["input"])
	var result map[string]any
	var evalErr error

	switch runner {
	case "parsing":
		hexStr, _ := asString(inputMap["hex"])
		rawBytes, decErr := hex.DecodeString(hexStr)
		if decErr != nil {
			return nil, decErr
		}
		val, err := StrictParseContractJSON(rawBytes)
		if err != nil {
			evalErr = err
		} else {
			result = map[string]any{"value": val}
		}
	case "transitions":
		trIn, err := parseTransitionMap(inputMap)
		if err != nil {
			evalErr = err
		} else {
			out, err := FoldTransition(trIn)
			if err != nil {
				evalErr = err
			} else {
				result = map[string]any{
					"rows":                     rowsToAnySlice(out.Rows),
					"admission":                out.Admission,
					"retained_unobserved_rows": out.RetainedUnobservedRows,
				}
			}
		}
	case "bindings":
		auth, err := VerifyBindings(inputMap, nil)
		if err != nil {
			evalErr = err
		} else {
			result = map[string]any{
				"absence_authority": scopesToAnySlice(auth),
			}
		}
	case "bound_transitions":
		out, err := VerifyBoundTransition(inputMap)
		if err != nil {
			evalErr = err
		} else {
			result = map[string]any{
				"rows":                     rowsToAnySlice(out.Rows),
				"admission":                out.Admission,
				"retained_unobserved_rows": out.RetainedUnobservedRows,
			}
		}
	case "cutover":
		result, evalErr = VerifyCutover(inputMap)
	case "rows":
		result, evalErr = VerifyRowMembers(inputMap)
	case "graphs":
		result, evalErr = VerifyLogicalGraph(inputMap)
	case "cutover_graphs":
		result, evalErr = VerifyCutoverGraphs(inputMap)
	default:
		return nil, fmt.Errorf("unsupported runner %q", runner)
	}

	if evalErr != nil {
		if reason, ok := RefusalReason(evalErr); ok {
			return map[string]any{"refusal": reason}, nil
		}
		return nil, evalErr
	}
	return result, nil
}
