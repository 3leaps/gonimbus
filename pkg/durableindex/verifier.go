package durableindex

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"unicode/utf8"

	durableindexv0 "github.com/3leaps/gonimbus/internal/contracts/durableindex/v0"
)

const (
	coverageAttestationSchemaRelPath = "schemas/coverage-attestation/v0/coverage-attestation.schema.json"
	snapshotManifestSchemaRelPath    = "schemas/segmented-snapshot/v0/snapshot-manifest.schema.json"
	snapshotProfileSchemaRelPath     = "schemas/segmented-snapshot/v0/snapshot-profile.schema.json"
	artifactDescriptorSchemaRelPath  = "schemas/data-artifact/v0/artifact-descriptor.schema.json"
	logicalProfileSpecRelPath        = "schemas/durable-index/v0/fixtures/logical-profile.md"
)

// StrictParseContractJSON parses raw UTF-8 JSON bytes under contract strictness rules,
// returning a *RefusalError with reason "invalid_unicode", "duplicate_key", or "invalid_json".
func StrictParseContractJSON(data []byte) (any, error) {
	if !utf8.Valid(data) {
		return nil, NewRefusal("invalid_unicode", "input is not valid UTF-8")
	}
	if err := checkSurrogateEscapesForRefusal(data); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	val, err := parseStrictJSONValue(dec, 0, 256)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, NewRefusal("invalid_json", "trailing tokens after root JSON value")
	}
	return val, nil
}

func checkSurrogateEscapesForRefusal(data []byte) error {
	inString := false
	for i := 0; i < len(data); i++ {
		b := data[i]
		if !inString {
			if b == '"' {
				inString = true
			}
			continue
		}
		if b == '"' {
			inString = false
			continue
		}
		if b != '\\' {
			continue
		}
		if i+1 >= len(data) {
			return NewRefusal("invalid_json", "unterminated escape")
		}
		esc := data[i+1]
		if esc != 'u' {
			i++
			continue
		}
		u1, ok := parseHexUint16(data, i+2)
		if !ok {
			return NewRefusal("invalid_json", "invalid unicode escape")
		}
		if u1 >= 0xDC00 && u1 <= 0xDFFF {
			return NewRefusal("invalid_unicode", "lone low surrogate escape")
		}
		if u1 >= 0xD800 && u1 <= 0xDBFF {
			if i+11 >= len(data) || data[i+6] != '\\' || data[i+7] != 'u' {
				return NewRefusal("invalid_unicode", "unpaired high surrogate escape")
			}
			u2, ok2 := parseHexUint16(data, i+8)
			if !ok2 {
				return NewRefusal("invalid_json", "invalid unicode escape")
			}
			if u2 < 0xDC00 || u2 > 0xDFFF {
				return NewRefusal("invalid_unicode", "high surrogate not followed by low surrogate")
			}
			i += 11
			continue
		}
		i += 5
	}
	return nil
}

func parseHexUint16(data []byte, start int) (uint16, bool) {
	if start+4 > len(data) {
		return 0, false
	}
	var out uint16
	for j := 0; j < 4; j++ {
		c := data[start+j]
		var nib uint16
		switch {
		case c >= '0' && c <= '9':
			nib = uint16(c - '0')
		case c >= 'a' && c <= 'f':
			nib = uint16(c-'a') + 10
		case c >= 'A' && c <= 'F':
			nib = uint16(c-'A') + 10
		default:
			return 0, false
		}
		out = (out << 4) | nib
	}
	return out, true
}

func parseStrictJSONValue(dec *json.Decoder, depth, maxDepth int) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, NewRefusal("invalid_json", err.Error())
	}
	return parseStrictJSONToken(dec, tok, depth, maxDepth)
}

func parseStrictJSONToken(dec *json.Decoder, tok json.Token, depth, maxDepth int) (any, error) {
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '{':
			if depth+1 > maxDepth {
				return nil, NewRefusal("invalid_json", "JSON container nesting exceeds 256")
			}
			obj := make(map[string]any)
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, NewRefusal("invalid_json", err.Error())
				}
				key, ok := keyTok.(string)
				if !ok {
					return nil, NewRefusal("invalid_json", "non-string object key")
				}
				if _, exists := obj[key]; exists {
					return nil, NewRefusal("duplicate_key", fmt.Sprintf("duplicate JSON key %q", key))
				}
				child, err := parseStrictJSONValue(dec, depth+1, maxDepth)
				if err != nil {
					return nil, err
				}
				obj[key] = child
			}
			endTok, err := dec.Token()
			if err != nil || endTok != json.Delim('}') {
				return nil, NewRefusal("invalid_json", "expected closing brace")
			}
			return obj, nil
		case '[':
			if depth+1 > maxDepth {
				return nil, NewRefusal("invalid_json", "JSON container nesting exceeds 256")
			}
			arr := make([]any, 0)
			for dec.More() {
				child, err := parseStrictJSONValue(dec, depth+1, maxDepth)
				if err != nil {
					return nil, err
				}
				arr = append(arr, child)
			}
			endTok, err := dec.Token()
			if err != nil || endTok != json.Delim(']') {
				return nil, NewRefusal("invalid_json", "expected closing bracket")
			}
			return arr, nil
		default:
			return nil, NewRefusal("invalid_json", "unexpected delimiter")
		}
	case json.Number:
		f, err := strconv.ParseFloat(v.String(), 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, NewRefusal("invalid_json", "non-finite JSON number")
		}
		if i, intErr := strconv.ParseInt(v.String(), 10, 64); intErr == nil {
			return i, nil
		}
		return f, nil
	case string, bool, nil:
		return v, nil
	default:
		return nil, NewRefusal("invalid_json", "unsupported JSON token")
	}
}

func remountJSON[T any](v any) (T, error) {
	var out T
	raw, err := json.Marshal(v)
	if err != nil {
		return out, NewRefusal("invalid_shape", err.Error())
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return out, NewRefusal("invalid_shape", err.Error())
	}
	return out, nil
}

func asMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

func asSlice(v any) ([]any, bool) {
	s, ok := v.([]any)
	return s, ok
}

func asString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

func asInt64Exact(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := strconv.ParseInt(n.String(), 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

func parseObservationMap(obsMap map[string]any) (ObservationInput, error) {
	allowed := map[string]struct{}{
		"namespace":     {},
		"key":           {},
		"revision":      {},
		"size_bytes":    {},
		"last_modified": {},
		"storage_class": {},
	}
	if len(obsMap) != len(allowed) {
		return ObservationInput{}, NewRefusal("observation_fields", "observation must contain exact 6 source fields")
	}
	for k := range obsMap {
		if _, ok := allowed[k]; !ok {
			return ObservationInput{}, NewRefusal("observation_fields", fmt.Sprintf("unsupported observation field %q", k))
		}
	}
	ns, ok1 := asString(obsMap["namespace"])
	key, ok2 := asString(obsMap["key"])
	sizeBytes, ok3 := asInt64Exact(obsMap["size_bytes"])
	if !ok1 || !ok2 || !ok3 || sizeBytes < 0 || sizeBytes > MaxExactInteger {
		return ObservationInput{}, NewRefusal("invalid_shape", "invalid observation namespace, key, or size_bytes")
	}
	out := ObservationInput{
		Namespace: ns,
		Key:       key,
		SizeBytes: sizeBytes,
	}
	if revRaw := obsMap["revision"]; revRaw != nil {
		rev, err := remountJSON[Revision](revRaw)
		if err != nil {
			return ObservationInput{}, err
		}
		out.Revision = &rev
	}
	if lmRaw := obsMap["last_modified"]; lmRaw != nil {
		lm, ok := asString(lmRaw)
		if !ok {
			return ObservationInput{}, NewRefusal("invalid_shape", "last_modified must be string or null")
		}
		out.LastModified = &lm
	}
	if scRaw := obsMap["storage_class"]; scRaw != nil {
		sc, ok := asString(scRaw)
		if !ok {
			return ObservationInput{}, NewRefusal("invalid_shape", "storage_class must be string or null")
		}
		out.StorageClass = &sc
	}
	return out, nil
}

func parseRowMap(rowRaw any) (LogicalRow, error) {
	rowMap, ok := asMap(rowRaw)
	if !ok {
		return LogicalRow{}, NewRefusal("invalid_shape", "row must be a JSON object")
	}
	if err := validateShapeSchema(rowMap, durableindexv0.IndexRowSchemaPath); err != nil {
		return LogicalRow{}, err
	}
	if _, ok := asInt64Exact(rowMap["size_bytes"]); !ok {
		return LogicalRow{}, NewRefusal("invalid_shape", "size_bytes must be an exact integer")
	}
	return remountJSON[LogicalRow](rowMap)
}

func parseTransitionMap(caseMap map[string]any) (TransitionInput, error) {
	disc, ok := caseMap["discovery_complete"].(bool)
	if !ok {
		return TransitionInput{}, NewRefusal("invalid_shape", "discovery_complete must be boolean")
	}
	if !disc {
		return TransitionInput{DiscoveryComplete: false}, nil
	}
	mode, _ := asString(caseMap["mode"])
	setID, _ := asString(caseMap["set_id"])
	snapshotID, _ := asString(caseMap["snapshot_id"])
	gen, okGen := asInt64Exact(caseMap["generation"])
	if !okGen {
		return TransitionInput{}, NewRefusal("invalid_shape", "generation must be an integer")
	}
	scopes, err := parseScopeList(caseMap["scopes"])
	if err != nil {
		return TransitionInput{}, err
	}
	out := TransitionInput{
		Mode:              mode,
		SetID:             setID,
		SnapshotID:        snapshotID,
		Generation:        gen,
		DiscoveryComplete: true,
		Scopes:            scopes,
	}
	if obsAtRaw := caseMap["observation_at"]; obsAtRaw != nil {
		s, ok := asString(obsAtRaw)
		if !ok {
			return TransitionInput{}, NewRefusal("invalid_instant", "observation_at must be a string")
		}
		out.ObservationAt = &s
	}
	if admRaw := caseMap["admitted_parent_sha256"]; admRaw != nil {
		s, ok := asString(admRaw)
		if !ok {
			return TransitionInput{}, NewRefusal("invalid_shape", "admitted_parent_sha256 must be a string")
		}
		out.AdmittedParentSHA256 = &s
	}
	if parentRaw := caseMap["parent"]; parentRaw != nil {
		pMap, ok := asMap(parentRaw)
		if !ok {
			return TransitionInput{}, NewRefusal("invalid_shape", "parent must be an object")
		}
		pGen, okPGen := asInt64Exact(pMap["generation"])
		if !okPGen {
			return TransitionInput{}, NewRefusal("invalid_shape", "parent generation must be an integer")
		}
		pScopes, err := parseScopeList(pMap["scopes"])
		if err != nil {
			return TransitionInput{}, err
		}
		pRowsRaw, okRows := asSlice(pMap["rows"])
		if !okRows {
			return TransitionInput{}, NewRefusal("invalid_shape", "parent rows must be an array")
		}
		pRows := make([]LogicalRow, 0, len(pRowsRaw))
		for _, rRaw := range pRowsRaw {
			r, err := parseRowMap(rRaw)
			if err != nil {
				return TransitionInput{}, err
			}
			pRows = append(pRows, r)
		}
		var ancIDs []string
		if ancRaw, hasAnc := pMap["ancestor_snapshot_ids"]; hasAnc && ancRaw != nil {
			ancSlice, ok := asSlice(ancRaw)
			if !ok {
				return TransitionInput{}, NewRefusal("invalid_shape", "ancestor_snapshot_ids must be an array")
			}
			for _, a := range ancSlice {
				as, ok := asString(a)
				if !ok {
					return TransitionInput{}, NewRefusal("invalid_shape", "ancestor_snapshot_id must be a string")
				}
				ancIDs = append(ancIDs, as)
			}
		}
		pubSHA, _ := asString(pMap["publication_sha256"])
		pSetID, _ := asString(pMap["set_id"])
		pSnapID, _ := asString(pMap["snapshot_id"])
		out.Parent = &TransitionParentInput{
			PublicationSHA256:   pubSHA,
			SetID:               pSetID,
			SnapshotID:          pSnapID,
			Generation:          pGen,
			Scopes:              pScopes,
			Rows:                pRows,
			AncestorSnapshotIDs: ancIDs,
		}
	}
	if ackRaw := caseMap["acknowledgment"]; ackRaw != nil {
		ackMap, ok := asMap(ackRaw)
		if !ok {
			return TransitionInput{}, NewRefusal("acknowledgment_actor", "acknowledgment must be an object")
		}
		reqKeys := []string{"parent_sha256", "plan_digest", "acknowledged_by", "acknowledged_at"}
		if len(ackMap) != len(reqKeys) {
			return TransitionInput{}, NewRefusal("acknowledgment_actor", "acknowledgment must contain exact 4 fields")
		}
		for _, rk := range reqKeys {
			if _, ok := ackMap[rk]; !ok {
				return TransitionInput{}, NewRefusal("acknowledgment_actor", "acknowledgment missing required field")
			}
		}
		pSHA, ok1 := asString(ackMap["parent_sha256"])
		pDig, ok2 := asString(ackMap["plan_digest"])
		ackBy, ok3 := asString(ackMap["acknowledged_by"])
		ackAt, ok4 := asString(ackMap["acknowledged_at"])
		if !ok1 || !ok2 || !ok3 || !ok4 {
			return TransitionInput{}, NewRefusal("acknowledgment_actor", "acknowledgment fields must be strings")
		}
		out.Acknowledgment = &TransitionAcknowledgment{
			ParentSHA256:   pSHA,
			PlanDigest:     pDig,
			AcknowledgedBy: ackBy,
			AcknowledgedAt: ackAt,
		}
	}
	if obsSlice, ok := asSlice(caseMap["observations"]); ok {
		out.Observations = make([]ObservationInput, 0, len(obsSlice))
		for _, item := range obsSlice {
			om, ok := asMap(item)
			if !ok {
				return TransitionInput{}, NewRefusal("observation_fields", "observation must be an object")
			}
			parsedObs, err := parseObservationMap(om)
			if err != nil {
				return TransitionInput{}, err
			}
			out.Observations = append(out.Observations, parsedObs)
		}
	}
	if authRaw, hasAuth := caseMap["absence_authority"]; hasAuth && authRaw != nil {
		authScopes, err := parseScopeList(authRaw)
		if err != nil {
			return TransitionInput{}, err
		}
		out.AbsenceAuthority = authScopes
	}
	if enrichSlice, ok := asSlice(caseMap["enrichments"]); ok {
		out.Enrichments = make([]EnrichmentUpdateInput, 0, len(enrichSlice))
		for _, item := range enrichSlice {
			upd, err := remountJSON[EnrichmentUpdateInput](item)
			if err != nil {
				return TransitionInput{}, err
			}
			out.Enrichments = append(out.Enrichments, upd)
		}
	}
	return out, nil
}

func parseScopeList(v any) ([]Scope, error) {
	arr, ok := asSlice(v)
	if !ok {
		return nil, NewRefusal("invalid_shape", "scopes must be an array")
	}
	out := make([]Scope, 0, len(arr))
	for _, item := range arr {
		m, ok := asMap(item)
		if !ok || len(m) != 2 {
			return nil, NewRefusal("invalid_shape", "scope must be {namespace, prefix}")
		}
		ns, ok1 := asString(m["namespace"])
		pf, ok2 := asString(m["prefix"])
		if !ok1 || !ok2 {
			return nil, NewRefusal("invalid_shape", "scope namespace and prefix must be strings")
		}
		out = append(out, Scope{Namespace: ns, Prefix: pf})
	}
	return out, nil
}

func resolvePinnedStoreObject(pin Binding, store map[string]string) (map[string]any, []byte, error) {
	rawStr, ok := store[pin.Ref]
	if !ok {
		return nil, nil, NewRefusal("binding_unavailable", fmt.Sprintf("missing store ref %q", pin.Ref))
	}
	if pin.ByteLength < 0 || int64(len(rawStr)) != pin.ByteLength {
		return nil, nil, NewRefusal("binding_integrity", fmt.Sprintf("digest or length mismatch for %q", pin.Ref))
	}
	if int64(len(rawStr)) > MaxMetadataBytes {
		return nil, nil, NewRefusal("capacity", "pinned metadata object exceeds 64 MiB")
	}
	raw := []byte(rawStr)
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != pin.SHA256 {
		return nil, nil, NewRefusal("binding_integrity", fmt.Sprintf("digest or length mismatch for %q", pin.Ref))
	}
	parsed, err := StrictParseContractJSON(raw)
	if err != nil {
		return nil, nil, err
	}
	obj, ok := asMap(parsed)
	if !ok {
		return nil, nil, NewRefusal("invalid_shape", "resolved artifact is not a JSON object")
	}
	return obj, raw, nil
}

func parseCoverageClaimScope(scopeRaw any) (Scope, error) {
	scopeMap, ok := asMap(scopeRaw)
	if !ok {
		return Scope{}, NewRefusal("coverage_scope", "claim scope must be an object")
	}
	partRaw, hasPart := scopeMap["partition"]
	if !hasPart {
		return Scope{}, NewRefusal("coverage_scope", "claim scope requires partition")
	}
	partMap, ok := asMap(partRaw)
	if !ok || len(partMap) != 1 {
		return Scope{}, NewRefusal("coverage_scope", "partition must have exact key namespace")
	}
	ns, ok := asString(partMap["namespace"])
	if !ok {
		return Scope{}, NewRefusal("coverage_scope", "partition.namespace must be a string")
	}
	if prefRaw, hasPref := scopeMap["prefix"]; hasPref {
		if len(scopeMap) != 2 {
			return Scope{}, NewRefusal("coverage_scope", "unexpected keys in claim scope")
		}
		pref, ok := asString(prefRaw)
		if !ok || pref == "" {
			return Scope{}, NewRefusal("coverage_scope", "claim scope prefix must be non-empty when present")
		}
		return Scope{Namespace: ns, Prefix: pref}, nil
	}
	if len(scopeMap) != 1 {
		return Scope{}, NewRefusal("coverage_scope", "unexpected keys in claim scope")
	}
	return Scope{Namespace: ns, Prefix: ""}, nil
}

func parseStoreMap(v any) (map[string]string, error) {
	m, ok := asMap(v)
	if !ok {
		return nil, NewRefusal("invalid_shape", "store must be an object")
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		s, ok := asString(val)
		if !ok {
			return nil, NewRefusal("invalid_shape", "store value must be a string")
		}
		out[k] = s
	}
	return out, nil
}

// VerifyBindings validates an index_metadata/index_plan/coverage-attestation subgraph against source evidence (I1, I2, I3).
func VerifyBindings(caseMap map[string]any, ancestry []string) ([]Scope, error) {
	metaRaw := caseMap["metadata"]
	if err := validateShapeSchema(metaRaw, durableindexv0.IndexMetadataSchemaPath); err != nil {
		return nil, err
	}
	metaMap, _ := asMap(metaRaw)
	store, err := parseStoreMap(caseMap["store"])
	if err != nil {
		return nil, err
	}
	sel, err := remountJSON[Selection](metaMap["selection"])
	if err != nil {
		return nil, err
	}
	for _, anc := range ancestry {
		if anc == sel.SnapshotID {
			return nil, NewRefusal("parent_cycle", "ancestry repeats snapshot_id")
		}
	}
	if len(ancestry) >= MaxAncestryDepth {
		return nil, NewRefusal("capacity", "ancestry depth limit exceeded")
	}
	planPin, err := remountJSON[Binding](metaMap["plan"])
	if err != nil {
		return nil, err
	}
	planObj, _, err := resolvePinnedStoreObject(planPin, store)
	if err != nil {
		return nil, err
	}
	if err := validateShapeSchema(planObj, durableindexv0.IndexPlanSchemaPath); err != nil {
		return nil, err
	}
	planDoc, err := remountJSON[IndexPlan](planObj)
	if err != nil {
		return nil, err
	}
	if planDoc.SetID != planPin.ID || planDoc.SetID != sel.NamespaceID {
		return nil, NewRefusal("plan_identity", "plan set_id, binding id, and selection namespace_id must agree")
	}
	if err := ValidateCanonicalScopes(planDoc.Scopes); err != nil {
		return nil, err
	}
	effCount, _ := asInt64Exact(metaMap["effective_scope_count"])
	if effCount != int64(len(planDoc.Scopes)) {
		return nil, NewRefusal("scope_count", "metadata effective_scope_count differs from canonical plan length")
	}
	computedDigest, err := PlanDigest(planDoc.SetID, planDoc.Scopes)
	if err != nil {
		return nil, err
	}
	metaPlanDigest, _ := asString(metaMap["plan_digest"])
	if metaPlanDigest != computedDigest {
		return nil, NewRefusal("plan_digest", "metadata plan_digest differs from canonical plan framing")
	}
	covPins, err := remountJSON[[]Binding](metaMap["coverage"])
	if err != nil {
		return nil, err
	}
	if len(covPins) != len(planDoc.Scopes) {
		return nil, NewRefusal("coverage_count", "coverage bindings do not match effective scopes 1:1")
	}
	seenRefs := map[string]struct{}{planPin.Ref: {}}
	seenIDs := make(map[string]struct{}, len(covPins))
	for _, cp := range covPins {
		if _, dup := seenRefs[cp.Ref]; dup {
			return nil, NewRefusal("binding_alias", "duplicate ref across plan and coverage bindings")
		}
		seenRefs[cp.Ref] = struct{}{}
		if _, dup := seenIDs[cp.ID]; dup {
			return nil, NewRefusal("coverage_identity", "duplicate coverage binding id")
		}
		seenIDs[cp.ID] = struct{}{}
	}

	mode, _ := asString(metaMap["mode"])
	if mode == ModeEnrich {
		inhRaw, hasInh := caseMap["inherited_from"]
		if !hasInh || inhRaw == nil {
			return nil, NewRefusal("parent_unavailable", "enrich binding verification requires inherited_from")
		}
		inhMap, ok := asMap(inhRaw)
		if !ok {
			return nil, NewRefusal("parent_unavailable", "inherited_from must be an object")
		}
		nextAncestry := append(append([]string(nil), ancestry...), sel.SnapshotID)
		if _, err := VerifyBindings(inhMap, nextAncestry); err != nil {
			return nil, err
		}
		pm, _ := asMap(inhMap["metadata"])
		parentMap, _ := asMap(metaMap["parent"])
		if !reflect.DeepEqual(parentMap["selection"], pm["selection"]) {
			return nil, NewRefusal("coverage_inheritance", "parent selection disagrees with inherited metadata")
		}
		for _, field := range []string{"plan", "plan_digest", "effective_scope_count", "coverage"} {
			if !reflect.DeepEqual(metaMap[field], pm[field]) {
				return nil, NewRefusal("coverage_inheritance", fmt.Sprintf("enrich metadata changed inherited %s", field))
			}
		}
		obsSlice, _ := asSlice(caseMap["observations"])
		enumSlice, _ := asSlice(caseMap["enumerated"])
		if len(obsSlice) > 0 || len(enumSlice) > 0 {
			return nil, NewRefusal("enrichment_observation", "enrich mode cannot supply observations or enumerated keys")
		}
		return []Scope{}, nil
	}

	obsAt, _ := asString(metaMap["observation_at"])
	if err := ValidateInstant(obsAt); err != nil {
		return nil, err
	}
	obsRawSlice, _ := asSlice(caseMap["observations"])
	type nsKey struct {
		ns  string
		key string
	}
	observedSet := make(map[nsKey]struct{}, len(obsRawSlice))
	observations := make([]ObservationInput, 0, len(obsRawSlice))
	for _, item := range obsRawSlice {
		om, ok := asMap(item)
		if !ok {
			return nil, NewRefusal("observation_fields", "observation must be an object")
		}
		obs, err := parseObservationMap(om)
		if err != nil {
			return nil, err
		}
		if err := ValidateTextUTF8(obs.Key); err != nil {
			return nil, err
		}
		if obs.LastModified != nil {
			if err := ValidateInstant(*obs.LastModified); err != nil {
				return nil, err
			}
		}
		nk := nsKey{ns: obs.Namespace, key: obs.Key}
		if _, dup := observedSet[nk]; dup {
			return nil, NewRefusal("duplicate_observation", "duplicate observation key")
		}
		observedSet[nk] = struct{}{}
		inPlan := false
		for _, sc := range planDoc.Scopes {
			if ScopeContains(sc, obs.Namespace, obs.Key) {
				inPlan = true
				break
			}
		}
		if !inPlan {
			return nil, NewRefusal("observation_outside_plan", "observation outside plan")
		}
		observations = append(observations, obs)
	}

	enumRawSlice, _ := asSlice(caseMap["enumerated"])
	enumeratedSet := make(map[nsKey]struct{}, len(enumRawSlice))
	for _, item := range enumRawSlice {
		em, ok := asMap(item)
		if !ok {
			continue
		}
		ns, _ := asString(em["namespace"])
		k, _ := asString(em["key"])
		enumeratedSet[nsKey{ns: ns, key: k}] = struct{}{}
	}

	grainID, _ := asString(caseMap["grain_id"])
	expectedSubject := map[string]any{
		"artifact_id": sel.ArtifactID,
		"grain_id":    grainID,
		"subject_uri": SubjectURI(sel),
	}

	authority := make([]Scope, 0, len(planDoc.Scopes))
	for i, sc := range planDoc.Scopes {
		pin := covPins[i]
		covObj, _, err := resolvePinnedStoreObject(pin, store)
		if err != nil {
			return nil, err
		}
		if err := validateShapeSchema(covObj, coverageAttestationSchemaRelPath); err != nil {
			return nil, err
		}
		attID, _ := asString(covObj["attestation_id"])
		if attID != pin.ID {
			return nil, NewRefusal("coverage_identity", "attestation_id differs from coverage binding id")
		}
		claimsSlice, _ := asSlice(covObj["claims"])
		if _, hasSup := covObj["supersedes"]; hasSup || len(claimsSlice) != 1 {
			return nil, NewRefusal("coverage_scope", "coverage must have 1 claim and no supersedes")
		}
		claimMap, _ := asMap(claimsSlice[0])
		claimScope, err := parseCoverageClaimScope(claimMap["scope"])
		if err != nil {
			return nil, err
		}
		if claimScope != sc {
			return nil, NewRefusal("coverage_scope", "claim scope differs from plan scope at same index")
		}
		if !reflect.DeepEqual(covObj["subject"], expectedSubject) {
			return nil, NewRefusal("coverage_subject", "coverage subject disagrees with selection")
		}
		emitterMap, _ := asMap(covObj["emitter"])
		rel, _ := asString(emitterMap["relation"])
		runID, _ := asString(emitterMap["run_id"])
		if rel != "producer" || runID != sel.SnapshotID {
			return nil, NewRefusal("coverage_emitter", "coverage emitter relation or run_id mismatch")
		}
		asOf, _ := asString(covObj["as_of"])
		if err := ValidateInstant(asOf); err != nil {
			return nil, err
		}
		if asOf != obsAt {
			return nil, NewRefusal("coverage_time", "coverage as_of differs from observation_at")
		}
		volMap, _ := asMap(claimMap["volume"])
		unit, _ := asString(volMap["unit"])
		obsVol, okObsVol := asInt64Exact(volMap["observed"])
		var expectedInScope int64
		for _, obs := range observations {
			if ScopeContains(sc, obs.Namespace, obs.Key) {
				expectedInScope++
			}
		}
		if unit != "objects" || !okObsVol || obsVol != expectedInScope {
			return nil, NewRefusal("coverage_volume", "coverage volume unit or observed count mismatch")
		}
		if expRaw, hasExp := volMap["expected"]; hasExp {
			expVal, okExp := asInt64Exact(expRaw)
			if !okExp || expVal < expectedInScope || expVal > MaxExactInteger {
				return nil, NewRefusal("coverage_volume", "coverage expected volume invalid")
			}
		}
		gapsSlice, _ := asSlice(covObj["gaps"])
		for _, gRaw := range gapsSlice {
			gMap, _ := asMap(gRaw)
			gs, err := parseCoverageClaimScope(gMap["scope"])
			if err != nil {
				return nil, err
			}
			if !ScopeCovers(sc, gs) {
				return nil, NewRefusal("coverage_gap", "coverage gap lies outside plan scope")
			}
		}
		covState, _ := asString(covObj["coverage_state"])
		if covState == "complete" && len(gapsSlice) > 0 {
			return nil, NewRefusal("coverage_complete_gap", "complete coverage claim contains a gap")
		}
		basis, _ := asString(claimMap["basis"])
		method, _ := asString(claimMap["method"])
		if covState == "complete" && basis == "confirmed" && (method == "enumerated" || method == "reconciled") {
			for ek := range enumeratedSet {
				if ScopeContains(sc, ek.ns, ek.key) {
					if _, pos := observedSet[ek]; !pos {
						return nil, NewRefusal("enumerated_key_omitted", "confirmed-complete scope omitted an enumerated in-scope key")
					}
				}
			}
			authority = append(authority, sc)
		}
	}
	return authority, nil
}
