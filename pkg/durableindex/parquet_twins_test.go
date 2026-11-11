package durableindex

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func TestParquetInventoryWriteReadAndSliceDTwins(t *testing.T) {
	twins, err := GenerateSliceDParquetTwins()
	if err != nil {
		t.Fatalf("GenerateSliceDParquetTwins() error = %v", err)
	}
	if len(twins) != 23 {
		t.Fatalf("len(twins) = %d, want 23 (3 positive + 20 negative)", len(twins))
	}

	// Verify determinism: generating a second time yields byte-identical Parquet and NDJSON artifacts.
	twins2, err := GenerateSliceDParquetTwins()
	if err != nil {
		t.Fatalf("second GenerateSliceDParquetTwins() error = %v", err)
	}
	if len(twins) != len(twins2) {
		t.Fatalf("twin count mismatch: %d vs %d", len(twins), len(twins2))
	}
	var posCount, negCount int
	for i := range twins {
		if twins[i].ParquetSHA256 != twins2[i].ParquetSHA256 || !bytes.Equal(twins[i].ParquetBytes, twins2[i].ParquetBytes) {
			t.Fatalf("twin[%d] (%s) Parquet emission is non-deterministic: %s vs %s", i, twins[i].ID, twins[i].ParquetSHA256, twins2[i].ParquetSHA256)
		}
		if twins[i].ExpectedRefusal == "" {
			posCount++
			decoded, err := ReadParquetInventoryMember(twins[i].ParquetBytes, 0)
			if err != nil {
				t.Fatalf("ReadParquetInventoryMember(%s) error = %v", twins[i].ID, err)
			}
			if int64(len(decoded)) != twins[i].RowCount {
				t.Fatalf("ReadParquetInventoryMember(%s) row count = %d, want %d", twins[i].ID, len(decoded), twins[i].RowCount)
			}
			if !reflect.DeepEqual(decoded, twins[i].LogicalRows) {
				t.Fatalf("ReadParquetInventoryMember(%s) decoded rows mismatch:\ngot:  %+v\nwant: %+v", twins[i].ID, decoded, twins[i].LogicalRows)
			}
			// Also verify NDJSON round-trip parity.
			var ndjsonRows []LogicalRow
			lines := strings.Split(strings.TrimSuffix(string(twins[i].LogicalNDJSON), "\n"), "\n")
			for _, line := range lines {
				val, err := StrictParseContractJSON([]byte(line))
				if err != nil {
					t.Fatalf("StrictParseContractJSON(%s) error = %v", twins[i].ID, err)
				}
				r, err := parseRowMap(val)
				if err != nil {
					t.Fatalf("parseRowMap(%s) error = %v", twins[i].ID, err)
				}
				ndjsonRows = append(ndjsonRows, r)
			}
			if !reflect.DeepEqual(decoded, ndjsonRows) {
				t.Fatalf("Parquet vs NDJSON logical twin parity mismatch for %s", twins[i].ID)
			}
		} else {
			negCount++
			_, err := ReadParquetInventoryMember(twins[i].ParquetBytes, 0)
			reason, ok := RefusalReason(err)
			if !ok || reason != twins[i].ExpectedRefusal {
				t.Fatalf("ReadParquetInventoryMember(%s) refusal = %q (err=%v), want %q", twins[i].ID, reason, err, twins[i].ExpectedRefusal)
			}
		}
	}
	if posCount != 3 || negCount != 20 {
		t.Fatalf("posCount=%d, negCount=%d, want 3 and 20", posCount, negCount)
	}

	// Verify actual ZSTD decompression ceiling enforcement when compressed file size is smaller than
	// maxDecompressedBytes, and even when footer TotalUncompressedSize is forged to be smaller than budget.
	var compRows []LogicalRow
	for i := 0; i < 64; i++ {
		compRows = append(compRows, LogicalRow{
			Namespace:   "objects",
			Key:         fmt.Sprintf("k/%04d/%s", i, strings.Repeat("a", 256)),
			SizeBytes:   int64(i),
			FirstSeen:   Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
			LastSeen:    Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
			LastChanged: Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
			Enrichments: []EnrichmentAttribute{},
		})
	}
	zstdMember, err := WriteParquetInventoryMember(compRows, ParquetWriteOptions{
		Codec:               ParquetCodecZSTD,
		DataPageVersion:     2,
		SizeBytesAnnotation: SizeBytesAnnotationInt64Signed,
	})
	if err != nil {
		t.Fatalf("WriteParquetInventoryMember(compRows) error = %v", err)
	}
	budget := int64(len(zstdMember)) + 512
	_, err = ReadParquetInventoryMember(zstdMember, budget)
	if reason, ok := RefusalReason(err); !ok || reason != "capacity" {
		t.Fatalf("ReadParquetInventoryMember(zstdMember, budget=%d) reason = %q (err=%v), want capacity", budget, reason, err)
	}
	forgedKeyColFooter, err := mutateParquetFooter(zstdMember, func(meta *format.FileMetaData) {
		meta.RowGroups[0].Columns[1].MetaData.TotalUncompressedSize = 32
	})
	if err != nil {
		t.Fatalf("mutateParquetFooter(zstdMember) error = %v", err)
	}
	budget2 := int64(len(forgedKeyColFooter)) + 512
	_, err = ReadParquetInventoryMember(forgedKeyColFooter, budget2)
	if reason, ok := RefusalReason(err); !ok || reason != "capacity" {
		t.Fatalf("ReadParquetInventoryMember(forgedKeyColFooter, budget=%d) reason = %q (err=%v), want capacity", budget2, reason, err)
	}

	// Optional export hook when GONIMBUS_SLICE_D_EXPORT_DIR is set.
	if exportDir := os.Getenv("GONIMBUS_SLICE_D_EXPORT_DIR"); exportDir != "" {
		if err := ExportSliceDParquetTwins(exportDir); err != nil {
			t.Fatalf("ExportSliceDParquetTwins(%q) error = %v", exportDir, err)
		}
		if _, err := os.Stat(filepath.Join(exportDir, "manifest.json")); err != nil {
			t.Fatalf("missing exported manifest.json: %v", err)
		}
	}
}

func TestPhysicalParquetGraphsR2W1W2W3Parity(t *testing.T) {
	graphCases := []struct {
		relPath      string
		wantWScope   string
		wantGen      int64
		wantRetained int64
		opts         ParquetWriteOptions
	}{
		{
			relPath:      "vectors/graphs/baseline.json",
			wantWScope:   "W1",
			wantGen:      0,
			wantRetained: 0,
			opts: ParquetWriteOptions{
				Codec:               ParquetCodecZSTD,
				DataPageVersion:     2,
				SizeBytesAnnotation: SizeBytesAnnotationInt64Signed,
			},
		},
		{
			relPath:      "vectors/graphs/observe.json",
			wantWScope:   "W2",
			wantGen:      1,
			wantRetained: 1,
			opts: ParquetWriteOptions{
				Codec:               ParquetCodecZSTD,
				DataPageVersion:     1,
				SizeBytesAnnotation: SizeBytesAnnotationAbsent,
			},
		},
		{
			relPath:      "vectors/graphs/enrich.json",
			wantWScope:   "W3",
			wantGen:      2,
			wantRetained: 2,
			opts: ParquetWriteOptions{
				Codec:               ParquetCodecUncompressed,
				DataPageVersion:     2,
				SizeBytesAnnotation: SizeBytesAnnotationInt64Signed,
			},
		},
	}

	for _, gc := range graphCases {
		t.Run(gc.relPath, func(t *testing.T) {
			logicalCase, err := LoadCorpusCase(gc.relPath)
			if err != nil {
				t.Fatalf("LoadCorpusCase(%q) error = %v", gc.relPath, err)
			}
			logicalInput, _ := asMap(logicalCase["input"])
			logicalExpected, _ := asMap(logicalCase["expected"])

			pg, err := BuildPhysicalGraphFromLogical(logicalInput, gc.opts)
			if err != nil {
				t.Fatalf("BuildPhysicalGraphFromLogical(%q) error = %v", gc.relPath, err)
			}

			// 1. Verify full transition graph (W1 / W2 / W3).
			gotW, err := VerifyPhysicalGraph(pg)
			if err != nil {
				t.Fatalf("VerifyPhysicalGraph(%q) W-mode error = %v", gc.relPath, err)
			}
			if gotW["evidence_scope"] != gc.wantWScope {
				t.Fatalf("evidence_scope = %v, want %q", gotW["evidence_scope"], gc.wantWScope)
			}
			if gotW["generation"] != gc.wantGen || gotW["retained_unobserved_rows"] != gc.wantRetained {
				t.Fatalf("generation/retained = (%v, %v), want (%d, %d)",
					gotW["generation"], gotW["retained_unobserved_rows"], gc.wantGen, gc.wantRetained)
			}
			if !reflect.DeepEqual(gotW["rows"], logicalExpected["rows"]) {
				t.Fatalf("physical vs logical rows mismatch:\ngot:  %#v\nwant: %#v", gotW["rows"], logicalExpected["rows"])
			}

			// 2. Verify self-contained snapshot reader (R2) with SourceEvidence omitted.
			pgR2 := pg
			pgR2.SourceEvidence = nil
			gotR2, err := VerifyPhysicalGraph(pgR2)
			if err != nil {
				t.Fatalf("VerifyPhysicalGraph(%q) R2-mode error = %v", gc.relPath, err)
			}
			if gotR2["evidence_scope"] != "R2" {
				t.Fatalf("R2 evidence_scope = %v, want R2", gotR2["evidence_scope"])
			}
			if !reflect.DeepEqual(gotR2["rows"], logicalExpected["rows"]) {
				t.Fatalf("R2 rows mismatch")
			}

			// 3. Verify export protection refusal on physical graph.
			pgExport := pg
			pgExport.RequestExport = true
			if _, err := VerifyPhysicalGraph(pgExport); err == nil {
				t.Fatalf("expected protected_export refusal")
			} else if reason, ok := RefusalReason(err); !ok || reason != "protected_export" {
				t.Fatalf("refusal = %q, want protected_export", reason)
			}

			// 4. Verify member byte corruption refuses with binding_integrity.
			for ref, origBytes := range pg.MemberParquet {
				corrupt := append([]byte(nil), origBytes...)
				corrupt[len(corrupt)-1] ^= 0xff
				pgCorrupt := pg
				pgCorrupt.MemberParquet = make(map[string][]byte, len(pg.MemberParquet))
				for k, v := range pg.MemberParquet {
					pgCorrupt.MemberParquet[k] = v
				}
				pgCorrupt.MemberParquet[ref] = corrupt
				_, err := VerifyPhysicalGraph(pgCorrupt)
				if reason, ok := RefusalReason(err); !ok || reason != "binding_integrity" {
					t.Fatalf("corrupted member %s refusal = %q (err=%v), want binding_integrity", ref, reason, err)
				}
				break
			}
		})
	}
}

func rewriteFirstColumnPageHeader(t *testing.T, data []byte, pageOrdinal int, change func(*format.ColumnMetaData, *format.PageHeader)) []byte {
	t.Helper()
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	var meta format.FileMetaData
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), data[len(data)-8-footerLen:len(data)-8], &meta); err != nil {
		t.Fatal(err)
	}
	out := []byte("PAR1")
	for gi := range meta.RowGroups {
		for ci := range meta.RowGroups[gi].Columns {
			col := &meta.RowGroups[gi].Columns[ci]
			cmd := &col.MetaData
			start := cmd.DataPageOffset
			if cmd.DictionaryPageOffset > 0 && cmd.DictionaryPageOffset < start {
				start = cmd.DictionaryPageOffset
			}
			chunk := data[start : start+cmd.TotalCompressedSize]
			nextStart := int64(len(out))
			delta := int64(0)
			var dataPageShift int64
			if gi == 0 && ci == 0 {
				cr := &countingByteReader{r: bufio.NewReader(bytes.NewReader(chunk))}
				var chunkOut []byte
				for ord := 0; cr.n < int64(len(chunk)); ord++ {
					var h format.PageHeader
					hdrStart := cr.n
					if err := thrift.NewDecoder(new(thrift.CompactProtocol).NewReader(cr)).Decode(&h); err != nil {
						t.Fatal(err)
					}
					hdrLen := cr.n - hdrStart
					compLen := int64(h.CompressedPageSize)
					payload := make([]byte, compLen)
					if _, err := cr.Read(payload); err != nil {
						t.Fatal(err)
					}
					if ord == pageOrdinal {
						change(cmd, &h)
					}
					raw, err := thrift.Marshal(new(thrift.CompactProtocol), &h)
					if err != nil {
						t.Fatal(err)
					}
					pageDelta := int64(len(raw)) - hdrLen
					delta += pageDelta
					if ord == 0 && cmd.DictionaryPageOffset > 0 {
						dataPageShift = pageDelta
					}
					chunkOut = append(chunkOut, raw...)
					chunkOut = append(chunkOut, payload...)
				}
				out = append(out, chunkOut...)
			} else {
				out = append(out, chunk...)
			}
			cmd.DataPageOffset = nextStart + (cmd.DataPageOffset - start)
			if cmd.DictionaryPageOffset > 0 {
				cmd.DictionaryPageOffset = nextStart + (cmd.DictionaryPageOffset - start)
				cmd.DataPageOffset += dataPageShift
			}
			cmd.TotalCompressedSize += delta
			cmd.TotalUncompressedSize += delta
			col.ColumnIndexOffset = 0
			col.ColumnIndexLength = 0
			col.OffsetIndexOffset = 0
			col.OffsetIndexLength = 0
		}
	}
	footer, err := thrift.Marshal(new(thrift.CompactProtocol), &meta)
	if err != nil {
		t.Fatal(err)
	}
	out = append(out, footer...)
	var n [4]byte
	// #nosec G115
	binary.LittleEndian.PutUint32(n[:], uint32(len(footer)))
	out = append(out, n[:]...)
	out = append(out, []byte("PAR1")...)
	return out
}

func TestParquetInventoryReaderAllocationBounds(t *testing.T) {
	// 1. Forged footer length in 12-byte file must be rejected before allocating footer buffer.
	forgedFooterLen := []byte("PAR1xxxxPAR1")
	binary.LittleEndian.PutUint32(forgedFooterLen[4:8], 16<<20)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := ValidateParquetInventoryReaderAt(bytes.NewReader(forgedFooterLen), int64(len(forgedFooterLen)), 1024)
	runtime.ReadMemStats(&after)
	if reason, ok := RefusalReason(err); !ok || reason != "invalid_shape" {
		t.Fatalf("forgedFooterLen refusal = %q (err=%v), want invalid_shape", reason, err)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc >= 1<<20 {
		t.Fatalf("forgedFooterLen allocated %d bytes, want < 1 MiB", alloc)
	}

	// 2. Forged ColumnIndexLength must be rejected without allocating page index buffer.
	twins, err := GenerateSliceDParquetTwins()
	if err != nil {
		t.Fatal(err)
	}
	forgedIndexLen, err := mutateParquetFooter(twins[0].ParquetBytes, func(meta *format.FileMetaData) {
		meta.RowGroups[0].Columns[0].ColumnIndexLength = 16 << 20
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = ValidateParquetInventoryReaderAt(bytes.NewReader(forgedIndexLen), int64(len(forgedIndexLen)), 8192)
	runtime.ReadMemStats(&after)
	if reason, ok := RefusalReason(err); !ok || reason != "invalid_shape" {
		t.Fatalf("forgedIndexLen refusal = %q (err=%v), want invalid_shape", reason, err)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc >= 1<<20 {
		t.Fatalf("forgedIndexLen allocated %d bytes, want < 1 MiB", alloc)
	}

	// 3. Forged DictionaryPage NumValues exceeding UncompressedPageSize must be rejected during preflight.
	forgedDictVals := rewriteFirstColumnPageHeader(t, twins[0].ParquetBytes, 0, func(cmd *format.ColumnMetaData, h *format.PageHeader) {
		if !h.DictionaryPageHeader.Valid {
			t.Fatal("expected dictionary page header")
		}
		h.DictionaryPageHeader.V.NumValues = 1024
		cmd.NumValues = 1024
	})
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = ReadParquetInventoryMember(forgedDictVals, 8192)
	runtime.ReadMemStats(&after)
	if reason, ok := RefusalReason(err); !ok || reason != "invalid_shape" {
		t.Fatalf("forgedDictVals refusal = %q (err=%v), want invalid_shape", reason, err)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc >= 1<<20 {
		t.Fatalf("forgedDictVals allocated %d bytes, want < 1 MiB", alloc)
	}

	// 4. Forged DictionaryPage NumValues exceeding cmd.NumValues (while fitting UncompressedPageSize/4) must be rejected.
	forgedDictExceedsCol := rewriteFirstColumnPageHeader(t, twins[0].ParquetBytes, 0, func(cmd *format.ColumnMetaData, h *format.PageHeader) {
		if !h.DictionaryPageHeader.Valid {
			t.Fatal("expected dictionary page header")
		}
		// UncompressedPageSize is 11 bytes (max 2 plain byte_array entries), while cmd.NumValues is 1.
		h.DictionaryPageHeader.V.NumValues = 2
	})
	if _, err := ReadParquetInventoryMember(forgedDictExceedsCol, 8192); err == nil {
		t.Fatal("expected invalid_shape refusal for dictionary page num_values > cmd.NumValues")
	} else if reason, ok := RefusalReason(err); !ok || reason != "invalid_shape" {
		t.Fatalf("forgedDictExceedsCol refusal = %q (err=%v), want invalid_shape", reason, err)
	}

	// 5. Forged DataPage (V1) NumValues exceeding UncompressedPageSize must be rejected.
	forgedDataPageV1Vals := rewriteFirstColumnPageHeader(t, twins[1].ParquetBytes, 1, func(cmd *format.ColumnMetaData, h *format.PageHeader) {
		if !h.DataPageHeader.Valid {
			t.Fatal("expected data page v1 header")
		}
		h.DataPageHeader.V.Encoding = format.Plain
		h.DataPageHeader.V.NumValues = 4096
		cmd.NumValues = 4096
	})
	if _, err := ReadParquetInventoryMember(forgedDataPageV1Vals, 65536); err == nil {
		t.Fatal("expected invalid_shape refusal for data page v1 num_values exceeding uncompressed size")
	} else if reason, ok := RefusalReason(err); !ok || reason != "invalid_shape" {
		t.Fatalf("forgedDataPageV1Vals refusal = %q (err=%v), want invalid_shape", reason, err)
	}

	// 6. Forged DataPageV2 NumValues exceeding UncompressedPageSize must be rejected.
	forgedDataPageV2Vals := rewriteFirstColumnPageHeader(t, twins[0].ParquetBytes, 1, func(cmd *format.ColumnMetaData, h *format.PageHeader) {
		if !h.DataPageHeaderV2.Valid {
			t.Fatal("expected data page v2 header")
		}
		h.DataPageHeaderV2.V.Encoding = format.Plain
		h.DataPageHeaderV2.V.NumValues = 4096
		h.DataPageHeaderV2.V.NumNulls = 0
		h.DataPageHeaderV2.V.NumRows = 1
		cmd.NumValues = 4096
	})
	if _, err := ReadParquetInventoryMember(forgedDataPageV2Vals, 65536); err == nil {
		t.Fatal("expected invalid_shape refusal for data page v2 num_values exceeding uncompressed size")
	} else if reason, ok := RefusalReason(err); !ok || reason != "invalid_shape" {
		t.Fatalf("forgedDataPageV2Vals refusal = %q (err=%v), want invalid_shape", reason, err)
	}
}

func TestVerifyPhysicalGraphR2EmptyPrefixAndSnapshotBindings(t *testing.T) {
	logicalCase, err := LoadCorpusCase("vectors/graphs/baseline.json")
	if err != nil {
		t.Fatal(err)
	}
	logicalInput, _ := asMap(logicalCase["input"])
	pg, err := BuildPhysicalGraphFromLogical(logicalInput, ParquetWriteOptions{
		Codec:               ParquetCodecZSTD,
		DataPageVersion:     2,
		SizeBytesAnnotation: SizeBytesAnnotationInt64Signed,
	})
	if err != nil {
		t.Fatal(err)
	}
	pg.SourceEvidence = nil

	// Helper to clone and re-pin mutated metadata/plan/coverage in R2 physical graph.
	repinR2Graph := func(mutate func(planObj, covObj, metaObj map[string]any)) PhysicalGraphCase {
		store := make(map[string]string, len(pg.Store))
		for k, v := range pg.Store {
			store[k] = v
		}
		pubObj, _, err := resolvePinnedStoreObject(pg.Publication, store)
		if err != nil {
			t.Fatal(err)
		}
		manPin, err := remountJSON[Binding](pubObj["manifest"])
		if err != nil {
			t.Fatal(err)
		}
		manObj, _, err := resolvePinnedStoreObject(manPin, store)
		if err != nil {
			t.Fatal(err)
		}
		metaPin, err := remountJSON[Binding](manObj["profile_metadata"])
		if err != nil {
			t.Fatal(err)
		}
		metaObj, _, err := resolvePinnedStoreObject(metaPin, store)
		if err != nil {
			t.Fatal(err)
		}
		planPin, err := remountJSON[Binding](metaObj["plan"])
		if err != nil {
			t.Fatal(err)
		}
		planObj, _, err := resolvePinnedStoreObject(planPin, store)
		if err != nil {
			t.Fatal(err)
		}
		covPins, err := remountJSON[[]Binding](metaObj["coverage"])
		if err != nil {
			t.Fatal(err)
		}
		covObj, _, err := resolvePinnedStoreObject(covPins[0], store)
		if err != nil {
			t.Fatal(err)
		}

		mutate(planObj, covObj, metaObj)

		newPlanPin, err := storeObjectWithPin(store, planPin.ID, planPin.Ref, planObj)
		if err != nil {
			t.Fatal(err)
		}
		newCovPin, err := storeObjectWithPin(store, covPins[0].ID, covPins[0].Ref, covObj)
		if err != nil {
			t.Fatal(err)
		}
		metaObj["plan"] = newPlanPin
		effCount, _ := asInt64Exact(metaObj["effective_scope_count"])
		if effCount == 1 {
			metaObj["coverage"] = []any{newCovPin}
		} else {
			nextCov := make([]any, len(covPins))
			nextCov[0] = newCovPin
			for i := 1; i < len(covPins); i++ {
				nextCov[i] = covPins[i]
			}
			metaObj["coverage"] = nextCov
		}
		newMetaPin, err := storeObjectWithPin(store, metaPin.ID, metaPin.Ref, metaObj)
		if err != nil {
			t.Fatal(err)
		}
		manObj["profile_metadata"] = newMetaPin
		newManPin, err := storeObjectWithPin(store, manPin.ID, manPin.Ref, manObj)
		if err != nil {
			t.Fatal(err)
		}
		pubObj["manifest"] = newManPin
		newPubPin, err := storeObjectWithPin(store, pg.Publication.ID, pg.Publication.Ref, pubObj)
		if err != nil {
			t.Fatal(err)
		}
		out := pg
		out.Publication = newPubPin
		out.Store = store
		return out
	}

	// 1. Valid empty-prefix plan + coverage (where coverage omits "prefix") must succeed in R2.
	emptyPrefixScopes := []Scope{{Namespace: "objects", Prefix: ""}}
	pgEmptyPrefix := repinR2Graph(func(planObj, covObj, metaObj map[string]any) {
		setID, _ := asString(planObj["set_id"])
		emptyPrefixDigest, err := PlanDigest(setID, emptyPrefixScopes)
		if err != nil {
			t.Fatal(err)
		}
		planObj["scopes"] = []any{map[string]any{"namespace": "objects", "prefix": ""}}
		metaObj["plan_digest"] = emptyPrefixDigest
		metaObj["effective_scope_count"] = int64(1)
		claims, _ := asSlice(covObj["claims"])
		claimMap, _ := asMap(claims[0])
		scopeMap, _ := asMap(claimMap["scope"])
		delete(scopeMap, "prefix")
		volMap, _ := asMap(claimMap["volume"])
		volMap["observed"] = int64(2)
	})
	if _, err := VerifyPhysicalGraph(pgEmptyPrefix); err != nil {
		t.Fatalf("VerifyPhysicalGraph(pgEmptyPrefix) R2 error = %v, want nil", err)
	}

	// 2. Contradictory coverage emitter.run_id must fail R2 with coverage_emitter.
	pgBadEmitter := repinR2Graph(func(_, covObj, _ map[string]any) {
		emitterMap, _ := asMap(covObj["emitter"])
		emitterMap["run_id"] = "wrong-snapshot-id"
	})
	if _, err := VerifyPhysicalGraph(pgBadEmitter); err == nil {
		t.Fatal("expected coverage_emitter refusal in R2")
	} else if reason, ok := RefusalReason(err); !ok || reason != "coverage_emitter" {
		t.Fatalf("refusal = %q (err=%v), want coverage_emitter", reason, err)
	}

	// 3. Contradictory metadata observation_at vs row stamps/coverage as_of must fail R2 with coverage_time.
	pgBadObsAt := repinR2Graph(func(_, _, metaObj map[string]any) {
		metaObj["observation_at"] = "2026-10-18T00:00:09.000000000Z"
	})
	if _, err := VerifyPhysicalGraph(pgBadObsAt); err == nil {
		t.Fatal("expected coverage_time refusal in R2")
	} else if reason, ok := RefusalReason(err); !ok || reason != "coverage_time" {
		t.Fatalf("refusal = %q (err=%v), want coverage_time", reason, err)
	}
}
