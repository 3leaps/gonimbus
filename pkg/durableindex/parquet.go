package durableindex

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"

	durableindexv0 "github.com/3leaps/gonimbus/internal/contracts/durableindex/v0"
)

// DefaultMaxParquetDecompressedBytes is the default 128 MiB per-member decompressed byte ceiling.
const DefaultMaxParquetDecompressedBytes int64 = 128 * 1024 * 1024

// SizeBytesAnnotation selects which DDR-0004 size_bytes schema annotation form to emit.
type SizeBytesAnnotation string

const (
	// SizeBytesAnnotationAbsent emits physical INT64 size_bytes with no logical annotation.
	SizeBytesAnnotationAbsent SizeBytesAnnotation = "absent"
	// SizeBytesAnnotationInt64Signed emits physical INT64 size_bytes with INT(64, signed=true).
	SizeBytesAnnotationInt64Signed SizeBytesAnnotation = "int64_signed"
)

// ParquetCodec selects the DDR-0004 column compression codec.
type ParquetCodec string

const (
	// ParquetCodecZSTD selects ZSTD column compression.
	ParquetCodecZSTD ParquetCodec = "zstd"
	// ParquetCodecUncompressed selects UNCOMPRESSED column encoding.
	ParquetCodecUncompressed ParquetCodec = "uncompressed"
)

// ParquetWriteOptions configures deterministic DDR-0004 parquet-inventory/v0 member emission.
type ParquetWriteOptions struct {
	Codec               ParquetCodec
	DataPageVersion     int
	SizeBytesAnnotation SizeBytesAnnotation
	MaxRowsPerRowGroup  int64
}

type parquetRevision struct {
	Kind  string `parquet:"kind,plain"`
	Value string `parquet:"value,plain"`
}

type parquetStamp struct {
	SnapshotID string `parquet:"snapshot_id,dict"`
	At         string `parquet:"at,dict"`
}

type parquetEnrichment struct {
	Name            string          `parquet:"name,dict"`
	Value           *string         `parquet:"value,optional,plain"`
	Operation       string          `parquet:"operation,dict"`
	ObservedAt      string          `parquet:"observed_at,dict"`
	AgainstRevision parquetRevision `parquet:"against_revision"`
}

type parquetRowUnannotatedSize struct {
	Namespace    string              `parquet:"namespace,dict"`
	Key          string              `parquet:"key,plain"`
	Revision     *parquetRevision    `parquet:"revision,optional"`
	SizeBytes    int64               `parquet:"size_bytes,plain"`
	LastModified *string             `parquet:"last_modified,optional,plain"`
	StorageClass *string             `parquet:"storage_class,optional,dict"`
	FirstSeen    parquetStamp        `parquet:"first_seen"`
	LastSeen     parquetStamp        `parquet:"last_seen"`
	LastChanged  parquetStamp        `parquet:"last_changed"`
	Deleted      *parquetStamp       `parquet:"deleted,optional"`
	Enrichments  []parquetEnrichment `parquet:"enrichments,list"`
}

type parquetRowAnnotatedSize struct {
	Namespace    string              `parquet:"namespace,dict"`
	Key          string              `parquet:"key,plain"`
	Revision     *parquetRevision    `parquet:"revision,optional"`
	SizeBytes    int64               `parquet:"size_bytes,int(64),plain"`
	LastModified *string             `parquet:"last_modified,optional,plain"`
	StorageClass *string             `parquet:"storage_class,optional,dict"`
	FirstSeen    parquetStamp        `parquet:"first_seen"`
	LastSeen     parquetStamp        `parquet:"last_seen"`
	LastChanged  parquetStamp        `parquet:"last_changed"`
	Deleted      *parquetStamp       `parquet:"deleted,optional"`
	Enrichments  []parquetEnrichment `parquet:"enrichments,list"`
}

var (
	parquetSchemaUnannotated = parquet.SchemaOf(new(parquetRowUnannotatedSize))
	parquetSchemaAnnotated   = parquet.SchemaOf(new(parquetRowAnnotatedSize))
)

// WriteParquetInventoryMember encodes validated logical rows into a self-contained DDR-0004 parquet-inventory/v0 member.
func WriteParquetInventoryMember(rows []LogicalRow, opts ParquetWriteOptions) ([]byte, error) {
	if err := ValidateRows(rows, ""); err != nil {
		return nil, err
	}
	var codec compress.Codec
	switch opts.Codec {
	case "", ParquetCodecZSTD:
		codec = &parquet.Zstd
	case ParquetCodecUncompressed:
		codec = &parquet.Uncompressed
	default:
		return nil, NewRefusal("graph_profile", fmt.Sprintf("unsupported parquet codec %q", opts.Codec))
	}
	pageVersion := opts.DataPageVersion
	if pageVersion == 0 {
		pageVersion = 2
	}
	if pageVersion != 1 && pageVersion != 2 {
		return nil, NewRefusal("graph_profile", fmt.Sprintf("unsupported parquet data page version %d", pageVersion))
	}
	annotation := opts.SizeBytesAnnotation
	if annotation == "" {
		annotation = SizeBytesAnnotationInt64Signed
	}
	if annotation != SizeBytesAnnotationAbsent && annotation != SizeBytesAnnotationInt64Signed {
		return nil, NewRefusal("graph_profile", fmt.Sprintf("unsupported size_bytes annotation %q", annotation))
	}

	var buf bytes.Buffer
	writerOpts := []parquet.WriterOption{
		parquet.Compression(codec),
		parquet.DataPageVersion(pageVersion),
		&parquet.WriterConfig{
			CreatedBy: "gonimbus parquet-inventory/v0",
		},
	}
	if opts.MaxRowsPerRowGroup > 0 {
		writerOpts = append(writerOpts, parquet.MaxRowsPerRowGroup(opts.MaxRowsPerRowGroup))
	}

	if annotation == SizeBytesAnnotationAbsent {
		writerOpts = append(writerOpts, parquetSchemaUnannotated)
		w := parquet.NewGenericWriter[parquetRowUnannotatedSize](&buf, writerOpts...)
		phys := make([]parquetRowUnannotatedSize, len(rows))
		for i, r := range rows {
			phys[i] = toParquetRowUnannotated(r)
		}
		if _, err := w.Write(phys); err != nil {
			return nil, fmt.Errorf("write parquet rows: %w", err)
		}
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("close parquet writer: %w", err)
		}
	} else {
		writerOpts = append(writerOpts, parquetSchemaAnnotated)
		w := parquet.NewGenericWriter[parquetRowAnnotatedSize](&buf, writerOpts...)
		phys := make([]parquetRowAnnotatedSize, len(rows))
		for i, r := range rows {
			phys[i] = toParquetRowAnnotated(r)
		}
		if _, err := w.Write(phys); err != nil {
			return nil, fmt.Errorf("write parquet rows: %w", err)
		}
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("close parquet writer: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func toParquetRowUnannotated(r LogicalRow) parquetRowUnannotatedSize {
	var rev *parquetRevision
	if r.Revision != nil {
		rev = &parquetRevision{Kind: r.Revision.Kind, Value: r.Revision.Value}
	}
	var del *parquetStamp
	if r.Deleted != nil {
		del = &parquetStamp{SnapshotID: r.Deleted.SnapshotID, At: r.Deleted.At}
	}
	enrichments := make([]parquetEnrichment, len(r.Enrichments))
	for i, e := range r.Enrichments {
		var val *string
		if e.Value != nil {
			cp := *e.Value
			val = &cp
		}
		enrichments[i] = parquetEnrichment{
			Name:            e.Name,
			Value:           val,
			Operation:       e.Operation,
			ObservedAt:      e.ObservedAt,
			AgainstRevision: parquetRevision{Kind: e.AgainstRevision.Kind, Value: e.AgainstRevision.Value},
		}
	}
	var lm *string
	if r.LastModified != nil {
		cp := *r.LastModified
		lm = &cp
	}
	var sc *string
	if r.StorageClass != nil {
		cp := *r.StorageClass
		sc = &cp
	}
	return parquetRowUnannotatedSize{
		Namespace:    r.Namespace,
		Key:          r.Key,
		Revision:     rev,
		SizeBytes:    r.SizeBytes,
		LastModified: lm,
		StorageClass: sc,
		FirstSeen:    parquetStamp{SnapshotID: r.FirstSeen.SnapshotID, At: r.FirstSeen.At},
		LastSeen:     parquetStamp{SnapshotID: r.LastSeen.SnapshotID, At: r.LastSeen.At},
		LastChanged:  parquetStamp{SnapshotID: r.LastChanged.SnapshotID, At: r.LastChanged.At},
		Deleted:      del,
		Enrichments:  enrichments,
	}
}

func toParquetRowAnnotated(r LogicalRow) parquetRowAnnotatedSize {
	u := toParquetRowUnannotated(r)
	return parquetRowAnnotatedSize(u)
}

func fromParquetRowUnannotated(p parquetRowUnannotatedSize) LogicalRow {
	var rev *Revision
	if p.Revision != nil {
		rev = &Revision{Kind: p.Revision.Kind, Value: p.Revision.Value}
	}
	var del *Stamp
	if p.Deleted != nil {
		del = &Stamp{SnapshotID: p.Deleted.SnapshotID, At: p.Deleted.At}
	}
	enrichments := make([]EnrichmentAttribute, len(p.Enrichments))
	for i, e := range p.Enrichments {
		var val *string
		if e.Value != nil {
			cp := *e.Value
			val = &cp
		}
		enrichments[i] = EnrichmentAttribute{
			Name:            e.Name,
			Value:           val,
			Operation:       e.Operation,
			ObservedAt:      e.ObservedAt,
			AgainstRevision: Revision{Kind: e.AgainstRevision.Kind, Value: e.AgainstRevision.Value},
		}
	}
	var lm *string
	if p.LastModified != nil {
		cp := *p.LastModified
		lm = &cp
	}
	var sc *string
	if p.StorageClass != nil {
		cp := *p.StorageClass
		sc = &cp
	}
	return LogicalRow{
		Namespace:    p.Namespace,
		Key:          p.Key,
		Revision:     rev,
		SizeBytes:    p.SizeBytes,
		LastModified: lm,
		StorageClass: sc,
		FirstSeen:    Stamp{SnapshotID: p.FirstSeen.SnapshotID, At: p.FirstSeen.At},
		LastSeen:     Stamp{SnapshotID: p.LastSeen.SnapshotID, At: p.LastSeen.At},
		LastChanged:  Stamp{SnapshotID: p.LastChanged.SnapshotID, At: p.LastChanged.At},
		Deleted:      del,
		Enrichments:  enrichments,
	}
}

// ValidateParquetInventoryReaderAt validates a physical DDR-0004 parquet-inventory/v0 member (including magic,
// footer metadata, schema elements, actual page headers, and bounded ZSTD decompression) before row decoding.
func ValidateParquetInventoryReaderAt(r io.ReaderAt, size int64, maxDecompressedBytes int64) (*parquet.File, error) {
	limit := DefaultMaxParquetDecompressedBytes
	if maxDecompressedBytes > 0 {
		limit = maxDecompressedBytes
	}
	if limit > MaxDecompressedMemberBytes {
		limit = MaxDecompressedMemberBytes
	}
	if size < 0 || size > limit {
		return nil, NewRefusal("capacity", "parquet member byte length exceeds limit")
	}
	if size < 8 {
		return nil, NewRefusal("invalid_shape", "invalid or truncated parquet magic bytes")
	}
	var head [4]byte
	var trailer [8]byte
	if _, err := r.ReadAt(head[:], 0); err != nil {
		return nil, NewRefusal("invalid_shape", "invalid or truncated parquet magic bytes")
	}
	if _, err := r.ReadAt(trailer[:], size-8); err != nil {
		return nil, NewRefusal("invalid_shape", "invalid or truncated parquet magic bytes")
	}
	if string(head[:]) != "PAR1" || string(trailer[4:8]) != "PAR1" {
		return nil, NewRefusal("invalid_shape", "invalid or truncated parquet magic bytes")
	}
	footerLen := int64(binary.LittleEndian.Uint32(trailer[:4]))
	if footerLen <= 0 || footerLen > size-8 {
		return nil, NewRefusal("invalid_shape", "parquet footer length out of file bounds")
	}
	if footerLen > limit || footerLen > MaxMetadataBytes {
		return nil, NewRefusal("capacity", "parquet footer length exceeds member or metadata ceiling")
	}
	dataEnd := size - 8 - footerLen

	pf, err := safeOpenParquetFile(r, size)
	if err != nil {
		return nil, err
	}
	meta := pf.Metadata()
	if meta == nil {
		return nil, NewRefusal("invalid_shape", "missing parquet file metadata")
	}
	if meta.EncryptionAlgorithm.Value != nil || len(meta.FooterSigningKeyMetadata) > 0 {
		return nil, NewRefusal("graph_profile", "encrypted parquet files are unsupported in parquet-inventory/v0")
	}
	if err := validateDDR0004SchemaElements(meta.Schema); err != nil {
		return nil, err
	}
	if meta.NumRows < 0 || meta.NumRows > MaxExactInteger {
		return nil, NewRefusal("capacity", "invalid parquet num_rows")
	}

	var totalDeclaredUncompressed int64
	var totalActualDecompressed int64
	var totalRowGroupRows int64
	for _, rg := range meta.RowGroups {
		if rg.NumRows < 0 {
			return nil, NewRefusal("invalid_shape", "negative row group row count")
		}
		if totalRowGroupRows > MaxExactInteger-rg.NumRows {
			return nil, NewRefusal("capacity", "row group row count overflow")
		}
		totalRowGroupRows += rg.NumRows
		if len(rg.Columns) != 21 {
			return nil, NewRefusal("invalid_shape", fmt.Sprintf("expected 21 leaf columns in row group, got %d", len(rg.Columns)))
		}
		for _, col := range rg.Columns {
			if col.FilePath != "" {
				return nil, NewRefusal("graph_profile", "external column chunks are unsupported in parquet-inventory/v0")
			}
			if col.CryptoMetadata.Value != nil || len(col.EncryptedColumnMetadata) > 0 {
				return nil, NewRefusal("graph_profile", "encrypted column chunks are unsupported in parquet-inventory/v0")
			}
			cmd := col.MetaData
			if cmd.Codec != format.Uncompressed && cmd.Codec != format.Zstd {
				return nil, NewRefusal("graph_profile", fmt.Sprintf("unsupported parquet compression codec %s", cmd.Codec))
			}
			if cmd.TotalUncompressedSize < 0 || cmd.TotalCompressedSize < 0 || cmd.NumValues < 0 {
				return nil, NewRefusal("invalid_shape", "negative column chunk size or num_values in metadata")
			}
			if cmd.NumValues > limit {
				return nil, NewRefusal("capacity", "column chunk num_values exceeds member decompression ceiling")
			}
			if err := validateOptionalParquetRange(col.ColumnIndexOffset, int64(col.ColumnIndexLength), dataEnd); err != nil {
				return nil, err
			}
			if err := validateOptionalParquetRange(col.OffsetIndexOffset, int64(col.OffsetIndexLength), dataEnd); err != nil {
				return nil, err
			}
			if err := validateOptionalParquetRange(cmd.BloomFilterOffset, int64(cmd.BloomFilterLength), dataEnd); err != nil {
				return nil, err
			}
			if totalDeclaredUncompressed > limit-cmd.TotalUncompressedSize {
				return nil, NewRefusal("capacity", "parquet decompressed column size exceeds member decompression ceiling")
			}
			totalDeclaredUncompressed += cmd.TotalUncompressedSize
			for _, enc := range cmd.Encoding {
				if !isSupportedDDR0004Encoding(enc) {
					return nil, NewRefusal("graph_profile", fmt.Sprintf("unsupported parquet column encoding %s", enc))
				}
			}
			for _, es := range cmd.EncodingStats {
				if !isSupportedDDR0004PageType(es.PageType) {
					return nil, NewRefusal("graph_profile", fmt.Sprintf("unsupported parquet page type %s", es.PageType))
				}
				if !isSupportedDDR0004Encoding(es.Encoding) {
					return nil, NewRefusal("graph_profile", fmt.Sprintf("unsupported parquet page encoding %s", es.Encoding))
				}
			}

			actualChunkDecompressed, err := validateAndMeasureColumnChunkPages(r, dataEnd, cmd, limit-totalActualDecompressed)
			if err != nil {
				return nil, err
			}
			totalActualDecompressed += actualChunkDecompressed
		}
	}
	if totalRowGroupRows != meta.NumRows {
		return nil, NewRefusal("graph_count", "parquet row group row count sum disagrees with file metadata num_rows")
	}
	if meta.NumRows > 0 && (meta.NumRows > totalActualDecompressed || meta.NumRows > limit) {
		return nil, NewRefusal("capacity", "parquet num_rows exceeds decompressed payload bounds")
	}
	return pf, nil
}

func validateOptionalParquetRange(offset, length, dataEnd int64) error {
	if offset < 0 || length < 0 {
		return NewRefusal("invalid_shape", "negative parquet index or bloom filter offset/length")
	}
	if offset == 0 && length == 0 {
		return nil
	}
	if offset < 4 || offset > dataEnd || length > dataEnd-offset {
		return NewRefusal("invalid_shape", "parquet index or bloom filter range out of file bounds")
	}
	return nil
}

func safeOpenParquetFile(r io.ReaderAt, size int64) (pf *parquet.File, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			pf = nil
			err = NewRefusal("invalid_shape", fmt.Sprintf("malformed parquet file metadata: %v", rec))
		}
	}()
	f, openErr := parquet.OpenFile(r, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if openErr != nil {
		return nil, NewRefusal("invalid_shape", fmt.Sprintf("open parquet member: %v", openErr))
	}
	return f, nil
}

type countingByteReader struct {
	r *bufio.Reader
	n int64
}

func (c *countingByteReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingByteReader) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err == nil {
		c.n++
	}
	return b, err
}

func validateAndMeasureColumnChunkPages(r io.ReaderAt, dataEnd int64, cmd format.ColumnMetaData, remainingBudget int64) (int64, error) {
	if cmd.TotalCompressedSize == 0 {
		if cmd.TotalUncompressedSize != 0 || cmd.NumValues != 0 {
			return 0, NewRefusal("invalid_shape", "empty column chunk has non-zero uncompressed size or num_values")
		}
		return 0, nil
	}
	chunkOffset := cmd.DataPageOffset
	if cmd.DictionaryPageOffset > 0 && cmd.DictionaryPageOffset < chunkOffset {
		chunkOffset = cmd.DictionaryPageOffset
	}
	if chunkOffset < 4 || cmd.TotalCompressedSize < 0 || chunkOffset > dataEnd || cmd.TotalCompressedSize > dataEnd-chunkOffset {
		return 0, NewRefusal("invalid_shape", "parquet column chunk offset or size out of bounds")
	}

	sec := io.NewSectionReader(r, chunkOffset, cmd.TotalCompressedSize)
	cr := &countingByteReader{r: bufio.NewReader(sec)}
	var compact thrift.CompactProtocol

	var chunkUncompressedWithHeaders int64
	var chunkUncompressedPagesOnly int64
	var totalDataPageValues int64
	var hasDictPage bool
	var dictValCount int64

	for cr.n < cmd.TotalCompressedSize {
		pageStart := cr.n
		var header format.PageHeader
		dec := thrift.NewDecoder(compact.NewReader(cr))
		if err := dec.Decode(&header); err != nil {
			return 0, NewRefusal("invalid_shape", fmt.Sprintf("decode parquet page header: %v", err))
		}
		headerBytes := cr.n - pageStart
		if header.CompressedPageSize < 0 || header.UncompressedPageSize < 0 {
			return 0, NewRefusal("invalid_shape", "negative page size in parquet page header")
		}
		compSize := int64(header.CompressedPageSize)
		uncompSize := int64(header.UncompressedPageSize)
		if cr.n+compSize > cmd.TotalCompressedSize {
			return 0, NewRefusal("invalid_shape", "parquet page extends past column chunk boundary")
		}

		var v2LevelsLen int64
		v2IsCompressed := true
		switch header.Type {
		case format.DictionaryPage:
			if !header.DictionaryPageHeader.Valid || header.DictionaryPageHeader.V.NumValues < 0 {
				return 0, NewRefusal("invalid_shape", "invalid dictionary page header")
			}
			if !isSupportedDDR0004DictEncoding(header.DictionaryPageHeader.V.Encoding) {
				return 0, NewRefusal("graph_profile", fmt.Sprintf("unsupported dictionary page encoding %s", header.DictionaryPageHeader.V.Encoding))
			}
			dictVals := int64(header.DictionaryPageHeader.V.NumValues)
			if dictVals > cmd.NumValues || dictVals*minPlainValueBytes(cmd.Type) > uncompSize {
				return 0, NewRefusal("invalid_shape", "dictionary page num_values exceeds column chunk or uncompressed page size")
			}
			hasDictPage = true
			dictValCount = dictVals
		case format.DataPage:
			if !header.DataPageHeader.Valid || header.DataPageHeader.V.NumValues < 0 {
				return 0, NewRefusal("invalid_shape", "invalid data page v1 header")
			}
			dph := header.DataPageHeader.V
			if !isSupportedDDR0004ValueEncoding(dph.Encoding) {
				return 0, NewRefusal("graph_profile", fmt.Sprintf("unsupported data page v1 value encoding %s", dph.Encoding))
			}
			if !isSupportedDDR0004LevelEncoding(dph.DefinitionLevelEncoding) || !isSupportedDDR0004LevelEncoding(dph.RepetitionLevelEncoding) {
				return 0, NewRefusal("graph_profile", "unsupported data page v1 level encoding")
			}
			pageVals := int64(dph.NumValues)
			if pageVals > cmd.NumValues-totalDataPageValues || pageVals > remainingBudget {
				return 0, NewRefusal("invalid_shape", "data page v1 num_values exceeds column chunk bounds")
			}
			if err := validateDataPageV1HeaderUncompBounds(cmd, dph, pageVals, uncompSize); err != nil {
				return 0, err
			}
			totalDataPageValues += pageVals
		case format.DataPageV2:
			if !header.DataPageHeaderV2.Valid {
				return 0, NewRefusal("invalid_shape", "invalid data page v2 header")
			}
			dph2 := header.DataPageHeaderV2.V
			if dph2.NumValues < 0 || dph2.NumNulls < 0 || dph2.NumRows < 0 ||
				dph2.NumNulls > dph2.NumValues || dph2.NumRows > dph2.NumValues ||
				dph2.DefinitionLevelsByteLength < 0 || dph2.RepetitionLevelsByteLength < 0 {
				return 0, NewRefusal("invalid_shape", "invalid counts or level lengths in data page v2 header")
			}
			if !isSupportedDDR0004ValueEncoding(dph2.Encoding) {
				return 0, NewRefusal("graph_profile", fmt.Sprintf("unsupported data page v2 value encoding %s", dph2.Encoding))
			}
			pageVals := int64(dph2.NumValues)
			if pageVals > cmd.NumValues-totalDataPageValues || pageVals > remainingBudget {
				return 0, NewRefusal("invalid_shape", "data page v2 num_values exceeds column chunk bounds")
			}
			v2LevelsLen = int64(dph2.DefinitionLevelsByteLength) + int64(dph2.RepetitionLevelsByteLength)
			if v2LevelsLen > compSize || v2LevelsLen > uncompSize {
				return 0, NewRefusal("invalid_shape", "data page v2 level byte lengths exceed page size")
			}
			if err := validateDataPageV2HeaderUncompBounds(cmd, dph2, pageVals, uncompSize); err != nil {
				return 0, err
			}
			totalDataPageValues += pageVals
			if dph2.IsCompressed.Valid {
				v2IsCompressed = dph2.IsCompressed.V
			}
		default:
			return 0, NewRefusal("graph_profile", fmt.Sprintf("unsupported parquet page type %s", header.Type))
		}

		if chunkUncompressedWithHeaders+headerBytes > remainingBudget {
			return 0, NewRefusal("capacity", "parquet decompressed page size exceeds member decompression ceiling")
		}
		pageBudget := remainingBudget - (chunkUncompressedWithHeaders + headerBytes)
		if uncompSize > pageBudget {
			return 0, NewRefusal("capacity", "parquet decompressed page size exceeds member decompression ceiling")
		}

		payload := make([]byte, compSize)
		if _, err := io.ReadFull(cr, payload); err != nil {
			return 0, NewRefusal("invalid_shape", fmt.Sprintf("read parquet page payload: %v", err))
		}

		var uncompData []byte
		var actualPageUncomp int64
		switch cmd.Codec {
		case format.Uncompressed:
			if compSize != uncompSize {
				return 0, NewRefusal("invalid_shape", "uncompressed parquet page size mismatch")
			}
			uncompData = payload
			actualPageUncomp = compSize
		case format.Zstd:
			if header.Type == format.DataPageV2 {
				compValues := payload[v2LevelsLen:]
				if !v2IsCompressed || (len(compValues) == 0 && uncompSize == v2LevelsLen) {
					uncompData = payload
					actualPageUncomp = int64(len(payload))
				} else {
					decValues, err := decompressBoundedZstd(compValues, pageBudget-v2LevelsLen)
					if err != nil {
						return 0, err
					}
					uncompData = make([]byte, int(v2LevelsLen)+len(decValues))
					copy(uncompData, payload[:v2LevelsLen])
					copy(uncompData[v2LevelsLen:], decValues)
					actualPageUncomp = v2LevelsLen + int64(len(decValues))
				}
			} else if len(payload) == 0 && uncompSize == 0 {
				uncompData = nil
				actualPageUncomp = 0
			} else {
				decBuf, err := decompressBoundedZstd(payload, pageBudget)
				if err != nil {
					return 0, err
				}
				uncompData = decBuf
				actualPageUncomp = int64(len(decBuf))
			}
			if actualPageUncomp != uncompSize {
				return 0, NewRefusal("invalid_shape", "parquet page decompressed byte length disagrees with page header")
			}
		default:
			return 0, NewRefusal("graph_profile", fmt.Sprintf("unsupported parquet compression codec %s", cmd.Codec))
		}

		if err := validateDecompressedPagePayload(cmd, header, uncompData, hasDictPage, dictValCount); err != nil {
			return 0, err
		}

		chunkUncompressedWithHeaders += headerBytes + actualPageUncomp
		chunkUncompressedPagesOnly += actualPageUncomp
	}

	if totalDataPageValues != cmd.NumValues {
		return 0, NewRefusal("invalid_shape", "column chunk num_values disagrees with data page headers")
	}
	if cmd.TotalUncompressedSize != chunkUncompressedWithHeaders && cmd.TotalUncompressedSize != chunkUncompressedPagesOnly {
		return 0, NewRefusal("invalid_shape", "column chunk total_uncompressed_size disagrees with page headers")
	}
	return chunkUncompressedWithHeaders, nil
}

func minPlainValueBytes(t format.Type) int64 {
	if t == format.Int64 {
		return 8
	}
	return 4
}

func ddr0004ColumnLevels(path []string) (hasRep bool, hasDef bool, maxDef int64) {
	if len(path) == 0 {
		return false, false, 0
	}
	switch path[0] {
	case "enrichments":
		if len(path) == 4 && path[3] == "value" {
			return true, true, 2
		}
		return true, true, 1
	case "revision", "last_modified", "storage_class", "deleted":
		return false, true, 1
	default:
		return false, false, 0
	}
}

func validateDataPageV1HeaderUncompBounds(cmd format.ColumnMetaData, dph format.DataPageHeader, pageVals, uncompSize int64) error {
	if pageVals <= 0 {
		return nil
	}
	hasRep, hasDef, _ := ddr0004ColumnLevels(cmd.PathInSchema)
	var minBytes int64
	if hasRep {
		if dph.RepetitionLevelEncoding == format.BitPacked {
			minBytes += (pageVals + 7) / 8
		} else {
			minBytes += 6
		}
	}
	if hasDef {
		if dph.DefinitionLevelEncoding == format.BitPacked {
			minBytes += (pageVals + 7) / 8
		} else {
			minBytes += 6
		}
	}
	if !hasRep && !hasDef {
		if dph.Encoding == format.Plain {
			minBytes += pageVals * minPlainValueBytes(cmd.Type)
		} else {
			minBytes += 2
		}
	}
	if uncompSize < minBytes {
		return NewRefusal("invalid_shape", "data page v1 num_values exceeds uncompressed page size")
	}
	return nil
}

func validateDataPageV2HeaderUncompBounds(cmd format.ColumnMetaData, dph2 format.DataPageHeaderV2, pageVals, uncompSize int64) error {
	hasRep, hasDef, _ := ddr0004ColumnLevels(cmd.PathInSchema)
	if !hasDef && dph2.NumNulls > 0 {
		return NewRefusal("invalid_shape", "data page v2 declares nulls for required column")
	}
	if pageVals > 0 {
		if (hasRep && dph2.RepetitionLevelsByteLength < 2) || (!hasRep && dph2.RepetitionLevelsByteLength != 0) {
			return NewRefusal("invalid_shape", "data page v2 repetition level length invalid for column")
		}
		if (hasDef && dph2.DefinitionLevelsByteLength < 2) || (!hasDef && dph2.DefinitionLevelsByteLength != 0) {
			return NewRefusal("invalid_shape", "data page v2 definition level length invalid for column")
		}
	}
	nonNullVals := pageVals - int64(dph2.NumNulls)
	valUncomp := uncompSize - int64(dph2.DefinitionLevelsByteLength) - int64(dph2.RepetitionLevelsByteLength)
	if nonNullVals > 0 {
		if dph2.Encoding == format.Plain {
			if nonNullVals*minPlainValueBytes(cmd.Type) > valUncomp {
				return NewRefusal("invalid_shape", "data page v2 num_values exceeds uncompressed page size")
			}
		} else if valUncomp < 2 {
			return NewRefusal("invalid_shape", "data page v2 num_values exceeds uncompressed page size")
		}
	}
	return nil
}

func validateDecompressedPagePayload(cmd format.ColumnMetaData, header format.PageHeader, uncompData []byte, hasDictPage bool, dictValCount int64) error {
	switch header.Type {
	case format.DictionaryPage:
		dictVals := int64(header.DictionaryPageHeader.V.NumValues)
		return validatePlainValuesPayload(cmd.Type, dictVals, uncompData)
	case format.DataPage:
		dph := header.DataPageHeader.V
		pageVals := int64(dph.NumValues)
		if pageVals == 0 {
			return nil
		}
		hasRep, hasDef, maxDef := ddr0004ColumnLevels(cmd.PathInSchema)
		rem := uncompData
		if hasRep {
			var err error
			_, rem, err = consumeV1LevelStream(rem, dph.RepetitionLevelEncoding, 1, pageVals, 1)
			if err != nil {
				return err
			}
		}
		nonNullVals := pageVals
		if hasDef {
			var err error
			defBitWidth := int64(1)
			if maxDef > 1 {
				defBitWidth = 2
			}
			nonNullVals, rem, err = consumeV1LevelStream(rem, dph.DefinitionLevelEncoding, defBitWidth, pageVals, maxDef)
			if err != nil {
				return err
			}
		}
		return validateEncodedValuesPayload(cmd.Type, dph.Encoding, nonNullVals, rem, hasDictPage, dictValCount)
	case format.DataPageV2:
		dph2 := header.DataPageHeaderV2.V
		pageVals := int64(dph2.NumValues)
		if pageVals == 0 {
			return nil
		}
		hasRep, hasDef, maxDef := ddr0004ColumnLevels(cmd.PathInSchema)
		repLen := int64(dph2.RepetitionLevelsByteLength)
		defLen := int64(dph2.DefinitionLevelsByteLength)
		if hasRep {
			if _, err := validateRLEHybridStream(uncompData[:repLen], 1, pageVals, 1); err != nil {
				return err
			}
		}
		nonNullVals := pageVals
		if hasDef {
			defBitWidth := int64(1)
			if maxDef > 1 {
				defBitWidth = 2
			}
			matched, err := validateRLEHybridStream(uncompData[repLen:repLen+defLen], defBitWidth, pageVals, maxDef)
			if err != nil {
				return err
			}
			nonNullVals = matched
			if pageVals-nonNullVals != int64(dph2.NumNulls) {
				return NewRefusal("invalid_shape", "data page v2 num_nulls disagrees with definition levels")
			}
		}
		return validateEncodedValuesPayload(cmd.Type, dph2.Encoding, nonNullVals, uncompData[repLen+defLen:], hasDictPage, dictValCount)
	default:
		return nil
	}
}

func consumeV1LevelStream(data []byte, enc format.Encoding, bitWidth int64, expectedVals int64, maxVal int64) (int64, []byte, error) {
	if enc == format.BitPacked {
		byteLen := ((expectedVals * bitWidth) + 7) / 8
		if int64(len(data)) < byteLen {
			return 0, nil, NewRefusal("invalid_shape", "truncated bit-packed level stream in data page v1")
		}
		block := data[:byteLen]
		var matchMax int64
		var bitPos int64
		for v := int64(0); v < expectedVals; v++ {
			var val int64
			for b := int64(0); b < bitWidth; b++ {
				if (block[(bitPos+b)/8]>>((bitPos+b)%8))&1 != 0 {
					val |= 1 << b
				}
			}
			bitPos += bitWidth
			if val > maxVal {
				return 0, nil, NewRefusal("invalid_shape", "decoded bit-packed level exceeds maximum")
			}
			if val == maxVal {
				matchMax++
			}
		}
		return matchMax, data[byteLen:], nil
	}
	if len(data) < 4 {
		return 0, nil, NewRefusal("invalid_shape", "truncated rle level length prefix in data page v1")
	}
	n := int64(binary.LittleEndian.Uint32(data[:4]))
	if n <= 0 || int64(len(data)-4) < n {
		return 0, nil, NewRefusal("invalid_shape", "invalid rle level length prefix in data page v1")
	}
	matchMax, err := validateRLEHybridStream(data[4:4+n], bitWidth, expectedVals, maxVal)
	if err != nil {
		return 0, nil, err
	}
	return matchMax, data[4+n:], nil
}

func validateRLEHybridStream(src []byte, bitWidth int64, expectedVals int64, maxVal int64) (int64, error) {
	if bitWidth < 0 || bitWidth > 32 {
		return 0, NewRefusal("invalid_shape", "rle bit width exceeds 32")
	}
	var decoded int64
	var matchMax int64
	for i := 0; i < len(src); {
		u, n := binary.Uvarint(src[i:])
		if n <= 0 {
			return 0, NewRefusal("invalid_shape", "invalid rle block header")
		}
		i += n
		count := int64(u >> 1)
		bitpacked := (u & 1) != 0
		if count <= 0 || count > 0x7fffffff {
			return 0, NewRefusal("invalid_shape", "invalid rle run count")
		}
		if bitpacked {
			numVals := count * 8
			byteLen := count * bitWidth
			if int64(len(src)-i) < byteLen {
				return 0, NewRefusal("invalid_shape", "truncated bit-packed run")
			}
			if decoded+numVals > expectedVals+7 {
				return 0, NewRefusal("invalid_shape", "bit-packed run exceeds page num_values")
			}
			block := src[i : i+int(byteLen)]
			validInBlock := numVals
			if decoded+validInBlock > expectedVals {
				validInBlock = expectedVals - decoded
			}
			if bitWidth == 0 {
				if maxVal == 0 {
					matchMax += validInBlock
				}
			} else {
				var bitPos int64
				for v := int64(0); v < validInBlock; v++ {
					var val int64
					for b := int64(0); b < bitWidth; b++ {
						if (block[(bitPos+b)/8]>>((bitPos+b)%8))&1 != 0 {
							val |= 1 << b
						}
					}
					bitPos += bitWidth
					if val > maxVal {
						return 0, NewRefusal("invalid_shape", "decoded level or index exceeds maximum")
					}
					if val == maxVal {
						matchMax++
					}
				}
			}
			decoded += numVals
			i += int(byteLen)
		} else {
			valBytes := int((bitWidth + 7) / 8)
			if len(src)-i < valBytes {
				return 0, NewRefusal("invalid_shape", "truncated rle run")
			}
			if decoded+count > expectedVals {
				return 0, NewRefusal("invalid_shape", "rle run exceeds page num_values")
			}
			var val int64
			for b := 0; b < valBytes; b++ {
				val |= int64(src[i+b]) << (8 * b)
			}
			if val > maxVal {
				return 0, NewRefusal("invalid_shape", "rle value exceeds maximum")
			}
			if val == maxVal {
				matchMax += count
			}
			decoded += count
			i += valBytes
		}
	}
	if decoded < expectedVals {
		return 0, NewRefusal("invalid_shape", "rle stream decoded fewer values than page num_values")
	}
	return matchMax, nil
}

func validatePlainValuesPayload(colType format.Type, numVals int64, data []byte) error {
	if numVals*minPlainValueBytes(colType) > int64(len(data)) {
		return NewRefusal("invalid_shape", "plain value count exceeds uncompressed payload size")
	}
	if colType == format.Int64 {
		if int64(len(data)) != numVals*8 {
			return NewRefusal("invalid_shape", "int64 plain payload byte length mismatch")
		}
		return nil
	}
	rem := data
	for i := int64(0); i < numVals; i++ {
		if len(rem) < 4 {
			return NewRefusal("invalid_shape", "truncated byte_array length prefix in plain payload")
		}
		elemLen := int64(binary.LittleEndian.Uint32(rem[:4]))
		if elemLen < 0 || elemLen > int64(len(rem)-4) {
			return NewRefusal("invalid_shape", "byte_array length prefix exceeds remaining plain payload")
		}
		rem = rem[4+elemLen:]
	}
	if len(rem) != 0 {
		return NewRefusal("invalid_shape", "trailing bytes after plain byte_array values")
	}
	return nil
}

func validateEncodedValuesPayload(colType format.Type, enc format.Encoding, nonNullVals int64, data []byte, hasDictPage bool, dictValCount int64) error {
	if enc == format.Plain {
		return validatePlainValuesPayload(colType, nonNullVals, data)
	}
	if nonNullVals == 0 {
		return nil
	}
	if !hasDictPage || dictValCount <= 0 {
		return NewRefusal("invalid_shape", "dictionary-encoded data page without valid dictionary entries")
	}
	if len(data) < 2 {
		return NewRefusal("invalid_shape", "truncated dictionary index stream in data page")
	}
	bitWidth := int64(data[0])
	if bitWidth > 32 {
		return NewRefusal("invalid_shape", "dictionary index bit width exceeds 32")
	}
	_, err := validateRLEHybridStream(data[1:], bitWidth, nonNullVals, dictValCount-1)
	return err
}

func decompressBoundedZstd(compressed []byte, maxBytes int64) ([]byte, error) {
	if maxBytes < 0 {
		return nil, NewRefusal("capacity", "parquet decompressed page size exceeds member decompression ceiling")
	}
	zr, err := zstd.NewReader(bytes.NewReader(compressed), zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, NewRefusal("invalid_shape", fmt.Sprintf("init zstd page decoder: %v", err))
	}
	defer zr.Close()

	limited := io.LimitReader(zr, maxBytes+1)
	out, readErr := io.ReadAll(limited)
	if int64(len(out)) > maxBytes {
		return nil, NewRefusal("capacity", "parquet decompressed page size exceeds member decompression ceiling")
	}
	if readErr != nil {
		return nil, NewRefusal("invalid_shape", fmt.Sprintf("decompress zstd parquet page: %v", readErr))
	}
	return out, nil
}

// ReadParquetInventoryMember validates a physical DDR-0004 parquet-inventory/v0 member and decodes its logical rows.
func ReadParquetInventoryMember(data []byte, maxDecompressedBytes int64) (rows []LogicalRow, err error) {
	pf, err := ValidateParquetInventoryReaderAt(bytes.NewReader(data), int64(len(data)), maxDecompressedBytes)
	if err != nil {
		return nil, err
	}
	meta := pf.Metadata()

	defer func() {
		if rec := recover(); rec != nil {
			rows = nil
			err = NewRefusal("invalid_shape", fmt.Sprintf("decode parquet rows panic: %v", rec))
		}
	}()

	reader := parquet.NewGenericReader[parquetRowUnannotatedSize](pf)
	defer func() { _ = reader.Close() }()

	initCap := int(meta.NumRows)
	if initCap > 1024 {
		initCap = 1024
	}
	out := make([]LogicalRow, 0, initCap)
	batch := make([]parquetRowUnannotatedSize, 128)
	for {
		n, readErr := reader.Read(batch)
		for i := 0; i < n; i++ {
			out = append(out, fromParquetRowUnannotated(batch[i]))
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, NewRefusal("invalid_shape", fmt.Sprintf("decode parquet rows: %v", readErr))
		}
	}
	if int64(len(out)) != meta.NumRows {
		return nil, NewRefusal("graph_count", "decoded parquet row count disagrees with file metadata num_rows")
	}
	if err := ValidateRows(out, ""); err != nil {
		return nil, err
	}
	return out, nil
}

func isSupportedDDR0004Encoding(enc format.Encoding) bool {
	switch enc {
	//nolint:staticcheck // DDR-0004 explicitly enumerates PLAIN_DICTIONARY alongside RLE_DICTIONARY
	case format.Plain, format.PlainDictionary, format.RLE, format.BitPacked, format.RLEDictionary:
		return true
	default:
		return false
	}
}

func isSupportedDDR0004DictEncoding(enc format.Encoding) bool {
	switch enc {
	//nolint:staticcheck // DDR-0004 explicitly permits PLAIN and PLAIN_DICTIONARY on dictionary pages
	case format.Plain, format.PlainDictionary:
		return true
	default:
		return false
	}
}

func isSupportedDDR0004ValueEncoding(enc format.Encoding) bool {
	switch enc {
	//nolint:staticcheck // DDR-0004 explicitly permits PLAIN, PLAIN_DICTIONARY, and RLE_DICTIONARY for page values
	case format.Plain, format.PlainDictionary, format.RLEDictionary:
		return true
	default:
		return false
	}
}

func isSupportedDDR0004LevelEncoding(enc format.Encoding) bool {
	switch enc {
	case format.RLE, format.BitPacked:
		return true
	default:
		return false
	}
}

func isSupportedDDR0004PageType(pt format.PageType) bool {
	switch pt {
	case format.DataPage, format.DictionaryPage, format.DataPageV2:
		return true
	default:
		return false
	}
}

type expectedSchemaNode struct {
	name           string
	isGroup        bool
	repetition     format.FieldRepetitionType
	numChildren    int32
	physicalType   format.Type
	isString       bool
	isSizeBytesCol bool
	isListGroup    bool
}

var ddr0004ExpectedSchema = []expectedSchemaNode{
	{name: "namespace", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "key", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "revision", isGroup: true, repetition: format.Optional, numChildren: 2},
	{name: "kind", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "value", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "size_bytes", isGroup: false, repetition: format.Required, physicalType: format.Int64, isSizeBytesCol: true},
	{name: "last_modified", isGroup: false, repetition: format.Optional, physicalType: format.ByteArray, isString: true},
	{name: "storage_class", isGroup: false, repetition: format.Optional, physicalType: format.ByteArray, isString: true},
	{name: "first_seen", isGroup: true, repetition: format.Required, numChildren: 2},
	{name: "snapshot_id", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "at", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "last_seen", isGroup: true, repetition: format.Required, numChildren: 2},
	{name: "snapshot_id", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "at", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "last_changed", isGroup: true, repetition: format.Required, numChildren: 2},
	{name: "snapshot_id", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "at", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "deleted", isGroup: true, repetition: format.Optional, numChildren: 2},
	{name: "snapshot_id", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "at", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "enrichments", isGroup: true, repetition: format.Required, numChildren: 1, isListGroup: true},
	{name: "list", isGroup: true, repetition: format.Repeated, numChildren: 1},
	{name: "element", isGroup: true, repetition: format.Required, numChildren: 5},
	{name: "name", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "value", isGroup: false, repetition: format.Optional, physicalType: format.ByteArray, isString: true},
	{name: "operation", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "observed_at", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "against_revision", isGroup: true, repetition: format.Required, numChildren: 2},
	{name: "kind", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
	{name: "value", isGroup: false, repetition: format.Required, physicalType: format.ByteArray, isString: true},
}

func validateDDR0004SchemaElements(elements []format.SchemaElement) error {
	if len(elements) != len(ddr0004ExpectedSchema)+1 {
		return NewRefusal("invalid_shape", fmt.Sprintf("parquet schema element count %d != %d", len(elements), len(ddr0004ExpectedSchema)+1))
	}
	root := elements[0]
	if root.Type.Valid || !root.NumChildren.Valid || root.NumChildren.V != 11 {
		return NewRefusal("invalid_shape", "parquet root schema element must be a group with 11 children")
	}
	if root.LogicalType.Value != nil || root.ConvertedType.Valid {
		return NewRefusal("invalid_shape", "parquet root schema element must not carry logical annotations")
	}
	for i, want := range ddr0004ExpectedSchema {
		got := elements[i+1]
		if got.Name != want.name {
			return NewRefusal("invalid_shape", fmt.Sprintf("parquet schema element[%d] name %q != %q", i+1, got.Name, want.name))
		}
		if !got.RepetitionType.Valid || got.RepetitionType.V != want.repetition {
			return NewRefusal("invalid_shape", fmt.Sprintf("parquet schema element %q repetition mismatch", want.name))
		}
		if want.isGroup {
			if got.Type.Valid || !got.NumChildren.Valid || got.NumChildren.V != want.numChildren {
				return NewRefusal("invalid_shape", fmt.Sprintf("parquet group %q children mismatch", want.name))
			}
			if want.isListGroup {
				hasLogical := got.LogicalType.Value != nil
				if hasLogical {
					if _, ok := got.LogicalType.Value.(*format.ListType); !ok {
						return NewRefusal("invalid_shape", "enrichments group logical annotation must be LIST")
					}
				}
				hasConv := got.ConvertedType.Valid
				if hasConv && got.ConvertedType.V != deprecated.List {
					return NewRefusal("invalid_shape", "enrichments group converted_type must be LIST")
				}
				if !hasLogical && !hasConv {
					return NewRefusal("invalid_shape", "enrichments group must carry LIST logical annotation")
				}
			} else if got.LogicalType.Value != nil || got.ConvertedType.Valid {
				return NewRefusal("invalid_shape", fmt.Sprintf("unexpected logical annotation on group %q", want.name))
			}
			continue
		}
		if !got.Type.Valid || got.Type.V != want.physicalType || got.NumChildren.Valid {
			return NewRefusal("invalid_shape", fmt.Sprintf("parquet leaf %q physical type mismatch", want.name))
		}
		if want.isString {
			hasLogical := got.LogicalType.Value != nil
			if hasLogical {
				if _, ok := got.LogicalType.Value.(*format.StringType); !ok {
					return NewRefusal("invalid_shape", fmt.Sprintf("parquet leaf %q logical annotation must be STRING", want.name))
				}
			}
			hasConv := got.ConvertedType.Valid
			if hasConv && got.ConvertedType.V != deprecated.UTF8 {
				return NewRefusal("invalid_shape", fmt.Sprintf("parquet leaf %q converted_type must be UTF8", want.name))
			}
			if !hasLogical && !hasConv {
				return NewRefusal("invalid_shape", fmt.Sprintf("parquet leaf %q must carry STRING/UTF8 logical annotation", want.name))
			}
		} else if want.isSizeBytesCol {
			if got.LogicalType.Value != nil {
				intType, ok := got.LogicalType.Value.(*format.IntType)
				if !ok || intType.BitWidth != 64 || !intType.IsSigned {
					return NewRefusal("invalid_shape", "size_bytes logical annotation must be absent or INT(64, signed=true)")
				}
			}
			if got.ConvertedType.Valid && got.ConvertedType.V != deprecated.Int64 {
				return NewRefusal("invalid_shape", "size_bytes converted_type must be absent or INT_64")
			}
		}
	}
	return nil
}

// SliceDTwinArtifact describes one deterministic physical Parquet twin member or negative fixture.
type SliceDTwinArtifact struct {
	ID                  string              `json:"id"`
	Codec               ParquetCodec        `json:"codec,omitempty"`
	DataPageVersion     int                 `json:"data_page_version,omitempty"`
	SizeBytesAnnotation SizeBytesAnnotation `json:"size_bytes_annotation,omitempty"`
	ParquetBytes        []byte              `json:"-"`
	ParquetSHA256       string              `json:"parquet_sha256"`
	ParquetByteLength   int64               `json:"parquet_byte_length"`
	LogicalRows         []LogicalRow        `json:"-"`
	LogicalNDJSON       []byte              `json:"-"`
	LogicalNDJSONSHA256 string              `json:"logical_ndjson_sha256,omitempty"`
	LogicalNDJSONBytes  int64               `json:"logical_ndjson_byte_length,omitempty"`
	RowCount            int64               `json:"row_count"`
	ExpectedRefusal     string              `json:"expected_refusal,omitempty"`
}

// EncodeLogicalRowsNDJSON serializes logical rows into deterministic UTF-8 NDJSON matching Crucible's twin format.
func EncodeLogicalRowsNDJSON(rows []LogicalRow) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func mutateParquetFooter(baseBytes []byte, mutate func(meta *format.FileMetaData)) ([]byte, error) {
	footerLen := int(binary.LittleEndian.Uint32(baseBytes[len(baseBytes)-8 : len(baseBytes)-4]))
	footerRaw := baseBytes[len(baseBytes)-8-footerLen : len(baseBytes)-8]
	var meta format.FileMetaData
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), footerRaw, &meta); err != nil {
		return nil, err
	}
	mutate(&meta)
	newFooterRaw, err := thrift.Marshal(new(thrift.CompactProtocol), &meta)
	if err != nil {
		return nil, err
	}
	if uint64(len(newFooterRaw)) > uint64(^uint32(0)) {
		return nil, errors.New("parquet footer length overflow")
	}
	out := make([]byte, 0, len(baseBytes)-footerLen+len(newFooterRaw))
	out = append(out, baseBytes[:len(baseBytes)-8-footerLen]...)
	out = append(out, newFooterRaw...)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(newFooterRaw))) // #nosec G115 -- bounded above by ^uint32(0)
	out = append(out, lenBuf[:]...)
	out = append(out, []byte("PAR1")...)
	return out, nil
}

// GenerateSliceDParquetTwins generates the complete deterministic DDR-0004 physical Parquet twin suite (3 positive + 20 negative).
func GenerateSliceDParquetTwins() ([]SliceDTwinArtifact, error) {
	strPtr := func(s string) *string { return &s }
	richRows := []LogicalRow{
		{
			Namespace:    "objects",
			Key:          "alpha/.hidden/zero",
			Revision:     nil,
			SizeBytes:    0,
			LastModified: nil,
			StorageClass: nil,
			FirstSeen:    Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
			LastSeen:     Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
			LastChanged:  Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
			Deleted:      nil,
			Enrichments:  []EnrichmentAttribute{},
		},
		{
			Namespace:    "objects",
			Key:          "alpha/max-int-enriched",
			Revision:     &Revision{Kind: "opaque-etag", Value: "rev-max-1"},
			SizeBytes:    MaxExactInteger,
			LastModified: strPtr("2026-10-18T00:00:00.123456789Z"),
			StorageClass: strPtr("STANDARD"),
			FirstSeen:    Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
			LastSeen:     Stamp{SnapshotID: "snapshot-1", At: "2026-10-18T01:00:00.000000001Z"},
			LastChanged:  Stamp{SnapshotID: "snapshot-1", At: "2026-10-18T01:00:00.000000001Z"},
			Deleted:      nil,
			Enrichments: []EnrichmentAttribute{
				{
					Name:            "content_encoding",
					Value:           nil,
					Operation:       "head-object",
					ObservedAt:      "2026-10-18T01:05:00.000000009Z",
					AgainstRevision: Revision{Kind: "opaque-etag", Value: "rev-max-1"},
				},
				{
					Name:            "content_type",
					Value:           strPtr("application/octet-stream"),
					Operation:       "head-object",
					ObservedAt:      "2026-10-18T01:05:00.000000009Z",
					AgainstRevision: Revision{Kind: "opaque-etag", Value: "rev-max-1"},
				},
			},
		},
		{
			Namespace:    "objects",
			Key:          "beta/tombstoned-unicode-λ",
			Revision:     &Revision{Kind: "opaque-etag", Value: "rev-old"},
			SizeBytes:    4096,
			LastModified: strPtr("2026-10-17T23:59:59.999999999Z"),
			StorageClass: strPtr("GLACIER"),
			FirstSeen:    Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
			LastSeen:     Stamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
			LastChanged:  Stamp{SnapshotID: "snapshot-1", At: "2026-10-18T01:00:00.000000001Z"},
			Deleted:      &Stamp{SnapshotID: "snapshot-1", At: "2026-10-18T01:00:00.000000001Z"},
			Enrichments:  []EnrichmentAttribute{},
		},
	}

	buildPositive := func(id string, rows []LogicalRow, opts ParquetWriteOptions) (SliceDTwinArtifact, error) {
		pBytes, err := WriteParquetInventoryMember(rows, opts)
		if err != nil {
			return SliceDTwinArtifact{}, err
		}
		ndjsonBytes, err := EncodeLogicalRowsNDJSON(rows)
		if err != nil {
			return SliceDTwinArtifact{}, err
		}
		pSum := sha256.Sum256(pBytes)
		lSum := sha256.Sum256(ndjsonBytes)
		return SliceDTwinArtifact{
			ID:                  id,
			Codec:               opts.Codec,
			DataPageVersion:     opts.DataPageVersion,
			SizeBytesAnnotation: opts.SizeBytesAnnotation,
			ParquetBytes:        pBytes,
			ParquetSHA256:       hex.EncodeToString(pSum[:]),
			ParquetByteLength:   int64(len(pBytes)),
			LogicalRows:         rows,
			LogicalNDJSON:       ndjsonBytes,
			LogicalNDJSONSHA256: hex.EncodeToString(lSum[:]),
			LogicalNDJSONBytes:  int64(len(ndjsonBytes)),
			RowCount:            int64(len(rows)),
		}, nil
	}

	addNegative := func(list *[]SliceDTwinArtifact, id, refusal string, raw []byte) {
		sum := sha256.Sum256(raw)
		*list = append(*list, SliceDTwinArtifact{
			ID:                id,
			ParquetBytes:      raw,
			ParquetSHA256:     hex.EncodeToString(sum[:]),
			ParquetByteLength: int64(len(raw)),
			ExpectedRefusal:   refusal,
		})
	}

	positiveSpecs := []struct {
		id   string
		rows []LogicalRow
		opts ParquetWriteOptions
	}{
		{
			id:   "twin-zstd-v2-int64-signed",
			rows: richRows,
			opts: ParquetWriteOptions{
				Codec:               ParquetCodecZSTD,
				DataPageVersion:     2,
				SizeBytesAnnotation: SizeBytesAnnotationInt64Signed,
			},
		},
		{
			id:   "twin-zstd-v1-unannotated-size",
			rows: richRows,
			opts: ParquetWriteOptions{
				Codec:               ParquetCodecZSTD,
				DataPageVersion:     1,
				SizeBytesAnnotation: SizeBytesAnnotationAbsent,
			},
		},
		{
			id:   "twin-uncompressed-v1-int64-signed-multi-rg",
			rows: richRows,
			opts: ParquetWriteOptions{
				Codec:               ParquetCodecUncompressed,
				DataPageVersion:     1,
				SizeBytesAnnotation: SizeBytesAnnotationInt64Signed,
				MaxRowsPerRowGroup:  1,
			},
		},
	}

	var out []SliceDTwinArtifact
	for _, spec := range positiveSpecs {
		art, err := buildPositive(spec.id, spec.rows, spec.opts)
		if err != nil {
			return nil, err
		}
		out = append(out, art)
	}
	baseBytes := out[0].ParquetBytes

	// Negative 1: truncated Parquet file.
	addNegative(&out, "reject-parquet-truncated", "invalid_shape", append([]byte(nil), baseBytes[:len(baseBytes)/2]...))

	// Negative 2: corrupted trailing PAR1 magic bytes.
	badMagic := append([]byte(nil), baseBytes...)
	badMagic[len(badMagic)-1] = '0'
	addNegative(&out, "reject-parquet-bad-magic", "invalid_shape", badMagic)

	// Negative 3: row group with 0 columns (tests panic recovery and column-count check).
	emptyRGCols, err := mutateParquetFooter(baseBytes, func(meta *format.FileMetaData) {
		meta.RowGroups[0].Columns = nil
	})
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-empty-row-group-columns", "invalid_shape", emptyRGCols)

	// Negative 4: unsupported Snappy codec.
	var snappyBuf bytes.Buffer
	snappyW := parquet.NewGenericWriter[parquetRowAnnotatedSize](
		&snappyBuf,
		parquetSchemaAnnotated,
		parquet.Compression(&parquet.Snappy),
	)
	if _, err := snappyW.Write([]parquetRowAnnotatedSize{toParquetRowAnnotated(richRows[0])}); err != nil {
		return nil, err
	}
	if err := snappyW.Close(); err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-unsupported-codec-snappy", "graph_profile", snappyBuf.Bytes())

	// Negative 5: unsigned UINT(64) annotation on size_bytes.
	type badUnsignedSizeRow struct {
		Namespace    string              `parquet:"namespace"`
		Key          string              `parquet:"key"`
		Revision     *parquetRevision    `parquet:"revision,optional"`
		SizeBytes    uint64              `parquet:"size_bytes,uint(64)"`
		LastModified *string             `parquet:"last_modified,optional"`
		StorageClass *string             `parquet:"storage_class,optional"`
		FirstSeen    parquetStamp        `parquet:"first_seen"`
		LastSeen     parquetStamp        `parquet:"last_seen"`
		LastChanged  parquetStamp        `parquet:"last_changed"`
		Deleted      *parquetStamp       `parquet:"deleted,optional"`
		Enrichments  []parquetEnrichment `parquet:"enrichments,list"`
	}
	var uintBuf bytes.Buffer
	uintW := parquet.NewGenericWriter[badUnsignedSizeRow](
		&uintBuf,
		parquet.SchemaOf(new(badUnsignedSizeRow)),
		parquet.Compression(&parquet.Uncompressed),
	)
	if _, err := uintW.Write([]badUnsignedSizeRow{{
		Namespace:   "objects",
		Key:         "alpha/one",
		SizeBytes:   12,
		FirstSeen:   parquetStamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
		LastSeen:    parquetStamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
		LastChanged: parquetStamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
		Enrichments: []parquetEnrichment{},
	}}); err != nil {
		return nil, err
	}
	if err := uintW.Close(); err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-unsigned-size-bytes", "invalid_shape", uintBuf.Bytes())

	// Negative 6: conflicting logical/converted annotation on string leaf (`LogicalType=STRING` + `ConvertedType=ENUM`).
	conflictAnnot, err := mutateParquetFooter(baseBytes, func(meta *format.FileMetaData) {
		meta.Schema[1].ConvertedType.Valid = true
		meta.Schema[1].ConvertedType.V = deprecated.Enum
	})
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-conflicting-string-annotation", "invalid_shape", conflictAnnot)

	// Negative 7: external column chunk file_path set.
	extCol, err := mutateParquetFooter(baseBytes, func(meta *format.FileMetaData) {
		meta.RowGroups[0].Columns[0].FilePath = "external.parquet"
	})
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-external-column-chunk", "graph_profile", extCol)

	// Negative 8: encrypted file metadata.
	encFooter, err := mutateParquetFooter(baseBytes, func(meta *format.FileMetaData) {
		meta.FooterSigningKeyMetadata = []byte("key")
	})
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-encrypted-footer", "graph_profile", encFooter)

	// Negative 9: encrypted column chunk metadata.
	encCol, err := mutateParquetFooter(baseBytes, func(meta *format.FileMetaData) {
		meta.RowGroups[0].Columns[0].EncryptedColumnMetadata = []byte("col-key")
	})
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-encrypted-column-chunk", "graph_profile", encCol)

	// Negative 10: unsupported DELTA_BYTE_ARRAY encoding.
	type badDeltaKeyRow struct {
		Namespace    string              `parquet:"namespace,plain"`
		Key          string              `parquet:"key,delta"`
		Revision     *parquetRevision    `parquet:"revision,optional"`
		SizeBytes    int64               `parquet:"size_bytes,int(64),plain"`
		LastModified *string             `parquet:"last_modified,optional,plain"`
		StorageClass *string             `parquet:"storage_class,optional,dict"`
		FirstSeen    parquetStamp        `parquet:"first_seen"`
		LastSeen     parquetStamp        `parquet:"last_seen"`
		LastChanged  parquetStamp        `parquet:"last_changed"`
		Deleted      *parquetStamp       `parquet:"deleted,optional"`
		Enrichments  []parquetEnrichment `parquet:"enrichments,list"`
	}
	var deltaBuf bytes.Buffer
	deltaW := parquet.NewGenericWriter[badDeltaKeyRow](
		&deltaBuf,
		parquet.SchemaOf(new(badDeltaKeyRow)),
		parquet.Compression(&parquet.Uncompressed),
	)
	if _, err := deltaW.Write([]badDeltaKeyRow{{
		Namespace:   "objects",
		Key:         "alpha/one",
		SizeBytes:   12,
		FirstSeen:   parquetStamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
		LastSeen:    parquetStamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
		LastChanged: parquetStamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:00.000000000Z"},
		Enrichments: []parquetEnrichment{},
	}}); err != nil {
		return nil, err
	}
	if err := deltaW.Close(); err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-unsupported-encoding-delta", "graph_profile", deltaBuf.Bytes())

	// Negative 11: forged footer Encoding/EncodingStats claiming PLAIN while actual page header uses DELTA_BYTE_ARRAY.
	forgedDeltaFooter, err := mutateParquetFooter(deltaBuf.Bytes(), func(meta *format.FileMetaData) {
		for i := range meta.RowGroups[0].Columns {
			meta.RowGroups[0].Columns[i].MetaData.Encoding = []format.Encoding{format.Plain, format.RLE}
			meta.RowGroups[0].Columns[i].MetaData.EncodingStats = nil
		}
	})
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-forged-footer-page-encoding", "graph_profile", forgedDeltaFooter)

	// Negative 12: forged footer TotalUncompressedSize disagreeing with actual decompressed page bytes.
	forgedUncomp, err := mutateParquetFooter(baseBytes, func(meta *format.FileMetaData) {
		meta.RowGroups[0].Columns[0].MetaData.TotalUncompressedSize = 1
	})
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-forged-uncompressed-size", "invalid_shape", forgedUncomp)

	// Negative 13: excessive NumRows claiming 1,000,000 rows on a tiny payload.
	excessiveRows, err := mutateParquetFooter(baseBytes, func(meta *format.FileMetaData) {
		meta.NumRows = 1_000_000
		meta.RowGroups[0].NumRows = 1_000_000
	})
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-excessive-num-rows", "capacity", excessiveRows)

	// Negative 14: row-group NumRows sum disagrees with file metadata NumRows.
	rgCountMismatch, err := mutateParquetFooter(baseBytes, func(meta *format.FileMetaData) {
		meta.RowGroups[0].NumRows = meta.NumRows + 1
	})
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-row-group-count-mismatch", "graph_count", rgCountMismatch)

	writeRawAnnotatedRows := func(rows []parquetRowAnnotatedSize, maxRowsPerRG int64) ([]byte, error) {
		var b bytes.Buffer
		wOpts := []parquet.WriterOption{
			parquetSchemaAnnotated,
			parquet.Compression(&parquet.Zstd),
		}
		if maxRowsPerRG > 0 {
			wOpts = append(wOpts, parquet.MaxRowsPerRowGroup(maxRowsPerRG))
		}
		w := parquet.NewGenericWriter[parquetRowAnnotatedSize](&b, wOpts...)
		if _, err := w.Write(rows); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	}

	// Negative 15: out-of-order primary keys inside Parquet member.
	unorderedBytes, err := writeRawAnnotatedRows([]parquetRowAnnotatedSize{
		toParquetRowAnnotated(richRows[2]),
		toParquetRowAnnotated(richRows[0]),
	}, 0)
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-row-order", "row_order", unorderedBytes)

	// Negative 16: duplicate primary key across row groups.
	dupAcrossRGBytes, err := writeRawAnnotatedRows([]parquetRowAnnotatedSize{
		toParquetRowAnnotated(richRows[0]),
		toParquetRowAnnotated(richRows[0]),
	}, 1)
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-duplicate-key-across-row-groups", "duplicate_key_row", dupAcrossRGBytes)

	// Negative 17: negative size_bytes.
	negSizeRow := toParquetRowAnnotated(richRows[0])
	negSizeRow.SizeBytes = -1
	negSizeBytes, err := writeRawAnnotatedRows([]parquetRowAnnotatedSize{negSizeRow}, 0)
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-negative-size-bytes", "invalid_shape", negSizeBytes)

	// Negative 18: tombstoned row carrying enrichments.
	tombEnrichRow := toParquetRowAnnotated(richRows[2])
	tombEnrichRow.Enrichments = []parquetEnrichment{
		{
			Name:            "content_type",
			Value:           strPtr("text/plain"),
			Operation:       "head-object",
			ObservedAt:      "2026-10-18T01:00:00.000000001Z",
			AgainstRevision: parquetRevision{Kind: "opaque-etag", Value: "rev-old"},
		},
	}
	tombEnrichBytes, err := writeRawAnnotatedRows([]parquetRowAnnotatedSize{tombEnrichRow}, 0)
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-tombstone-with-enrichment", "enrichment_target", tombEnrichBytes)

	// Negative 19: enrichment against_revision disagrees with row revision.
	revMismatchRow := toParquetRowAnnotated(richRows[1])
	revMismatchRow.Enrichments = []parquetEnrichment{
		{
			Name:            "content_type",
			Value:           strPtr("text/plain"),
			Operation:       "head-object",
			ObservedAt:      "2026-10-18T01:05:00.000000009Z",
			AgainstRevision: parquetRevision{Kind: "opaque-etag", Value: "rev-wrong"},
		},
	}
	revMismatchBytes, err := writeRawAnnotatedRows([]parquetRowAnnotatedSize{revMismatchRow}, 0)
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-enrichment-revision-mismatch", "enrichment_revision", revMismatchBytes)

	// Negative 20: stamp conflict (same snapshot_id with conflicting timestamps across rows).
	conflictRow0 := toParquetRowAnnotated(richRows[0])
	conflictRow1 := toParquetRowAnnotated(richRows[1])
	conflictRow1.FirstSeen = parquetStamp{SnapshotID: "snapshot-0", At: "2026-10-18T00:00:01.000000000Z"}
	stampConflictBytes, err := writeRawAnnotatedRows([]parquetRowAnnotatedSize{conflictRow0, conflictRow1}, 0)
	if err != nil {
		return nil, err
	}
	addNegative(&out, "reject-parquet-stamp-conflict", "stamp_conflict", stampConflictBytes)

	// Self-verify all 23 generated twins before returning.
	for _, art := range out {
		decoded, decErr := ReadParquetInventoryMember(art.ParquetBytes, 0)
		if art.ExpectedRefusal != "" {
			if decErr == nil {
				return nil, fmt.Errorf("negative twin %q unexpectedly succeeded", art.ID)
			}
			if got, _ := RefusalReason(decErr); got != art.ExpectedRefusal {
				return nil, fmt.Errorf("negative twin %q refusal %q != expected %q (%v)", art.ID, got, art.ExpectedRefusal, decErr)
			}
			continue
		}
		if decErr != nil {
			return nil, fmt.Errorf("positive twin %q failed verification: %w", art.ID, decErr)
		}
		if !reflect.DeepEqual(decoded, art.LogicalRows) {
			return nil, fmt.Errorf("positive twin %q decoded rows mismatch", art.ID)
		}
	}

	return out, nil
}

// ExportSliceDParquetTwins writes the deterministic Slice D physical Parquet twins, NDJSON logical twins, and manifest.json to dir.
func ExportSliceDParquetTwins(dir string) error {
	twins, err := GenerateSliceDParquetTwins()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	for _, t := range twins {
		pPath := filepath.Join(dir, t.ID+".parquet")
		if err := os.WriteFile(pPath, t.ParquetBytes, 0o600); err != nil {
			return err
		}
		if len(t.LogicalNDJSON) > 0 {
			lPath := filepath.Join(dir, t.ID+".ndjson")
			if err := os.WriteFile(lPath, t.LogicalNDJSON, 0o600); err != nil {
				return err
			}
		}
	}
	manifestBytes, err := json.MarshalIndent(map[string]any{
		"profile":   EncodingProfileParquetInventoryV0,
		"artifacts": twins,
	}, "", "  ")
	if err != nil {
		return err
	}
	manifestBytes = append(manifestBytes, '\n')
	return os.WriteFile(filepath.Join(dir, "manifest.json"), manifestBytes, 0o600)
}

const parquetSpecRelPath = "docs/decisions/DDR-0004-durable-index-row-and-plan-encoding.md"

// PhysicalGraphCase holds a complete segmented-snapshot/v0 + durable-index/v0 graph with real parquet-inventory/v0 members.
type PhysicalGraphCase struct {
	Publication   Binding
	Store         map[string]string
	MemberParquet map[string][]byte
	// SourceEvidence is optional. When nil/empty, VerifyPhysicalGraph evaluates R2
	// (self-contained snapshot reader without historical parent transition checks).
	// When present, VerifyPhysicalGraph evaluates W1/W2/W3 transition parity.
	SourceEvidence map[string]any
	RequestExport  bool
}

func storeObjectWithPin(store map[string]string, id, ref string, obj any) (Binding, error) {
	raw, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return Binding{}, err
	}
	raw = append(raw, '\n')
	sum := sha256.Sum256(raw)
	store[ref] = string(raw)
	return Binding{
		ID:         id,
		Ref:        ref,
		SHA256:     hex.EncodeToString(sum[:]),
		ByteLength: int64(len(raw)),
	}, nil
}

// BuildPhysicalGraphFromLogical converts a logical projection graph case (such as vectors/graphs/baseline.json,
// observe.json, or enrich.json) into a physical parquet-inventory/v0 graph case with real Parquet member bytes.
func BuildPhysicalGraphFromLogical(logicalInput map[string]any, opts ParquetWriteOptions) (PhysicalGraphCase, error) {
	origStore, err := parseStoreMap(logicalInput["store"])
	if err != nil {
		return PhysicalGraphCase{}, err
	}
	store := make(map[string]string, len(origStore)+2)
	for k, v := range origStore {
		store[k] = v
	}
	memberParquet := make(map[string][]byte)

	upstreamFS, err := durableindexv0.FS()
	if err != nil {
		return PhysicalGraphCase{}, err
	}
	ddrBytes, err := fs.ReadFile(upstreamFS, parquetSpecRelPath)
	if err != nil {
		return PhysicalGraphCase{}, err
	}
	ddrSum := sha256.Sum256(ddrBytes)
	specRef := "artifact:parquet-spec"
	store[specRef] = string(ddrBytes)
	specPin := Binding{
		ID:         EncodingProfileParquetInventoryV0,
		Ref:        specRef,
		SHA256:     hex.EncodeToString(ddrSum[:]),
		ByteLength: int64(len(ddrBytes)),
	}

	profileRef := "artifact:parquet-profile"
	profileObj := map[string]any{
		"capabilities":     []any{SegmentedSnapshotCapability},
		"kind":             "snapshot_profile",
		"profile_id":       EncodingProfileParquetInventoryV0,
		"semantic_spec":    specPin,
		"operations":       []any{"full_verify"},
		"count_relations":  []any{"equal"},
		"physical_orders":  []any{"manifest_order"},
		"empty_snapshots":  true,
		"streaming":        "unsupported",
		"durability_claim": "none",
		"retention":        "unsupported",
		"limits": map[string]any{
			"max_members":        int64(16),
			"max_metadata_bytes": int64(1048576),
			"max_member_bytes":   int64(1048576),
		},
	}
	profilePin, err := storeObjectWithPin(store, EncodingProfileParquetInventoryV0, profileRef, profileObj)
	if err != nil {
		return PhysicalGraphCase{}, err
	}

	var srcEv map[string]any
	if seRaw, ok := asMap(logicalInput["source_evidence"]); ok {
		rawSE, _ := json.Marshal(seRaw)
		parsedSE, _ := StrictParseContractJSON(rawSE)
		srcEv, _ = asMap(parsedSE)
	}

	rebuiltPubs := make(map[string]Binding)
	var rebuildPub func(pubPin Binding) (Binding, error)
	rebuildPub = func(pubPin Binding) (Binding, error) {
		if b, ok := rebuiltPubs[pubPin.Ref]; ok {
			return b, nil
		}
		pubObj, _, err := resolvePinnedStoreObject(pubPin, origStore)
		if err != nil {
			return Binding{}, err
		}
		manPin, err := remountJSON[Binding](pubObj["manifest"])
		if err != nil {
			return Binding{}, err
		}
		manObj, _, err := resolvePinnedStoreObject(manPin, origStore)
		if err != nil {
			return Binding{}, err
		}
		descPin, err := remountJSON[Binding](pubObj["descriptor"])
		if err != nil {
			return Binding{}, err
		}
		descObj, _, err := resolvePinnedStoreObject(descPin, origStore)
		if err != nil {
			return Binding{}, err
		}
		metaPin, err := remountJSON[Binding](manObj["profile_metadata"])
		if err != nil {
			return Binding{}, err
		}
		metaObj, _, err := resolvePinnedStoreObject(metaPin, origStore)
		if err != nil {
			return Binding{}, err
		}
		sel, err := remountJSON[Selection](pubObj["selection"])
		if err != nil {
			return Binding{}, err
		}

		if metaObj["parent"] != nil {
			pBind, err := remountJSON[ParentBinding](metaObj["parent"])
			if err != nil {
				return Binding{}, err
			}
			newParentPub, err := rebuildPub(pBind.Publication)
			if err != nil {
				return Binding{}, err
			}
			metaObj["parent"] = map[string]any{
				"selection":   pBind.Selection,
				"publication": newParentPub,
			}
			if srcEv != nil {
				if snapEv, ok := asMap(srcEv[sel.SnapshotID]); ok {
					snapEv["admitted_parent_sha256"] = newParentPub.SHA256
				}
			}
		}

		newMetaPin, err := storeObjectWithPin(store, metaPin.ID, metaPin.Ref, metaObj)
		if err != nil {
			return Binding{}, err
		}

		membersSlice, _ := asSlice(manObj["members"])
		var newMembers []any
		for _, mRaw := range membersSlice {
			mm, _ := asMap(mRaw)
			mRef, _ := asString(mm["ref"])
			mID, _ := asString(mm["id"])
			ndjsonStr := origStore[mRef]
			trimmed := strings.TrimSuffix(ndjsonStr, "\n")
			var rows []LogicalRow
			if trimmed != "" {
				for _, line := range strings.Split(trimmed, "\n") {
					val, err := StrictParseContractJSON([]byte(line))
					if err != nil {
						return Binding{}, err
					}
					r, err := parseRowMap(val)
					if err != nil {
						return Binding{}, err
					}
					rows = append(rows, r)
				}
			}
			pBytes, err := WriteParquetInventoryMember(rows, opts)
			if err != nil {
				return Binding{}, err
			}
			pSum := sha256.Sum256(pBytes)
			delete(store, mRef)
			memberParquet[mRef] = pBytes
			newMembers = append(newMembers, map[string]any{
				"id":          mID,
				"ref":         mRef,
				"sha256":      hex.EncodeToString(pSum[:]),
				"byte_length": int64(len(pBytes)),
				"row_count":   int64(len(rows)),
			})
		}

		manObj["profile"] = profilePin
		manObj["representation_id"] = "parquet-rows"
		manObj["members"] = newMembers
		manObj["profile_metadata"] = newMetaPin
		newManPin, err := storeObjectWithPin(store, manPin.ID, manPin.Ref, manObj)
		if err != nil {
			return Binding{}, err
		}

		if prodMap, ok := asMap(descObj["producer"]); ok {
			prodMap["profile"] = EncodingProfileParquetInventoryV0
		}
		if repsSlice, ok := asSlice(descObj["representations"]); ok && len(repsSlice) == 1 {
			if repMap, ok := asMap(repsSlice[0]); ok {
				repMap["id"] = "parquet-rows"
				repMap["format"] = "parquet"
				repMap["profile"] = EncodingProfileParquetInventoryV0
			}
		}
		newDescPin, err := storeObjectWithPin(store, descPin.ID, descPin.Ref, descObj)
		if err != nil {
			return Binding{}, err
		}

		pubObj["profile"] = profilePin
		pubObj["manifest"] = newManPin
		pubObj["descriptor"] = newDescPin
		newPubPin, err := storeObjectWithPin(store, pubPin.ID, pubPin.Ref, pubObj)
		if err != nil {
			return Binding{}, err
		}
		rebuiltPubs[pubPin.Ref] = newPubPin
		return newPubPin, nil
	}

	rootPin, err := remountJSON[Binding](logicalInput["publication"])
	if err != nil {
		return PhysicalGraphCase{}, err
	}
	newRootPin, err := rebuildPub(rootPin)
	if err != nil {
		return PhysicalGraphCase{}, err
	}
	delete(store, "artifact:logical-spec")
	delete(store, "artifact:logical-profile")
	reqExp, _ := logicalInput["request_export"].(bool)
	return PhysicalGraphCase{
		Publication:    newRootPin,
		Store:          store,
		MemberParquet:  memberParquet,
		SourceEvidence: srcEv,
		RequestExport:  reqExp,
	}, nil
}

// VerifyPhysicalGraph validates a complete segmented-snapshot/v0 + durable-index/v0 graph over physical
// parquet-inventory/v0 members (R2 when SourceEvidence is nil; W1/W2/W3 when SourceEvidence is supplied).
func VerifyPhysicalGraph(pg PhysicalGraphCase) (map[string]any, error) {
	upstreamFS, err := durableindexv0.FS()
	if err != nil {
		return nil, err
	}
	ddrSpecBytes, err := fs.ReadFile(upstreamFS, parquetSpecRelPath)
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

	fetchMetadata := func(pin Binding, metadataRefs map[string]struct{}, maxMetaCeiling int64, schemaRel string) (map[string]any, []byte, error) {
		rawStr, ok := pg.Store[pin.Ref]
		if !ok {
			return nil, nil, NewRefusal("binding_unavailable", fmt.Sprintf("missing store ref %q", pin.Ref))
		}
		if pin.ByteLength < 0 || int64(len(rawStr)) != pin.ByteLength {
			return nil, nil, NewRefusal("binding_integrity", fmt.Sprintf("digest or length mismatch for %q", pin.Ref))
		}
		metadataRefs[pin.Ref] = struct{}{}
		allMetadataRefs[pin.Ref] = struct{}{}
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
			metaBudget += int64(len(pg.Store[mr]))
		}
		if metaBudget > metaLimit {
			return nil, nil, NewRefusal("capacity", "snapshot metadata budget exceeds limit")
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

	fetchMemberParquet := func(pin Binding, maxMemberCeiling int64) ([]LogicalRow, error) {
		rawBytes, ok := pg.MemberParquet[pin.Ref]
		if !ok {
			return nil, NewRefusal("binding_unavailable", fmt.Sprintf("missing parquet member ref %q", pin.Ref))
		}
		if pin.ByteLength < 0 || int64(len(rawBytes)) != pin.ByteLength {
			return nil, NewRefusal("binding_integrity", fmt.Sprintf("digest or length mismatch for %q", pin.Ref))
		}
		memberRefs[pin.Ref] = struct{}{}
		for mr := range allMetadataRefs {
			if _, conflict := memberRefs[mr]; conflict {
				return nil, NewRefusal("binding_alias", "metadata and member bindings alias a ref")
			}
		}
		memberLimit := int64(MaxDecompressedMemberBytes)
		if maxMemberCeiling > 0 && maxMemberCeiling < memberLimit {
			memberLimit = maxMemberCeiling
		}
		if int64(len(rawBytes)) > memberLimit {
			return nil, NewRefusal("capacity", "member byte length exceeds profile max_member_bytes")
		}
		sum := sha256.Sum256(rawBytes)
		if hex.EncodeToString(sum[:]) != pin.SHA256 {
			return nil, NewRefusal("binding_integrity", fmt.Sprintf("digest or length mismatch for %q", pin.Ref))
		}
		return ReadParquetInventoryMember(rawBytes, memberLimit)
	}

	var verifySnapshot func(pin Binding) (graphSnapshotResult, error)
	verifySnapshot = func(pin Binding) (graphSnapshotResult, error) {
		metadataRefs := make(map[string]struct{})
		var maxMetaBytes int64
		var maxMemberBytes int64
		fetch := func(b Binding, schemaRel string) (map[string]any, []byte, error) {
			return fetchMetadata(b, metadataRefs, maxMetaBytes, schemaRel)
		}
		if _, cyc := active[pin.SHA256]; cyc {
			return graphSnapshotResult{}, NewRefusal("parent_cycle", "cycle in parent publication chain")
		}
		if len(active) >= MaxAncestryDepth {
			return graphSnapshotResult{}, NewRefusal("capacity", "ancestry depth limit exceeded")
		}
		active[pin.SHA256] = struct{}{}
		defer delete(active, pin.SHA256)

		pub, _, err := fetch(pin, durableindexv0.IndexPublicationSchemaPath)
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
		profile, _, err := fetch(profPin, snapshotProfileSchemaRelPath)
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
			initialMetaBudget += int64(len(pg.Store[mr]))
		}
		if initialMetaBudget > metaLimit {
			return graphSnapshotResult{}, NewRefusal("capacity", "metadata bytes exceed profile max_metadata_bytes")
		}
		manifest, _, err := fetch(manPin, snapshotManifestSchemaRelPath)
		if err != nil {
			return graphSnapshotResult{}, err
		}
		descriptor, _, err := fetch(descPin, artifactDescriptorSchemaRelPath)
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
		if !reflect.DeepEqual(manifest["profile"], pub["profile"]) || profID != profPin.ID || profID != EncodingProfileParquetInventoryV0 {
			return graphSnapshotResult{}, NewRefusal("graph_profile", "profile binding or profile_id mismatch")
		}
		semSpecPin, err := remountJSON[Binding](profile["semantic_spec"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		_, semSpecRaw, err := fetch(semSpecPin, "")
		if err != nil {
			return graphSnapshotResult{}, err
		}
		if semSpecPin.ID != EncodingProfileParquetInventoryV0 || string(semSpecRaw) != string(ddrSpecBytes) {
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
		meta, _, err := fetch(pmPin, durableindexv0.IndexMetadataSchemaPath)
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
		_, rowSchemaRaw, err := fetch(rowSchemaPin, "")
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
		catalog, _, err := fetch(fcPin, "fieldCatalog")
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
		if repMap["profile"] != EncodingProfileParquetInventoryV0 || prodMap["profile"] != EncodingProfileParquetInventoryV0 ||
			repMap["uri"] != manPin.Ref || repMap["format"] != "parquet" ||
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
		if pg.RequestExport {
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
			mRows, err := fetchMemberParquet(mPin, maxMemberBytes)
			if err != nil {
				return graphSnapshotResult{}, err
			}
			mRowCount, _ := asInt64Exact(mm["row_count"])
			if int64(len(mRows)) != mRowCount {
				return graphSnapshotResult{}, NewRefusal("graph_count", "member decoded row count disagrees with member.row_count")
			}
			rows = append(rows, mRows...)
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

		planPin, err := remountJSON[Binding](meta["plan"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		planObj, _, err := fetch(planPin, durableindexv0.IndexPlanSchemaPath)
		if err != nil {
			return graphSnapshotResult{}, err
		}
		scopes, err := parseScopeList(planObj["scopes"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		covPins, err := remountJSON[[]Binding](meta["coverage"])
		if err != nil {
			return graphSnapshotResult{}, err
		}
		var covObjs []map[string]any
		for _, cp := range covPins {
			cObj, _, err := fetch(cp, coverageAttestationSchemaRelPath)
			if err != nil {
				return graphSnapshotResult{}, err
			}
			covObjs = append(covObjs, cObj)
		}

		if len(pg.SourceEvidence) == 0 {
			// R2 self-contained snapshot reader: verify plan, coverage, selection, snapshot bindings, and counts without ancestry walk.
			if err := ValidateCanonicalScopes(scopes); err != nil {
				return graphSnapshotResult{}, err
			}
			planSetID, _ := asString(planObj["set_id"])
			if planSetID != sel.NamespaceID || planPin.ID != sel.NamespaceID {
				return graphSnapshotResult{}, NewRefusal("plan_identity", "plan set_id or binding id mismatch")
			}
			metaPlanDigest, _ := asString(meta["plan_digest"])
			computedDigest, err := PlanDigest(planSetID, scopes)
			if err != nil {
				return graphSnapshotResult{}, err
			}
			if computedDigest != metaPlanDigest {
				return graphSnapshotResult{}, NewRefusal("plan_digest", "plan_digest mismatch")
			}
			effCount, _ := asInt64Exact(meta["effective_scope_count"])
			if int64(len(scopes)) != effCount {
				return graphSnapshotResult{}, NewRefusal("scope_count", "effective_scope_count mismatch")
			}
			if len(covPins) != len(scopes) {
				return graphSnapshotResult{}, NewRefusal("coverage_count", "coverage count does not match scope count")
			}
			seenRefs := map[string]struct{}{planPin.Ref: {}}
			seenIDs := make(map[string]struct{}, len(covPins))
			for _, cp := range covPins {
				if _, dup := seenRefs[cp.Ref]; dup {
					return graphSnapshotResult{}, NewRefusal("binding_alias", "duplicate ref across plan and coverage bindings")
				}
				seenRefs[cp.Ref] = struct{}{}
				if _, dup := seenIDs[cp.ID]; dup {
					return graphSnapshotResult{}, NewRefusal("coverage_identity", "duplicate coverage binding id")
				}
				seenIDs[cp.ID] = struct{}{}
			}

			mode, _ := asString(meta["mode"])
			genDeclared, _ := asInt64Exact(meta["generation"])
			admission, _ := asString(meta["admission"])
			var obsAt string
			if mode == ModeEnrich {
				if meta["observation_at"] != nil || meta["loss_acknowledgment"] != nil || admission != AdmissionInherited {
					return graphSnapshotResult{}, NewRefusal("metadata_transition", "enrich snapshot has invalid observation_at, loss_acknowledgment, or admission")
				}
			} else {
				var okObs bool
				obsAt, okObs = asString(meta["observation_at"])
				if !okObs {
					return graphSnapshotResult{}, NewRefusal("invalid_instant", "observation_at is required")
				}
				if err := ValidateInstant(obsAt); err != nil {
					return graphSnapshotResult{}, err
				}
			}
			if mode == ModeBaseline {
				if genDeclared != 0 || meta["parent"] != nil || meta["loss_acknowledgment"] != nil || admission != AdmissionBaselineNotCompared {
					return graphSnapshotResult{}, NewRefusal("metadata_transition", "baseline snapshot has invalid generation, parent, loss_acknowledgment, or admission")
				}
			} else {
				if genDeclared < 1 || meta["parent"] == nil {
					return graphSnapshotResult{}, NewRefusal("metadata_transition", "incremental snapshot requires generation >= 1 and parent")
				}
				pBind, err := remountJSON[ParentBinding](meta["parent"])
				if err != nil {
					return graphSnapshotResult{}, err
				}
				if pBind.Selection.NamespaceID != sel.NamespaceID {
					return graphSnapshotResult{}, NewRefusal("set_mismatch", "cross-set parent edge")
				}
				if pBind.Selection.SnapshotID == sel.SnapshotID {
					return graphSnapshotResult{}, NewRefusal("snapshot_reuse", "successor reuses parent snapshot_id")
				}
				if admission == AdmissionLossAcknowledged {
					if meta["loss_acknowledgment"] == nil {
						return graphSnapshotResult{}, NewRefusal("scope_loss", "narrowed_acknowledged admission requires loss_acknowledgment")
					}
					la, err := remountJSON[LossAcknowledgment](meta["loss_acknowledgment"])
					if err != nil {
						return graphSnapshotResult{}, err
					}
					if la.Parent != pBind || la.PlanSHA256 != metaPlanDigest {
						return graphSnapshotResult{}, NewRefusal("acknowledgment_target", "loss_acknowledgment parent or plan_sha256 mismatch")
					}
					if !ValidateIdentifier(la.AcknowledgedBy) {
						return graphSnapshotResult{}, NewRefusal("acknowledgment_actor", "invalid acknowledged_by")
					}
					if err := ValidateInstant(la.AcknowledgedAt); err != nil {
						return graphSnapshotResult{}, err
					}
				} else if meta["loss_acknowledgment"] != nil {
					return graphSnapshotResult{}, NewRefusal("unexpected_acknowledgment", "unexpected loss_acknowledgment")
				}
			}

			authority := make([]Scope, 0, len(scopes))
			for idx, s := range scopes {
				cp := covPins[idx]
				cObj := covObjs[idx]
				attID, _ := asString(cObj["attestation_id"])
				if attID != cp.ID {
					return graphSnapshotResult{}, NewRefusal("coverage_identity", "attestation_id mismatch")
				}
				claims, _ := asSlice(cObj["claims"])
				if _, hasSup := cObj["supersedes"]; hasSup || len(claims) != 1 {
					return graphSnapshotResult{}, NewRefusal("coverage_scope", "coverage attestation must have 1 claim and no supersedes")
				}
				claimMap, _ := asMap(claims[0])
				claimScope, err := parseCoverageClaimScope(claimMap["scope"])
				if err != nil {
					return graphSnapshotResult{}, err
				}
				if claimScope != s {
					return graphSnapshotResult{}, NewRefusal("coverage_scope", "coverage claim scope mismatch")
				}
				subjMap, _ := asMap(cObj["subject"])
				if subjMap["artifact_id"] != sel.ArtifactID || subjMap["grain_id"] != manGrainID {
					return graphSnapshotResult{}, NewRefusal("coverage_subject", "coverage subject artifact_id or grain_id mismatch")
				}
				emitterMap, _ := asMap(cObj["emitter"])
				rel, _ := asString(emitterMap["relation"])
				runID, _ := asString(emitterMap["run_id"])
				if rel != "producer" {
					return graphSnapshotResult{}, NewRefusal("coverage_emitter", "coverage emitter relation must be producer")
				}
				asOf, _ := asString(cObj["as_of"])
				if err := ValidateInstant(asOf); err != nil {
					return graphSnapshotResult{}, err
				}
				if mode != ModeEnrich {
					expectedSubject := map[string]any{
						"artifact_id": sel.ArtifactID,
						"grain_id":    manGrainID,
						"subject_uri": SubjectURI(sel),
					}
					if !reflect.DeepEqual(cObj["subject"], expectedSubject) {
						return graphSnapshotResult{}, NewRefusal("coverage_subject", "coverage subject disagrees with selection")
					}
					if runID != sel.SnapshotID {
						return graphSnapshotResult{}, NewRefusal("coverage_emitter", "coverage emitter run_id mismatch")
					}
					if asOf != obsAt {
						return graphSnapshotResult{}, NewRefusal("coverage_time", "coverage as_of differs from observation_at")
					}
					volMap, _ := asMap(claimMap["volume"])
					unit, _ := asString(volMap["unit"])
					obsVol, okObsVol := asInt64Exact(volMap["observed"])
					var observedInScope int64
					for _, r := range rows {
						if r.LastSeen.SnapshotID == sel.SnapshotID && ScopeContains(s, r.Namespace, r.Key) {
							observedInScope++
						}
					}
					if unit != "objects" || !okObsVol || obsVol != observedInScope {
						return graphSnapshotResult{}, NewRefusal("coverage_volume", "coverage volume unit or observed count mismatch")
					}
					if expRaw, hasExp := volMap["expected"]; hasExp {
						expVal, okExp := asInt64Exact(expRaw)
						if !okExp || expVal < observedInScope || expVal > MaxExactInteger {
							return graphSnapshotResult{}, NewRefusal("coverage_volume", "coverage expected volume invalid")
						}
					}
				} else if runID == sel.SnapshotID {
					return graphSnapshotResult{}, NewRefusal("coverage_inheritance", "enrich snapshot coverage emitter run_id must be inherited from ancestor")
				}
				gapsSlice, _ := asSlice(cObj["gaps"])
				for _, gRaw := range gapsSlice {
					gMap, _ := asMap(gRaw)
					gs, err := parseCoverageClaimScope(gMap["scope"])
					if err != nil {
						return graphSnapshotResult{}, err
					}
					if !ScopeCovers(s, gs) {
						return graphSnapshotResult{}, NewRefusal("coverage_gap", "coverage gap lies outside plan scope")
					}
				}
				covState, _ := asString(cObj["coverage_state"])
				if covState == "complete" && len(gapsSlice) > 0 {
					return graphSnapshotResult{}, NewRefusal("coverage_complete_gap", "complete coverage claim contains a gap")
				}
				basis, _ := asString(claimMap["basis"])
				method, _ := asString(claimMap["method"])
				if covState == "complete" && basis == "confirmed" && (method == "enumerated" || method == "reconciled") {
					authority = append(authority, s)
				}
			}

			var retained int64
			for _, r := range rows {
				if mode == ModeEnrich {
					for _, sp := range []*Stamp{&r.FirstSeen, &r.LastSeen, &r.LastChanged, r.Deleted} {
						if sp != nil && sp.SnapshotID == sel.SnapshotID {
							return graphSnapshotResult{}, NewRefusal("enrichment_observation", "enrich snapshot row stamp names current snapshot_id")
						}
					}
					continue
				}
				for _, sp := range []*Stamp{&r.FirstSeen, &r.LastSeen, &r.LastChanged, r.Deleted} {
					if sp == nil {
						continue
					}
					if sp.SnapshotID == sel.SnapshotID {
						if sp.At != obsAt {
							return graphSnapshotResult{}, NewRefusal("coverage_time", "row stamp for current snapshot differs from observation_at")
						}
					} else if sp.At > obsAt {
						return graphSnapshotResult{}, NewRefusal("clock_reversal", "observation_at precedes inherited stamp")
					}
				}
				if mode == ModeBaseline {
					if r.Deleted != nil || r.FirstSeen.SnapshotID != sel.SnapshotID || r.LastSeen.SnapshotID != sel.SnapshotID || r.LastChanged.SnapshotID != sel.SnapshotID {
						return graphSnapshotResult{}, NewRefusal("metadata_transition", "baseline row must be active and stamped with baseline snapshot_id")
					}
				}
				if r.LastSeen.SnapshotID == sel.SnapshotID {
					if r.Deleted != nil {
						return graphSnapshotResult{}, NewRefusal("lineage_order", "row observed in current snapshot cannot be deleted")
					}
					inPlan := false
					for _, sc := range scopes {
						if ScopeContains(sc, r.Namespace, r.Key) {
							inPlan = true
							break
						}
					}
					if !inPlan {
						return graphSnapshotResult{}, NewRefusal("observation_outside_plan", "observed row lies outside plan")
					}
				} else {
					retained++
					if r.Deleted != nil && r.Deleted.SnapshotID == sel.SnapshotID {
						authorized := false
						for _, authScope := range authority {
							if ScopeContains(authScope, r.Namespace, r.Key) {
								authorized = true
								break
							}
						}
						if !authorized {
							return graphSnapshotResult{}, NewRefusal("coverage_outside_plan", "tombstone in current snapshot lacks confirmed-complete coverage authority")
						}
					}
				}
			}
			if mode == ModeEnrich {
				retained = int64(len(rows))
			}
			if mode == ModeBaseline && retained != 0 {
				return graphSnapshotResult{}, NewRefusal("metadata_transition", "baseline snapshot cannot have retained_unobserved_rows")
			}
			retainedDeclared, _ := asInt64Exact(meta["retained_unobserved_rows"])
			if retained != retainedDeclared {
				return graphSnapshotResult{}, NewRefusal("metadata_transition", "retained_unobserved_rows mismatch")
			}
			return graphSnapshotResult{
				selection:       sel,
				metadata:        meta,
				scopes:          scopes,
				rows:            rows,
				protectionClass: effClass,
			}, nil
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

		srcRaw, hasSrc := pg.SourceEvidence[sel.SnapshotID]
		if !hasSrc || srcRaw == nil {
			return graphSnapshotResult{}, NewRefusal("graph_evidence", "missing source_evidence for snapshot")
		}
		srcMap, _ := asMap(srcRaw)
		storeAny := make(map[string]any, len(pg.Store))
		for k, v := range pg.Store {
			storeAny[k] = v
		}
		evidence := map[string]any{
			"metadata":     meta,
			"store":        storeAny,
			"grain_id":     manGrainID,
			"observations": srcMap["observations"],
			"enumerated":   srcMap["enumerated"],
		}
		if inheritedEvidence != nil {
			evidence["inherited_from"] = inheritedEvidence
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
			return graphSnapshotResult{}, NewRefusal("graph_transition", "published parquet rows differ from checked transition output")
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

	res, err := verifySnapshot(pg.Publication)
	if err != nil {
		return nil, err
	}
	gen, _ := asInt64Exact(res.metadata["generation"])
	ret, _ := asInt64Exact(res.metadata["retained_unobserved_rows"])
	evScope := "R2"
	if len(pg.SourceEvidence) > 0 {
		mode, _ := asString(res.metadata["mode"])
		switch mode {
		case ModeBaseline:
			evScope = "W1"
		case ModeObserve:
			evScope = "W2"
		case ModeEnrich:
			evScope = "W3"
		}
	}
	return map[string]any{
		"rows":                     rowsToAnySlice(res.rows),
		"retained_unobserved_rows": ret,
		"generation":               gen,
		"protection": map[string]any{
			"action": "block_export",
			"class":  res.protectionClass,
		},
		"evidence_scope": evScope,
	}, nil
}
