package stream

import "time"

const (
	TypeStreamOpen  = "gonimbus.stream.open.v1"
	TypeStreamChunk = "gonimbus.stream.chunk.v1"
	TypeStreamClose = "gonimbus.stream.close.v1"
)

type Open struct {
	StreamID string `json:"stream_id"`
	URI      string `json:"uri"`

	ETag         string     `json:"etag,omitempty"`
	Size         *int64     `json:"size,omitempty"`
	LastModified *time.Time `json:"last_modified,omitempty"`
	ContentType  string     `json:"content_type,omitempty"`
	Encoding     string     `json:"content_encoding,omitempty"`

	Range *ByteRange `json:"range,omitempty"`
}

type ByteRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

type Chunk struct {
	StreamID string `json:"stream_id"`
	Seq      int64  `json:"seq"`
	NBytes   int64  `json:"nbytes"`
	Offset   *int64 `json:"offset,omitempty"`
}

type Close struct {
	StreamID string `json:"stream_id"`
	Status   string `json:"status"`
	Chunks   int64  `json:"chunks"`
	Bytes    int64  `json:"bytes"`

	DurationNS *int64 `json:"duration_ns,omitempty"`
}
