package stream

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/3leaps/gonimbus/pkg/output"
)

const DefaultMaxLineBytes = 1 << 20

type EventKind int

const (
	EventRecord EventKind = iota
	EventChunk
)

type Event struct {
	Kind   EventKind
	Record output.Record
	Chunk  *ChunkEvent
}

type ChunkEvent struct {
	Header Chunk
	Body   io.ReadCloser
}

type Decoder struct {
	r            *bufio.Reader
	maxLineBytes int
	activeChunk  *chunkReader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: bufio.NewReader(r), maxLineBytes: DefaultMaxLineBytes}
}

func (d *Decoder) SetMaxLineBytes(n int) {
	if n <= 0 {
		d.maxLineBytes = DefaultMaxLineBytes
		return
	}
	d.maxLineBytes = n
}

func (d *Decoder) Next() (Event, error) {
	if d.activeChunk != nil {
		_ = d.activeChunk.Close()
		d.activeChunk = nil
	}

	line, err := readLineLimited(d.r, d.maxLineBytes)
	if err != nil {
		return Event{}, err
	}
	if len(bytes.TrimSpace(line)) == 0 {
		return Event{}, io.EOF
	}

	var rec output.Record
	if err := json.Unmarshal(line, &rec); err != nil {
		return Event{}, err
	}

	if rec.Type != TypeStreamChunk {
		return Event{Kind: EventRecord, Record: rec}, nil
	}

	var hdr Chunk
	if err := json.Unmarshal(rec.Data, &hdr); err != nil {
		return Event{}, err
	}
	if hdr.NBytes < 0 {
		return Event{}, errors.New("stream chunk nbytes must be >= 0")
	}

	cr := &chunkReader{r: d.r, remaining: hdr.NBytes}
	d.activeChunk = cr

	return Event{
		Kind:   EventChunk,
		Record: rec,
		Chunk:  &ChunkEvent{Header: hdr, Body: cr},
	}, nil
}

type chunkReader struct {
	r         io.Reader
	remaining int64
	closed    bool
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}

	n, err := c.r.Read(p)
	if n > 0 {
		c.remaining -= int64(n)
		// If the underlying reader returns data and EOF, treat it as a normal read.
		// We'll validate completeness by remaining bytes.
		return n, nil
	}

	if errors.Is(err, io.EOF) {
		// EOF before consuming all declared bytes.
		return 0, io.ErrUnexpectedEOF
	}
	return n, err
}

func (c *chunkReader) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if c.remaining == 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, c.r, c.remaining)
	c.remaining = 0
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func readLineLimited(r *bufio.Reader, maxBytes int) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxLineBytes
	}

	var out []byte
	for {
		frag, err := r.ReadSlice('\n')
		out = append(out, frag...)
		if len(out) > maxBytes {
			return nil, errors.New("jsonl line exceeds max bytes")
		}
		if err == nil {
			return bytes.TrimSuffix(out, []byte("\n")), nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(out) == 0 {
				return nil, io.EOF
			}
			return out, nil
		}
		return nil, err
	}
}
