# QA Checklist: Mixed-Framing Streams

## Unit Tests (required)

- Writer -> Decoder roundtrip with one small chunk.
- Truncated chunk bytes yields unexpected EOF.
- Decoder draining behavior: consumer reads partial chunk then closes; next record remains parseable.
- Large JSON line handling respects max line bytes.

## Cloud Integration (moto)

- Cloud -> local: `GetObject` streamed through `pkg/stream.Writer` and re-decoded.
- Cloud -> cloud: decoded bytes uploaded to another bucket and verified by `Head`.

## Safety/Correctness

- Ensure output cannot interleave chunk payloads when multiple goroutines write concurrently.
- Ensure chunk boundaries are honored and never merged across headers.
