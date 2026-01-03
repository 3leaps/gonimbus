# Release Notes

This file contains release notes for the three most recent releases in reverse chronological order. For the complete release history, see the [CHANGELOG](CHANGELOG.md) or the [docs/releases/](docs/releases/) directory.

---

## v0.1.0 (2026-01-03)

**Initial Public Release**

Gonimbus is a Go-first library + CLI + server for large-scale inspection and crawl of cloud object storage (100K-1M+ objects). This release delivers S3 support with prefix-first listing and JSONL output.

### Highlights

- **S3 & S3-Compatible Support**: AWS S3, Wasabi, Cloudflare R2, DigitalOcean Spaces
- **Pattern Matching**: Doublestar globs with intelligent prefix derivation for scale
- **Streaming Output**: JSONL records with typed envelopes for objects, errors, and progress
- **Bounded Pipeline**: Configurable concurrency with backpressure and rate limiting
- **Schema-Validated Manifests**: YAML/JSON job manifests with strict validation

### CLI Commands

```bash
# Quick inspection
gonimbus inspect s3://bucket/prefix/

# Run a crawl job
gonimbus crawl --job manifest.yaml

# Check environment
gonimbus doctor
```

### Getting Started

```bash
go install github.com/3leaps/gonimbus/cmd/gonimbus@v0.1.0
gonimbus version
```

See [docs/releases/v0.1.0.md](docs/releases/v0.1.0.md) for full release notes.
