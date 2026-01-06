# Storage Provider Configuration

This document maps gonimbus configuration parameters to S3-compatible storage providers. For detailed provider documentation, refer to the official sources linked in each section.

## Quick Reference

| Parameter  | AWS S3           | Wasabi                         | Cloudflare R2                           |
| ---------- | ---------------- | ------------------------------ | --------------------------------------- |
| `endpoint` | (not needed)     | `s3.<region>.wasabisys.com`    | `<account_id>.r2.cloudflarestorage.com` |
| `region`   | Required         | Required (must match endpoint) | `auto` (placeholder)                    |
| `bucket`   | Bucket name      | Bucket name                    | Bucket name                             |
| Auth       | SDK chain / keys | Access key + secret            | Access key + secret (token-scoped)      |
| **Status** | Validated        | Validated                      | Validated                               |

## Provider Details

### AWS S3

Standard AWS S3 uses the SDK default credential chain and region resolution. No custom endpoint is needed.

```yaml
connection:
  provider: s3
  bucket: my-bucket
  region: us-east-1 # or omit to use SDK resolution
```

**Auth options:** Environment variables, shared credentials, instance profiles, IRSA.

### Wasabi

Wasabi requires both endpoint and region, and they must align.

```yaml
connection:
  provider: s3
  bucket: my-bucket
  endpoint: https://s3.us-east-2.wasabisys.com
  region: us-east-2
```

**Regions:** See [Wasabi Regions](https://docs.wasabi.com/docs/what-are-the-service-urls-for-wasabis-different-storage-regions)

| Region         | Endpoint                        |
| -------------- | ------------------------------- |
| us-east-1      | s3.us-east-1.wasabisys.com      |
| us-east-2      | s3.us-east-2.wasabisys.com      |
| us-central-1   | s3.us-central-1.wasabisys.com   |
| us-west-1      | s3.us-west-1.wasabisys.com      |
| eu-central-1   | s3.eu-central-1.wasabisys.com   |
| eu-central-2   | s3.eu-central-2.wasabisys.com   |
| eu-west-1      | s3.eu-west-1.wasabisys.com      |
| eu-west-2      | s3.eu-west-2.wasabisys.com      |
| ap-northeast-1 | s3.ap-northeast-1.wasabisys.com |
| ap-northeast-2 | s3.ap-northeast-2.wasabisys.com |
| ap-southeast-1 | s3.ap-southeast-1.wasabisys.com |
| ap-southeast-2 | s3.ap-southeast-2.wasabisys.com |

**Auth:** Access key + secret key via environment or explicit config.

### Cloudflare R2

R2 uses account-specific endpoints and ignores region (but gonimbus requires a placeholder).

```yaml
connection:
  provider: s3
  bucket: my-bucket
  endpoint: https://<account_id>.r2.cloudflarestorage.com
  region: auto # Required placeholder for SDK endpoint resolution
```

**Location hints:** R2 determines bucket location at creation time. Hints (`wnam`, `enam`, `weur`, `eeur`, `apac`, `oc`) are best-effort suggestions, not guarantees. The `region` parameter in gonimbus does not affect data placement.

**Jurisdiction-restricted buckets:** For EU or FedRAMP compliance, use jurisdiction-specific endpoints:

```yaml
endpoint: https://<account_id>.eu.r2.cloudflarestorage.com # EU jurisdiction
```

See [R2 Data Location](https://developers.cloudflare.com/r2/reference/data-location/) for details.

**Auth:** API token (access key + secret) scoped per-bucket. Create via Cloudflare dashboard.

### DigitalOcean Spaces

_(Coming soon)_

```yaml
connection:
  provider: s3
  bucket: my-bucket
  endpoint: https://<region>.digitaloceanspaces.com
  region: <region> # e.g., nyc3, sfo3, ams3, sgp1
```

## Preflight Permission Probes

Before running large-scale operations (crawling millions of objects, cross-account transfers), use the `preflight` command to validate that your credentials have the required permissions. This avoids discovering permission issues after minutes of enumeration.

### Why Preflight?

Cloud object stores do not provide a universal "check my permissions" API. The only reliable approach is to perform minimal probe operations:

- **Crawl operations** need `ListObjectsV2` (and optionally `HeadObject` for enrichment)
- **Transfer operations** need read permissions on source and write/delete on target

Without preflight validation, a job might:

- Spend 10 minutes listing 1M objects, then fail on the first `HeadObject` call
- Complete source enumeration, then discover it cannot write to the target bucket

### Preflight Modes

| Mode          | Provider Calls                                        | Use Case                                   |
| ------------- | ----------------------------------------------------- | ------------------------------------------ |
| `plan-only`   | None                                                  | Validate configuration syntax              |
| `read-safe`   | List, Head                                            | Validate read permissions before crawl     |
| `write-probe` | CreateMultipartUpload+Abort or PutObject+DeleteObject | Validate write permissions before transfer |

### Crawl Preflight

Validate list permissions before a crawl:

```bash
# AWS S3
gonimbus preflight crawl s3://my-bucket/data/**/*.parquet --mode read-safe

# Wasabi
gonimbus preflight crawl s3://my-bucket/data/**/*.parquet \
  --endpoint https://s3.us-east-2.wasabisys.com \
  --region us-east-2 \
  --mode read-safe

# Cloudflare R2
gonimbus preflight crawl s3://my-bucket/data/**/*.parquet \
  --endpoint https://<account_id>.r2.cloudflarestorage.com \
  --region auto \
  --mode read-safe
```

For exact object keys (not patterns), the crawl preflight also validates `HeadObject`:

```bash
gonimbus preflight crawl s3://my-bucket/path/to/specific-file.json --mode read-safe
```

### Write Preflight

Validate write permissions before transfer operations. This uses minimal-side-effect probes under an isolated prefix (`_gonimbus/probe/` by default).

**Preferred strategy: multipart-abort**

Creates and immediately aborts a multipart upload. No objects are durably stored.

```bash
gonimbus preflight write s3://target-bucket/ \
  --mode write-probe \
  --probe-strategy multipart-abort
```

**Fallback strategy: put-delete**

Writes a 0-byte object and deletes it. Use when multipart operations are restricted.

```bash
gonimbus preflight write s3://target-bucket/ \
  --mode write-probe \
  --probe-strategy put-delete
```

### Output Format

Preflight emits a `gonimbus.preflight.v1` JSONL record:

```json
{
  "type": "gonimbus.preflight.v1",
  "ts": "2024-01-15T10:00:00Z",
  "job_id": "abc123",
  "provider": "s3",
  "data": {
    "mode": "read-safe",
    "probe_strategy": "multipart-abort",
    "probe_prefix": "_gonimbus/probe/",
    "results": [
      {
        "capability": "source.list",
        "allowed": true,
        "method": "List(prefix=\"data/\",maxKeys=1)"
      }
    ]
  }
}
```

Failed probes include error details:

```json
{
  "capability": "target.write",
  "allowed": false,
  "method": "CreateMultipartUpload+Abort",
  "error_code": "ACCESS_DENIED",
  "detail": "access denied"
}
```

### Provider-Specific Notes

**AWS S3**: Both probe strategies work with standard IAM policies.

**Cloudflare R2**: Both strategies supported. Requires `--region auto` placeholder.

**Wasabi**: Some IAM configurations may allow `CreateMultipartUpload` but deny `AbortMultipartUpload`. If multipart-abort fails, use the put-delete fallback strategy.

### Required IAM Permissions

For crawl preflight (`read-safe`):

- `s3:ListBucket` (on bucket)
- `s3:GetObject` (on objects, if probing exact keys)

For write preflight (`write-probe` with multipart-abort):

- `s3:PutObject` (on probe prefix)
- `s3:AbortMultipartUpload` (on probe prefix)

For write preflight (`write-probe` with put-delete):

- `s3:PutObject` (on probe prefix)
- `s3:DeleteObject` (on probe prefix)

Example IAM policy for probe prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::my-bucket/_gonimbus/probe/*"
    }
  ]
}
```

## Environment Variables

All providers support standard AWS SDK environment variables:

| Variable                | Description                           |
| ----------------------- | ------------------------------------- |
| `AWS_ACCESS_KEY_ID`     | Access key                            |
| `AWS_SECRET_ACCESS_KEY` | Secret key                            |
| `AWS_REGION`            | Default region (if not in config)     |
| `AWS_PROFILE`           | Named profile from ~/.aws/credentials |

## See Also

- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Wasabi Documentation](https://docs.wasabi.com/)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)
- [DigitalOcean Spaces Documentation](https://docs.digitalocean.com/products/spaces/)
