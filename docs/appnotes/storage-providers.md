# Storage Provider Configuration

This document maps gonimbus configuration parameters to S3-compatible storage providers. For detailed provider documentation, refer to the official sources linked in each section.

## Quick Reference

| Parameter | AWS S3 | Wasabi | Cloudflare R2 |
|-----------|--------|--------|---------------|
| `endpoint` | (not needed) | `s3.<region>.wasabisys.com` | `<account_id>.r2.cloudflarestorage.com` |
| `region` | Required | Required (must match endpoint) | `auto` (placeholder) |
| `bucket` | Bucket name | Bucket name | Bucket name |
| Auth | SDK chain / keys | Access key + secret | Access key + secret (token-scoped) |
| **Status** | Validated | Validated | Validated |

## Provider Details

### AWS S3

Standard AWS S3 uses the SDK default credential chain and region resolution. No custom endpoint is needed.

```yaml
connection:
  provider: s3
  bucket: my-bucket
  region: us-east-1  # or omit to use SDK resolution
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

| Region | Endpoint |
|--------|----------|
| us-east-1 | s3.us-east-1.wasabisys.com |
| us-east-2 | s3.us-east-2.wasabisys.com |
| us-central-1 | s3.us-central-1.wasabisys.com |
| us-west-1 | s3.us-west-1.wasabisys.com |
| eu-central-1 | s3.eu-central-1.wasabisys.com |
| eu-central-2 | s3.eu-central-2.wasabisys.com |
| eu-west-1 | s3.eu-west-1.wasabisys.com |
| eu-west-2 | s3.eu-west-2.wasabisys.com |
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
  region: auto  # Required placeholder for SDK endpoint resolution
```

**Location hints:** R2 determines bucket location at creation time. Hints (`wnam`, `enam`, `weur`, `eeur`, `apac`, `oc`) are best-effort suggestions, not guarantees. The `region` parameter in gonimbus does not affect data placement.

**Jurisdiction-restricted buckets:** For EU or FedRAMP compliance, use jurisdiction-specific endpoints:

```yaml
endpoint: https://<account_id>.eu.r2.cloudflarestorage.com   # EU jurisdiction
```

See [R2 Data Location](https://developers.cloudflare.com/r2/reference/data-location/) for details.

**Auth:** API token (access key + secret) scoped per-bucket. Create via Cloudflare dashboard.

### DigitalOcean Spaces

*(Coming soon)*

```yaml
connection:
  provider: s3
  bucket: my-bucket
  endpoint: https://<region>.digitaloceanspaces.com
  region: <region>  # e.g., nyc3, sfo3, ams3, sgp1
```

## Environment Variables

All providers support standard AWS SDK environment variables:

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Access key |
| `AWS_SECRET_ACCESS_KEY` | Secret key |
| `AWS_REGION` | Default region (if not in config) |
| `AWS_PROFILE` | Named profile from ~/.aws/credentials |

## See Also

- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Wasabi Documentation](https://docs.wasabi.com/)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)
- [DigitalOcean Spaces Documentation](https://docs.digitalocean.com/products/spaces/)
