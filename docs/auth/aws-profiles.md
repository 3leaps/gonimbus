# AWS Profile Authentication

Gonimbus supports AWS SSO (Identity Center) profiles for enterprise authentication. This guide covers setup, usage, and troubleshooting.

## Authentication Methods

Gonimbus supports two primary authentication patterns:

| Method                 | Use Case                                       | Setup                      |
| ---------------------- | ---------------------------------------------- | -------------------------- |
| **Static credentials** | S3-compatible stores, simple AWS setups, CI/CD | Access key/secret key      |
| **AWS SSO profiles**   | Enterprise AWS with Identity Center            | Profile in `~/.aws/config` |

## Quick Start

### Using SSO Profiles

```bash
# 1. Authenticate with AWS SSO (one-time, valid for 8-12 hours typically)
aws sso login --profile myprofile

# 2. Use gonimbus with the profile
gonimbus inspect s3://my-bucket/prefix/ --profile myprofile
gonimbus doctor --provider s3 --profile myprofile
```

### Using Static Credentials

```bash
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
gonimbus inspect s3://my-bucket/prefix/

# Option 2: For S3-compatible stores (Wasabi, MinIO, etc.)
gonimbus inspect s3://my-bucket/prefix/ \
  --endpoint https://s3.wasabisys.com \
  --region us-east-1
```

## SSO Configuration

### AWS Config Structure

AWS SSO uses a shared session model. Multiple profiles can share one SSO session:

```ini
# ~/.aws/config

# Shared SSO session (authenticate once, use across profiles)
[sso-session myorg]
sso_start_url = https://myorg.awsapps.com/start
sso_region = us-west-2
sso_registration_scopes = sso:account:access

# Profile for account A
[profile myorg-account-a-readonly]
sso_session = myorg
sso_account_id = 111111111111
sso_role_name = ReadOnlyAccess
region = us-west-2

# Profile for account B (same SSO session)
[profile myorg-account-b-readonly]
sso_session = myorg
sso_account_id = 222222222222
sso_role_name = ReadOnlyAccess
region = us-west-2
```

### Multi-Account Access

One SSO login covers all profiles sharing the same `sso-session`. This enables:

```bash
# Single login
aws sso login --profile myorg-account-a-readonly

# Access both accounts (no additional login needed)
gonimbus inspect s3://bucket-in-account-a/ --profile myorg-account-a-readonly
gonimbus inspect s3://bucket-in-account-b/ --profile myorg-account-b-readonly

# Even concurrent access works
gonimbus crawl --job account-a.yaml &
gonimbus crawl --job account-b.yaml &
```

## Using Profiles with Manifests

Specify the profile in your crawl manifest:

```yaml
# crawl-manifest.yaml
version: "1.0"
connection:
  provider: s3
  bucket: my-data-bucket
  profile: myorg-account-a-readonly
match:
  includes:
    - "data/**/*.parquet"
crawl:
  concurrency: 4
output:
  destination: stdout
```

```bash
gonimbus crawl --job crawl-manifest.yaml
```

## Checking Credentials

Use `gonimbus doctor` to verify your credentials:

```bash
# Check specific profile
gonimbus doctor --provider s3 --profile myorg-account-a-readonly

# Example output:
# S3 Provider Checks (profile: myorg-account-a-readonly):
# [6/8] Checking AWS credentials... ✅ Found credentials
#       Access Key: ****ABCD
#       Source: SSOProvider
# [7/8] Checking credential source... ✅ SSOProvider
# [8/8] Checking credential expiry... ✅ Valid for 5h 42m
```

## Troubleshooting

### SSO Token Expired

**Symptom:**

```
Cannot retrieve credentials: token has expired
```

**Fix:**

```bash
aws sso login --profile <your-profile>
```

### Profile Not Found

**Symptom:**

```
failed to get shared config profile, nonexistent-profile
```

**Fix:** Check `~/.aws/config` for the profile name. Profile names are case-sensitive.

### Credential Expiry Warning

If `gonimbus doctor` shows credentials expiring soon:

```
[8/8] Checking credential expiry... ⚠️  Expires in 45m
```

Re-authenticate before running long crawls:

```bash
aws sso login --profile <your-profile>
```

### Slow Credential Lookup

If commands take 4+ seconds before failing with EC2 metadata errors, specify a profile or set credentials explicitly. This happens when no credentials are configured and the SDK waits for EC2 IMDS.

## Best Practices

1. **Use read-only roles for crawling** - Crawl operations only need `s3:ListBucket` and `s3:GetObject` permissions.

2. **Name profiles descriptively** - Include organization, account/environment, and role:

   ```
   myorg-prod-readonly
   myorg-dev-admin
   ```

3. **Re-authenticate before long jobs** - SSO tokens typically expire after 8-12 hours. Check with `gonimbus doctor --provider s3 --profile <name>`.

4. **Use manifests for repeatable jobs** - Embed the profile in your manifest rather than relying on environment variables.

## Environment Variables

Gonimbus respects standard AWS environment variables:

| Variable                | Description                              |
| ----------------------- | ---------------------------------------- |
| `AWS_PROFILE`           | Default profile to use                   |
| `AWS_ACCESS_KEY_ID`     | Static access key                        |
| `AWS_SECRET_ACCESS_KEY` | Static secret key                        |
| `AWS_REGION`            | Default region                           |
| `AWS_ENDPOINT_URL`      | Custom endpoint for S3-compatible stores |

## See Also

- [AWS SSO Configuration](https://docs.aws.amazon.com/cli/latest/userguide/sso-configure-profile-token.html)
- [gonimbus crawl command](../../README.md#cli-commands)
- [Job manifest schema](../architecture.md#job-manifest)
