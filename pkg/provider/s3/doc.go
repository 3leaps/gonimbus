// Package s3 implements the provider interface for AWS S3 and S3-compatible
// object storage.
//
// Library consumers should construct providers with Config and New. A Config
// struct literal is passive: it does not read environment variables or shared
// AWS config files. Provider construction calls the AWS SDK v2 default loader,
// so environment and shared-config reads happen during New when the SDK resolves
// credentials, region, profile, and configured endpoints.
//
// Explicit AccessKeyID and SecretAccessKey values suppress SDK credential-chain
// lookup for credentials only. They do not suppress ambient region, profile, or
// endpoint configuration. Consumers that need hermetic endpoint behavior should
// either pass a non-empty Config.Endpoint or set
// AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true in their own process. See
// docs/library-consumers.md for the full embedded-use contract.
//
// API stability: Stable. Breaking changes to exported symbols or documented
// behavior follow the Library API protocol in docs/api-stability.md.
package s3
