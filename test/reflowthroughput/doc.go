// Package reflowthroughput is a non-CI, on-demand measurement harness for
// transfer reflow throughput and parallelism behavior.
//
// It is test/tooling only: it executes a built gonimbus binary as a child
// process, never calls Cobra command internals, and must not introduce
// production package seams, product record schema, or provider runtime changes.
//
// Entry point: make test-reflow-throughput PROFILE=<name> PROVIDER=<class>
// Default PROFILE=smoke, PROVIDER=file (credential-free local backend).
//
// BYO cloud transport reuses the existing non-CI lanes in test/cloudtest and
// make test-cloud-real (GONIMBUS_S3_TEST_* / GONIMBUS_GCS_TEST_* opt-in,
// unique minted prefixes, ambient credentials). Moto uses the same endpoint
// and test-key pattern as make test-cloud. Local/moto results are correctness
// evidence only; real-provider claims require PROVIDER=s3-compatible with BYO
// env and are labeled provider_opt_in in the report.
package reflowthroughput
