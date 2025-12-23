#!/usr/bin/env bash

set -euo pipefail

echo "❌ Deprecated: scripts/sign-release-artifacts.sh" >&2
echo "This repository now signs checksum manifests (SHA256SUMS/SHA512SUMS) instead of signing each artifact." >&2
echo "Use:" >&2
echo "  make release-build" >&2
echo "  make release-sign" >&2
echo "  make release-export-keys" >&2
echo "  make release-upload" >&2
exit 2
