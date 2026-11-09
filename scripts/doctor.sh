#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scope="${1:-${DOCTOR_SCOPE:-dev}}"
exec python3 -I -B "$root/scripts/doctor.py" "$scope"
