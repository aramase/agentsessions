#!/usr/bin/env bash
# Regenerates docs/api-reference.md from the .proto comments.
#
# Run this after any change to api/*.proto. CI enforces that the checked-in reference
# matches what this script produces, so a schema change that skips it fails the build.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

out="docs/api-reference.md"

buf generate --template buf.gen.docs.yaml

# protoc-gen-doc emits no provenance header. A 1300-line file with no banner invites
# hand-edits that the next regeneration silently reverts, so prepend one.
tmp="$(mktemp)"
trap 'rm -f "${tmp}"' EXIT
cat >"${tmp}" <<'EOF'
<!--
GENERATED FILE. DO NOT EDIT.

Regenerate with: ./hack/gen-api-reference.sh
Source of truth: api/*.proto. Edit the proto comments, not this file.
-->

EOF
cat "${out}" >>"${tmp}"
mv "${tmp}" "${out}"

echo "wrote ${out}"
