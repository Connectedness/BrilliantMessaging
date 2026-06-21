#!/usr/bin/env bash
# Validates a release version and classifies it as stable or prerelease.
#
# The version is read from the RELEASE_VERSION environment variable so that
# untrusted workflow input is never interpolated into shell source. On success
# the script prints `version=<value>` and `prerelease=<true|false>` and, when
# GITHUB_OUTPUT points at a file, appends the same key/value pairs to it so the
# release workflow can consume them as step outputs. An invalid or missing
# version causes a non-zero exit with a diagnostic on stderr.
set -euo pipefail

pattern='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'
version="${RELEASE_VERSION:-}"

if [ -z "$version" ]; then
  echo "error: RELEASE_VERSION is not set" >&2
  exit 1
fi

if [[ "$version" == *$'\n'* || "$version" == *$'\r'* ]]; then
  echo "error: RELEASE_VERSION must contain exactly one line" >&2
  exit 1
fi

if ! printf '%s' "$version" | grep -Eq "$pattern"; then
  echo "error: '$version' is not a valid release version (expected $pattern)" >&2
  exit 1
fi

# A `-<suffix>` (the only place the pattern permits a hyphen) marks a prerelease.
if printf '%s' "$version" | grep -Eq -- '^[0-9]+\.[0-9]+\.[0-9]+-'; then
  prerelease=true
else
  prerelease=false
fi

emit() {
  printf '%s\n' "$1"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    printf '%s\n' "$1" >> "$GITHUB_OUTPUT"
  fi
}

emit "version=$version"
emit "prerelease=$prerelease"
