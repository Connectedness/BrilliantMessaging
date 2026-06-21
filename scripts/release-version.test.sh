#!/usr/bin/env bash
# Table-driven coverage for release-version.sh. Runs independently of the .NET
# test suite and needs no signing or publishing credentials, so it is safe to
# run as a plain CI step. GITHUB_OUTPUT is forced empty for every case so the
# tests never write to a real step-output file.
set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
target="$script_dir/release-version.sh"

failures=0

# Accepted versions and the prerelease classification each must report.
# Format: "<version>|<expected prerelease>"
accepted=(
  "0.1.0|false"
  "1.2.3|false"
  "10.20.30|false"
  "1.2.3-dev|true"
  "1.0.0-rc.1|true"
  "1.2.3-alpha-1|true"
  "0.1.0-preview.2|true"
)

# Versions that must be rejected with a non-zero exit.
rejected=(
  ""
  "1.2"
  "1.2.3.4"
  "v1.2.3"
  "1.2.3-"
  "1.2.3-beta!"
  "1.2.3 "
  " 1.2.3"
  $'1.2.3\ninvalid'
  $'1.2.3\nprerelease=true'
  $'1.2.3\r'
  "abc"
)

for case in "${accepted[@]}"; do
  version="${case%%|*}"
  expected="${case##*|}"
  if ! output="$(RELEASE_VERSION="$version" GITHUB_OUTPUT="" bash "$target" 2>/dev/null)"; then
    echo "FAIL: '$version' should be accepted but was rejected"
    failures=$((failures + 1))
    continue
  fi
  if ! printf '%s\n' "$output" | grep -qx "prerelease=$expected"; then
    echo "FAIL: '$version' expected prerelease=$expected, got: ${output//$'\n'/ }"
    failures=$((failures + 1))
    continue
  fi
  echo "PASS: '$version' accepted (prerelease=$expected)"
done

for version in "${rejected[@]}"; do
  if RELEASE_VERSION="$version" GITHUB_OUTPUT="" bash "$target" >/dev/null 2>&1; then
    echo "FAIL: '$version' should be rejected but was accepted"
    failures=$((failures + 1))
    continue
  fi
  echo "PASS: '$version' rejected"
done

if [ "$failures" -gt 0 ]; then
  echo "$failures test case(s) failed" >&2
  exit 1
fi

echo "All release-version.sh test cases passed."
