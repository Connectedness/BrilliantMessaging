#!/usr/bin/env bash
# Verifies that every NuGet package in the given directory embeds the package
# icon and README at its root and declares the expected tags. Symbol packages
# (.snupkg) are ignored. Exits non-zero if any package is missing the icon,
# README, or tags, so it can guard the pack job without publishing anything.
set -uo pipefail

package_dir="${1:-artifacts/packages}"
expected_icon='logo-128x128.png'
expected_readme='README.md'
expected_tags=(messaging communication rabbitmq amqp bmf cloudevents)

shopt -s nullglob
packages=("$package_dir"/*.nupkg)
shopt -u nullglob

if [ ${#packages[@]} -eq 0 ]; then
  echo "error: no .nupkg files found in '$package_dir'" >&2
  exit 1
fi

failures=0
for package in "${packages[@]}"; do
  name="$(basename "$package")"
  package_ok=1

  # List the archive once into a variable rather than piping into `grep -q`.
  # Under `pipefail`, `grep -q` closes the pipe on its first match and `unzip`
  # then dies with SIGPIPE, whose non-zero status would propagate and spuriously
  # report a missing file.
  listing="$(unzip -Z1 "$package")"

  if ! grep -qx "$expected_icon" <<< "$listing"; then
    echo "FAIL: $name is missing '$expected_icon' at the package root"
    package_ok=0
  fi

  if ! grep -qx "$expected_readme" <<< "$listing"; then
    echo "FAIL: $name is missing '$expected_readme' at the package root"
    package_ok=0
  fi

  nuspec="$(unzip -p "$package" '*.nuspec' 2>/dev/null || true)"
  tag_value="$(printf '%s' "$nuspec" | tr '\r\n' '  ' | sed -n 's:.*<tags>\([^<]*\)</tags>.*:\1:p')"
  normalized_tags="${tag_value//;/ }"
  read -r -a package_tags <<< "$normalized_tags"

  missing_tags=()
  for expected_tag in "${expected_tags[@]}"; do
    tag_found=0
    for package_tag in "${package_tags[@]}"; do
      if [ "$package_tag" = "$expected_tag" ]; then
        tag_found=1
        break
      fi
    done
    if [ "$tag_found" -eq 0 ]; then
      missing_tags+=("$expected_tag")
    fi
  done

  if [ ${#missing_tags[@]} -gt 0 ]; then
    echo "FAIL: $name is missing expected tag(s): ${missing_tags[*]}"
    package_ok=0
  fi

  if [ "$package_ok" -eq 1 ]; then
    echo "PASS: $name"
  else
    failures=$((failures + 1))
  fi
done

if [ "$failures" -gt 0 ]; then
  echo "error: $failures package check(s) failed" >&2
  exit 1
fi

echo "All package checks passed (${#packages[@]} package(s))."
