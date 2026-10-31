# Release Drafter and NuGet Package Metadata

## Rationale

Complete the NuGet release workflow with validated versions, consistent package branding and tags,
and GitHub releases whose notes are maintained by Release Drafter.

## Acceptance Criteria

- [x] Every generated BMF NuGet package (including the nested `Bmf.Transport.RabbitMq` package)
      contains `design/logo-128x128.png` at the package root as `logo-128x128.png`, matching the
      existing `PackageIcon` metadata already configured in `src/Directory.Build.props`.
- [x] Every generated BMF NuGet package declares the tags
      `messaging;communication;rabbitmq;amqp;bmf;cloudevents`.
- [x] The existing `<NoWarn>` element is completely removed from `src/Directory.Build.props`.
- [x] `.github/release-drafter.yml` configures a shared release-note template and sensible change
      categories for BMF.
- [x] A GitHub Actions workflow updates the `vNext` draft GitHub release through Release Drafter
      after every merge to `main`.
- [x] The NuGet release workflow rejects versions that do not match
      `^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$` before the value is used by restore, pack,
      publication, tagging, or release creation.
- [x] The NuGet release workflow uses Release Drafter to generate release notes for the validated
      version and overrides the draft version, name, and tag with `v<version>`.
- [x] The NuGet release workflow exposes a `create_release` boolean input that defaults to `true`.
- [x] When package publication succeeds and `create_release` is `true`, the workflow publishes a
      GitHub release for `v<version>` using the Release Drafter notes; no GitHub release is published
      when `create_release` is `false`.
- [x] A version with a `-<suffix>` is published as a GitHub prerelease, while a version without a
      suffix is published as the latest GitHub release.
- [x] The non-publishing workflow path remains available and cannot accidentally create a GitHub
      release.
- [x] Automated verification confirms the package icon and tags are present in every generated
      `.nupkg` without publishing packages or releases.
- [x] Automated coverage exercises the shared version script with stable, prerelease, and invalid
      inputs without publishing packages or releases.

## Technical Details

Keep shared NuGet metadata in `src/Directory.Build.props` so it applies to all three packable source
projects. The `PackageIcon` metadata and the logo `<None>` pack item already exist there; keep them,
but fix the logo `<None Include>` path to anchor on `$(MSBuildThisFileDirectory)` (e.g.
`$(MSBuildThisFileDirectory)../../design/logo-128x128.png`). Imported-item relative paths resolve
against the consuming project's directory, so the current unanchored `../../design/...` path omits
the asset from the nested `src/Transports/Bmf.Transport.RabbitMq` package; anchoring makes it resolve
for all three projects regardless of nesting depth. Add the shared `PackageTags` value there. Remove
the existing `NoWarn` property rather than leaving an empty element or relocating the suppression: the
maintainers have agreed that warnings are treated as errors and that public APIs always carry XML
documentation, so CS1591 must be satisfied by real doc comments — the Release pack runs with
`TreatWarningsAsErrors`, so any undocumented public member breaks the build. Add a verification step
to the always-running (non-publishing) pack job that inspects each `.nupkg` and asserts that both the
icon file and expected metadata are present.

Use the existing `.github/release-drafter.yml` as the shared configuration for both workflows; it
already defines the release-note template, categorized changes, branch filtering, and `vNext` draft
naming. Add a dedicated workflow under `.github/workflows/` that runs on pushes to `main`, grants
only the `contents: write` and pull-request read access required by Release Drafter, and creates or
updates the single rolling `vNext` draft. Pin the Release Drafter action to a stable major version.

Extract version validation and release classification into a repository script shared by automated
tests and the release workflow. The script validates the supplied value with `grep -Eq` against the
acceptance-criteria expression and reports whether the version is stable or has a `-<suffix>` and is
therefore a prerelease. Invoke it as the first release operation, before decoding the signing key or
running any command that consumes the version. Pass `${{ inputs.version }}` through an environment
variable rather than interpolating untrusted workflow input into shell source. All pack, publish,
tag, and release steps must depend on successful validation and use only that validated value. Add
table-driven coverage as a standalone shell test, run as a regular CI step on the Linux runner
(independent of the .NET/xUnit suite), that drives the script with accepted stable and prerelease
values and rejected malformed values without requiring signing or publishing credentials.

Add the typed `create_release` `workflow_dispatch` input with a default of `true`. After successful
NuGet publication, use Release Drafter to turn the current draft notes into a GitHub release when
both `publish` and `create_release` are true. Override Release Drafter's version input with the
validated workflow input and its name and tag with `v<version>` so the shared configuration's
`vNext` placeholders are never used for a published release. Use the script's classification to
mark suffix versions as prereleases; mark stable versions as the latest release. Keep
`contents: write` scoped to the release job. A package-only run (`publish: false`) must never publish
a GitHub release, regardless of the `create_release` default; `create_release: false` must preserve
the draft for later use. Ensure a failed package publication cannot leave behind a published GitHub
release.
