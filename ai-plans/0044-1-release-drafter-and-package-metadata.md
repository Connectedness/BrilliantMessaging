# Release Drafter and NuGet Package Metadata

## Rationale

Complete the NuGet release workflow with validated versions, consistent package branding and tags,
and GitHub releases whose notes are maintained by Release Drafter.

Issue: [#44](https://github.com/Connectedness/BMF/issues/44)

## Acceptance Criteria

- [ ] Every generated BMF NuGet package contains `design/logo-128x128.png` as its package icon.
- [ ] Every generated BMF NuGet package declares the tags
      `messaging;communication;rabbitmq;amqp;bmf;cloudevents`.
- [ ] The existing `<NoWarn>` element is completely removed from `src/Directory.Build.props`.
- [ ] `.github/release-drafter.yml` configures a shared release-note template and sensible change
      categories for BMF.
- [ ] A GitHub Actions workflow updates the `vNext` draft GitHub release through Release Drafter
      after every merge to `main`.
- [ ] The NuGet release workflow rejects versions that do not match
      `^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$` before the value is used by restore, pack,
      publication, tagging, or release creation.
- [ ] The NuGet release workflow uses Release Drafter to generate release notes for the validated
      version.
- [ ] The NuGet release workflow exposes a `create_release` boolean input that defaults to `true`.
- [ ] When package publication succeeds and `create_release` is `true`, the workflow publishes a
      GitHub release for `v<version>` using the Release Drafter notes; no GitHub release is published
      when `create_release` is `false`.
- [ ] The non-publishing workflow path remains available and cannot accidentally create a GitHub
      release.
- [ ] Automated verification confirms the package icon and tags in every generated `.nupkg`, and
      validates the version-acceptance and rejection cases without publishing packages or releases.

## Technical Details

Keep shared NuGet metadata in `src/Directory.Build.props` so it applies to all three packable source
projects. Set `PackageIcon` to `logo-128x128.png`, pack `design/logo-128x128.png` at the package root,
and add the shared `PackageTags` value there. Remove the existing `NoWarn` property rather than
leaving an empty element or relocating the suppression. Extend the non-publishing package
verification to inspect each `.nupkg` and assert that both the icon file and expected metadata are
present.

Add `.github/release-drafter.yml` with the release-note template, change categories, and `vNext`
draft naming used by both workflows. Add a dedicated workflow under `.github/workflows/` that runs
on pushes to `main`, grants only the `contents: write` and pull-request read access required by
Release Drafter, and creates or updates the single rolling `vNext` draft. Pin the Release Drafter
action to a stable major version.

Add version validation as the first release operation, before decoding the signing key or running
any command that consumes the version. Pass `${{ inputs.version }}` into the shell through an
environment variable and validate it with `grep -Eq` against the acceptance-criteria expression;
using an environment variable avoids interpolating untrusted workflow input into shell source. All
pack, publish, tag, and release steps must depend on successful validation and use only that
validated value.

Add the typed `create_release` `workflow_dispatch` input with a default of `true`. After successful
NuGet publication, use Release Drafter to turn the current draft notes into a GitHub release named
and tagged `v<version>` when both `publish` and `create_release` are true. Keep `contents: write`
scoped to the release job. A package-only run (`publish: false`) must never publish a GitHub release,
regardless of the `create_release` default; `create_release: false` must preserve the draft for later
use. Ensure a failed package publication cannot leave behind a published GitHub release.
