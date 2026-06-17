# NuGet Release Workflow

## Rationale

BMF needs a release workflow that turns a manually triggered release into signed NuGet packages
published to `nuget.org`. The workflow should build the repository in Release mode, sign all
produced assemblies, package the libraries, and publish only when the package artifacts are ready
and validated.

Issue: [#44](https://github.com/Connectedness/BMF/issues/44)

## Acceptance Criteria

- [ ] A manually triggered GitHub Actions workflow exists for NuGet releases.
- [ ] The workflow exposes a required `version` input on `workflow_dispatch`, defaulting to `0.1.0`.
- [ ] The workflow builds BMF in Release configuration.
- [ ] The root `Directory.Build.props` defines NuGet/package metadata with `<Copyright>`,
      `<Company>`, and `<Authors>`, but does not define `<Version>`.
- [ ] All assemblies included in NuGet packages are signed.
- [x] Signed Release builds suppress the benign CS8002 warning caused by the non-strong-named
      `Generator.Equals.Runtime` dependency.
- [ ] NuGet packages are created for every project under `./src/` that does not set `IsPackable=false`.
- [ ] All projects outside `./src/` set `IsPackable=false` so solution-level packing only produces
      library packages.
- [ ] The workflow decodes the base64-encoded `BMF_SNK` secret to `./BMF.snk` before building or
      packing signed assemblies.
- [ ] The workflow removes `./BMF.snk` from the runner filesystem even when an earlier step fails.
- [x] `BMF.snk` is listed in `.gitignore` so the signing key is never committed from a working tree.
- [ ] Package publishing uses NuGet trusted publishing with GitHub OIDC instead of a long-lived API key.
- [ ] The workflow publishes packages to `nuget.org` through the trusted publishing temporary API key.
- [ ] A non-publishing verification path restores, builds, and creates signed package artifacts in
      Release configuration without pushing to `nuget.org`.

## Technical Details

Add a dedicated workflow under `.github/workflows/` for release publishing. It should only run via
`workflow_dispatch`; do not trigger publishing from pushes, pull requests, releases, or version
tags. The manual trigger must require a `version` input, defaulting to `0.1.0`, using this shape:

```yaml
on:
  workflow_dispatch:
    inputs:
      version:
        description: 'Semantic version to release (e.g. 1.2.3 or 1.2.3-dev)'
        required: true
        default: '0.1.0'
```

Use `${{ github.event.inputs.version }}` as the package version during build/pack. Use `dotnet
restore`, `dotnet build --configuration Release`, and an explicit pack command shaped like:

```bash
dotnet pack ./BMF.slnx --configuration Release /p:SignAssembly=true /p:AssemblyOriginatorKeyFile=${{ github.workspace }}/BMF.snk /p:ContinuousIntegrationBuild=true /p:PackageRequireLicenseAcceptance=false /p:SymbolPackageFormat=snupkg /p:Version=<version>
```

Replace `<version>` with `${{ github.event.inputs.version }}` in the workflow. Pass the signing key as
an **absolute** path so it resolves regardless of each packed project's directory depth — `src/Bmf.Core`
is two levels deep while `src/Transports/Bmf.Transport.RabbitMq` is three, so a single relative value
cannot resolve for all of them. Use the runner-absolute `${{ github.workspace }}/BMF.snk` (which is
where the secret is decoded), never a relative path. Do **not** put `SignAssembly`,
`AssemblyOriginatorKeyFile`, or any other signing properties into `Directory.Build.props` or the
`.csproj` files: the signing key is not committed to the repository, so baking signing into the
projects would force every local/non-release build to require a key that may not be present. Signing is
enabled only by these explicit `dotnet pack` parameters in the release workflow, which keeps debug,
test, benchmark, and local builds unsigned and key-free.

Update the root `Directory.Build.props` with shared package metadata for `<Copyright>`,
`<Company>`, and `<Authors>`. Do not add `<Version>` to `Directory.Build.props`; releases should
take their package version from the workflow input. Keep project-specific package metadata in the
individual publishable projects when it differs per package, but centralize values that should
remain consistent across all BMF packages.

Assembly signing is enabled only by the explicit pack-command parameters above, using the existing
GitHub secret `BMF_SNK`. The secret contains the SNK file as one base64-encoded line. The workflow
must decode it to `./BMF.snk` before pack, pass that key file to the pack command's signing
parameters, and avoid committing private keys or secret-derived artifacts. Add `BMF.snk` to
`.gitignore` so the key can never be committed from a local working tree. Add a final cleanup step
with `if: always()` that removes `./BMF.snk` so the key file is deleted from the runner filesystem
even when restore, build, pack, or publish fails. Because signing is applied only by the pack
parameters, debug, test, benchmark, and local builds never sign and never require the key.

Publishable projects should have stable package metadata (`PackageId`, authors, description,
license/readme metadata, repository URL, symbols/source-link settings if applicable). Non-package
projects outside `./src/` must set `IsPackable=false` before solution-level packing is used: set it
once in the existing `tests/Directory.Build.props` (which covers both test projects) and, because
`benchmarks/` has no `Directory.Build.props`, either add one that sets `IsPackable=false` or set the
property directly on `benchmarks/Bmf.Benchmarks/Bmf.Benchmarks.csproj`. The root `Directory.Build.props`
and `src/Directory.Build.props` already exist; keep signing out of all of them (see above). The
publishing step should push the generated `.nupkg` files with `dotnet nuget push` and fail clearly if a
package cannot be published.

Use NuGet trusted publishing rather than a stored NuGet API key. The release job needs
`permissions: id-token: write` so GitHub Actions can issue an OIDC token. Shortly before pushing
packages, use `NuGet/login@v1` with the `NUGET_USER` repository variable as the nuget.org
user/profile name to exchange that token for a temporary NuGet API key, then pass the action output
to `dotnet nuget push`. The nuget.org trusted publishing policy must be configured
separately for the `Connectedness/BMF` repository and the workflow file name only, matching NuGet's
policy rules.

Strong naming the BMF assemblies (`SignAssembly=true`) raises **CS8002** because the transitive
`Generator.Equals.Runtime` dependency is not strong-named, and `Directory.Build.props` promotes
warnings to errors in Release. CS8002 is a compile-time warning only: neither the .NET (Core) nor
the .NET Framework CLR enforces strong-name verification of referenced assemblies at load time, so
consumers (including .NET Framework 4.8) are unaffected at runtime. The only practical limitation is
that the package graph cannot be installed into the GAC, which is irrelevant for app-local NuGet
consumption. Suppress CS8002 centrally via `<NoWarn>` in the root `Directory.Build.props` so it
covers `Bmf.Core` and every project that transitively references `Generator.Equals.Runtime`.

Validation should include a Release restore/build and package creation path that can be exercised
without publishing. If the publish workflow itself cannot be fully tested without live credentials,
factor packaging/signing logic into scripts or MSBuild targets that can be checked by regular CI and
produce signed `.nupkg`/`.snupkg` artifacts without invoking `dotnet nuget push`. No separate automated
unit tests are required for this change; the non-publishing verification path is the verification story.
