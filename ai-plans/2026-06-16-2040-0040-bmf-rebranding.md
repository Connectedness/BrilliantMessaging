# 0040 - BMF Rebranding

## Rationale

The project is being renamed from **USF** to **BMF**. The GitHub repository has already been
renamed; this plan covers adapting the codebase. Nothing is published yet (no NuGet packages, no
clients), so all renames — including public API and observable telemetry contracts — are safe
breaking changes. The `Usf` token appears ~600 times across project files, namespaces, public
types, telemetry strings, tooling config, and docs.

## Acceptance criteria

- [ ] All six projects are renamed (folder + `.csproj`): `Usf.Abstractions`, `Usf.Core`,
      `Usf.Transport.RabbitMq`, `Usf.Core.Tests`, `Usf.Transport.RabbitMq.Tests`, `Usf.Benchmarks`
      → `Bmf.*`. Assembly names and root namespaces follow automatically (no overrides exist).
- [ ] `USF.slnx` → `BMF.slnx`, with all internal `<Project>`/`<File>` paths updated.
- [ ] `USF.sln.DotSettings` → `BMF.sln.DotSettings` (and its reference inside the solution file).
- [ ] All namespace declarations and `using` statements move from `Usf.*` to `Bmf.*`.
- [ ] Public API identifiers renamed: `UsfBuilder` → `BmfBuilder`,
      `UsfServiceCollectionExtensions` → `BmfServiceCollectionExtensions`,
      `AddUsf()` → `AddBmf()`, `UsfUuid` → `BmfUuid`.
- [ ] Telemetry contracts renamed: the `"Usf.Outbound"` `ActivitySource`/`Meter` name →
      `"Bmf.Outbound"`, and all `usf.outbound.*` metric/tag/activity names → `bmf.outbound.*`.
      Tests asserting these strings are updated in lockstep.
- [ ] `.idea/.idea.USF/` folder → `.idea/.idea.BMF/`, and the matching `.gitignore` entries.
- [ ] CI workflow (`.github/workflows/ci.yml`) references `BMF.slnx`.
- [ ] Documentation updated: `AGENTS.md`, `tests/AGENTS.md`, the RabbitMQ transport `README.md`,
      and XML doc comments mentioning USF.
- [ ] `ai-plans/` is **not** modified — historical plans are immutable records.
- [ ] `dotnet build BMF.slnx --configuration Release` succeeds with no warnings. All automated tests complete successfully
- [ ] No `usf` tokens remain outside `ai-plans/`:
      `git grep -ilE 'usf' -- . ':(exclude)ai-plans/*'` returns nothing (201 files currently match).

## Technical details

The codebase has no `RootNamespace`, `AssemblyName`, or `PackageId` overrides in any `.csproj`,
`Directory.Build.props`, or `Directory.Packages.props`, so each assembly name and root namespace
is derived from its project file name. Renaming the `.csproj` files (and their folders) therefore
propagates to assembly names and default namespaces for free.

Execution order matters to keep contents and paths consistent and to let git detect renames:

1. **In-file text replacements first**, on the still-named files: namespace declarations and
   `using` statements (`Usf.` → `Bmf.`), public identifiers (`UsfBuilder`,
   `UsfServiceCollectionExtensions`, `AddUsf`, `UsfUuid`), telemetry strings (`Usf.Outbound`,
   `usf.outbound.*`), solution/`.csproj` path references, CI `.slnx` references, `.gitignore`
   entries, and docs. Scope the sweep to exclude `ai-plans/`.
2. **Then move files/folders** with `git mv`: the six project directories and their `.csproj`
   files, `USF.slnx`, `USF.sln.DotSettings`, and `.idea/.idea.USF/`.
3. **Build to verify**: `dotnet build BMF.slnx --configuration Release` (warnings-as-errors in
   Release catches stragglers).

Most occurrences are the case-sensitive PascalCase `Usf` (project/namespace/type). Watch the two
other casings: the all-caps `USF` in the solution file name, CI, `.gitignore`/`.idea` paths, and
prose; and the lowercase `usf` only in telemetry strings (`usf.outbound.*`). No automated tests
beyond updating existing assertions are required — this is a mechanical rename, and the existing
suite plus the Release build are the regression guard.
