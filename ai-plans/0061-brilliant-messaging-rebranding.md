# Brilliant Messaging Rebranding

## Rationale

Publishing the `Bmf.*` packages to nuget.org failed with HTTP 409 — *"The package ID is
reserved."* The project is therefore being rebranded from **BMF** to **Brilliant Messaging**, and
the package ID prefix from `Bmf.*` to `BrilliantMessaging.*`. Nothing has been published yet and
there are no external clients, so all renames — including public API and observable telemetry
contracts — are safe breaking changes.

This is the same class of mechanical rename as the earlier `Usf → Bmf` rebrand
(`ai-plans/0040-bmf-rebranding.md`), with two differences that need extra care this time: the
`.csproj` files carry `Description` text (and redundant explicit `PackageId`s) that mention the old
name, and there is package metadata (`PackageTags`) plus a CI guard script
(`scripts/verify-packages.sh`) that hard-codes the `bmf` tag. The `bmf` token appears in ~257 files
outside `ai-plans/`.

## Naming conventions

- PascalCase token `Bmf` (projects, namespaces, public types): → `BrilliantMessaging`.
- Lowercase token `bmf` (telemetry strings, package tag, ids): → `brilliantmessaging`.
- All-caps `BMF` used for internal infrastructure file/folder names: → `BrilliantMessaging`. The
  exception is GitHub Actions repository **secrets** (e.g. `BMF_SNK`), which keep their current
  names — see the CI criterion.
- The framework's full name "The Brilliant Messaging Framework" is dropped as a tagline; the
  product is simply **Brilliant Messaging**.
- The GitHub repository (`Connectedness/BMF`) is **out of scope** — it will be renamed manually
  later. Leave `RepositoryUrl`/`PackageProjectUrl`/badge URLs pointing at `.../BMF` untouched.
- The logo artwork itself (`design/logo-128x128.png`, monogram SVGs, favicon, NuGet icon usage)
  stays as-is; only text/wordmarks change.

## Acceptance criteria

- [x] All eight projects renamed (folder + `.csproj`): `Bmf.Abstractions`, `Bmf.Core`,
      `Bmf.OpenTelemetry`, `Bmf.Transport.RabbitMq`, `Bmf.Benchmarks`, `Bmf.Core.Tests`,
      `Bmf.OpenTelemetry.Tests`, `Bmf.Transport.RabbitMq.Tests` → `BrilliantMessaging.*`. Assembly
      names and root namespaces follow from the file name (no overrides exist for those).
- [x] `BMF.slnx` → `BrilliantMessaging.slnx`, with every `<Project>`/`<File>` path updated, and all
      `<ProjectReference>` paths inside the `.csproj` files updated to the new folders.
- [x] `BMF.sln.DotSettings` → `BrilliantMessaging.sln.DotSettings` (and its reference in the
      solution file). The companion `BMF.sln.DotSettings.user` is untracked but should be renamed
      locally too so Rider keeps its settings.
- [x] All namespace declarations and `using` statements move from `Bmf.*` to `BrilliantMessaging.*`.
- [x] The redundant explicit `<PackageId>` elements in the four packable projects
      (`Bmf.Abstractions`/`Bmf.Core`/`Bmf.OpenTelemetry`/`Bmf.Transport.RabbitMq`) are **removed**,
      letting the package ID default to the renamed project name. Each `<Description>` reworded to
      use "Brilliant Messaging".
- [x] `PackageTags` in `src/Directory.Build.props`: `bmf` tag → `brilliantmessaging`, **and** the
      matching `expected_tags` entry in `scripts/verify-packages.sh` updated in lockstep.
- [x] Public API identifiers renamed: `BmfBuilder` → `BrilliantMessagingBuilder`,
      `BmfServiceCollectionExtensions` → `BrilliantMessagingServiceCollectionExtensions`,
      `AddBmf()` → `AddBrilliantMessaging()`, `BmfUuid` → `BrilliantMessagingUuid`,
      `BmfInstrumentationExtensions` → `BrilliantMessagingInstrumentationExtensions`,
      `AddBmfInstrumentation()` → `AddBrilliantMessagingInstrumentation()`. Test types named after
      them (`BmfBuilderTests`, `AddBmfInstrumentationTests`) renamed accordingly.
- [x] Telemetry contracts renamed, with the tests that assert these literals updated in lockstep:
      - `ActivitySource`/`Meter` names `"Bmf.Outbound"`/`"Bmf.Inbound"` →
        `"BrilliantMessaging.Outbound"`/`"BrilliantMessaging.Inbound"`.
      - Activity/operation names `"bmf.outbound.publish"`, `"bmf.inbound.process"`,
        `"bmf.outbound.topology.provision"` → `brilliantmessaging.*`.
      - Metric names `"bmf.outbound.topology.provisioning.{attempts,failures,duration}"` and tag
        names `"bmf.outbound.transport.name"`, `"bmf.outbound.outcome"` → `brilliantmessaging.*`.
- [x] Internal infrastructure renamed: strong-name key `BMF.snk` → `BrilliantMessaging.snk` (and
      its `.gitignore` entry); `.idea/.idea.BMF/` → `.idea/.idea.BrilliantMessaging/`.
- [x] CI/release workflows updated: `.slnx` references in `ci.yml` and `nuget-release.yml`, and the
      decoded key **file** name `BMF.snk` → `BrilliantMessaging.snk` in `nuget-release.yml`. The
      GitHub Actions **secret** `BMF_SNK` keeps its current name — leave the `secrets.BMF_SNK`
      reference (and the `BMF_SNK` env var) unchanged.
- [x] README branding (`README.md`, `README.nuget.md`): hero wordmark/alt text spells out
      "Brilliant Messaging"; the `<em>The Brilliant Messaging Framework</em>` subtitle line is
      removed; package names and example code (`Bmf.Core`, `.AddBmf()`, `.AddBmfInstrumentation()`,
      `BmfUuid`, the `Bmf.Outbound`/`Bmf.Inbound` OTel source names) updated to the new names.
- [x] Design assets updated: `design/hero-light.svg`/`design/hero-dark.svg` wordmark text spells
      out "Brilliant Messaging"; the `design/logo/` vite project (`index.html`, `concept2.html`,
      `concept3.html`, `downloads.html`, `src/`, `package.json` `"name"`/`"description"`) updated,
      with the "BMF — The Brilliant Messaging Framework · logo exploration" footer removed. Rename
      the `design/logo/src/logos/bmf-*.svg` files and `design/bmf_wallet.png` to a
      `brilliantmessaging-*` prefix and fix references to them.
- [x] Documentation/prose updated: `AGENTS.md`, `tests/AGENTS.md`, and XML doc comments that name
      "BMF" use "Brilliant Messaging".
- [x] `ai-plans/` is **not** modified — historical plans are immutable records.
- [x] `dotnet build BrilliantMessaging.slnx --configuration Release` succeeds with no warnings, and
      all automated tests pass.
- [x] No `bmf` tokens remain outside `ai-plans/` **except the two intentionally retained ones** —
      the `Connectedness/BMF` repo URL and the `BMF_SNK` CI secret:
      `git grep -inE 'bmf' -- . ':(exclude)ai-plans/*' | grep -viE 'Connectedness/BMF|BMF_SNK'`
      returns nothing (257 files currently match the unfiltered grep).

## Technical details

The four library `.csproj` files set `PackageId` explicitly, but those values just duplicate the
project name, so they default to `$(MSBuildProjectName)` once the projects are renamed — delete the
`<PackageId>` elements rather than maintain them. Root namespaces and assembly names likewise have
no overrides, so they derive from the renamed `.csproj` file name automatically. Renaming a project
folder still requires updating the `<ProjectReference Include="..\Bmf.X\Bmf.X.csproj" />` paths in
the projects that reference it and the `<Project Path>` entries in the solution file.

Execution order matters so file contents and paths stay consistent and git can detect renames:

1. **In-file text replacements first**, on the still-named files. Run the sweep scoped to exclude
   `ai-plans/` (e.g. `git grep` pathspec `':(exclude)ai-plans/*'`). Mind the three casings:
   - PascalCase `Bmf` → `BrilliantMessaging`: namespaces, `using`s, public identifiers,
     `.csproj`/solution path references, type names in docs. (`Bmf` only ever appears as the intended
     token, never as a substring of another word, so the replacement is safe — but restrict the
     sweep to text files; binary assets like `bmf_wallet.png` are handled by `git mv` in step 2.)
     Before sweeping, confirm there are no `InternalsVisibleTo`/`AssemblyName` references to update
     (`git grep -n InternalsVisibleTo` — likely none, since the library favors `public`).
   - lowercase `bmf` → `brilliantmessaging`: telemetry strings, the `bmf` package tag (in both
     `src/Directory.Build.props` and `scripts/verify-packages.sh`), the `bmf-logo` package name and
     `bmf-*.svg`/`bmf_wallet` asset references.
   - all-caps `BMF` → `BrilliantMessaging`: `.slnx`/`.snk`/`.idea` paths in `.gitignore` and
     CI/release workflows. Leave the `BMF_SNK` secret reference unchanged.
   Then apply the prose/branding edits (README hero + subtitle removal, design wordmarks/footer,
   `Description` rewordings).
2. **Then move files/folders** with `git mv`: the eight project directories and their `.csproj`
   files, `BMF.slnx`, `BMF.sln.DotSettings`, `BMF.snk` (if present locally), the
   `design/logo/src/logos/bmf-*.svg` and `design/bmf_wallet.png` assets, and `.idea/.idea.BMF/`.
3. **Clean, then build to verify**: delete stale build outputs first (`dotnet clean`, or remove the
   `bin/`/`obj/` trees) so a lingering `Bmf.Core.dll` from a previous build cannot resolve and mask a
   missed reference. Then `dotnet build BrilliantMessaging.slnx --configuration Release`
   (warnings-as-errors in Release surfaces any stragglers) and run the test suite. The existing tests
   plus the Release build are the regression guard — no new tests are required beyond updating the
   assertions that pin the renamed identifiers and telemetry literals.

### Commit checkpoints

Keep the content edits and the file moves in **separate commits** so git records the moves as pure
renames (a content rewrite combined with a move in one commit can fall below git's rename-detection
threshold and show as delete+add):

1. **Content sweep** (step 1 above): all in-file replacements and prose/branding edits, on the
   still-named files. This commit intentionally does **not** build — namespaces and
   `ProjectReference`/`.slnx` paths now point at `BrilliantMessaging.*` locations that do not exist
   yet.
2. **Renames** (step 2 above): the `git mv` moves of folders, `.csproj`, `.slnx`,
   `.sln.DotSettings`, `.snk`, `.idea/`, and design assets. The solution builds again after this
   commit — run the build and tests (step 3) before committing, and fold any straggler fixes in
   here.

If history must be bisectable (every commit green), squash these into a single rename commit
instead. Either way, the plan-file commit and this rebrand should stay separate.

One follow-up cannot be completed from the codebase and is flagged for manual action (later):
renaming the GitHub repository. The GitHub Actions secret `BMF_SNK` is intentionally left as-is, so
no secret re-creation is needed — only the decoded `.snk` file it is written to is renamed.
