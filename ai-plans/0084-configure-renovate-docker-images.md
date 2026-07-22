# Configure Renovate to update Docker image constants

> Issue: [#84](https://github.com/Connectedness/BrilliantMessaging/issues/84)

## Rationale

The Docker images used by the Testcontainers-based integration tests are pinned in C# string constants, which no built-in Renovate manager recognizes. As a result these images silently go stale while all other dependencies are kept current. Renovate should treat those constants as Docker dependencies and propose tag updates alongside the matching transport dependency groups.

## Acceptance Criteria

- [x] The custom manager configuration recognizes the RabbitMQ image constant in `tests/Transports/BrilliantMessaging.Transport.RabbitMq.Tests/TestSupport/DockerImages.cs`.
- [x] The custom manager configuration recognizes the NATS image constant in `tests/Transports/BrilliantMessaging.Transport.Nats.Tests/TestSupport/DockerImages.cs`.
- [x] The custom manager configuration uses the `docker` datasource and `docker` versioning.
- [x] The `RabbitMQ` package rule group additionally matches the detected `rabbitmq` Docker package.
- [x] A `NATS` package rule group exists that matches `NATS.Net`, `Testcontainers.Nats`, and the detected `nats` Docker package.
- [x] `renovate.json` passes `renovate-config-validator`, and a `renovate --dry-run` confirms both images are extracted and correctly grouped.
- [x] The verification output is recorded in the PR description.

## Technical Details

Both constants live in files named `DockerImages.cs` and share the shape `public const string <Name> = "<image>:<tag>";`. A single custom regex manager can therefore cover both transports; use two managers only if the capture groups would otherwise become ambiguous.

Manager configuration:

- `customType`: `regex`, following the existing `dotnet-reportgenerator-globaltool` manager in `renovate.json` as the style reference.
- `managerFilePatterns`: restrict to the Testcontainers `TestSupport/DockerImages.cs` files rather than matching all C# sources.
- `matchStrings`: capture the image name into `depName` and the tag into `currentValue` from the constant's string literal. Keep `depName` free of `/` — a permissive capture also matches URL constants such as `nats://localhost:4222` and yields a bogus dependency.
- `datasourceTemplate`: `docker`, `versioningTemplate`: `docker`.

`config:recommended` sets `ignorePaths` to a list including `**/tests/**`, and custom managers cannot see files underneath it — the manager matches zero files and no error is reported. `ignorePaths` must therefore be overridden to the recommended list minus `**/tests/**`, which is a repo-wide change and the single most likely reason an otherwise-correct manager appears to do nothing.

The two tags differ in shape — `4.3.1-management-alpine` (three-part, suffixed) versus `2.11-alpine` (two-part, suffixed). Docker versioning handles both, but updates stay within the same suffix and the NATS image is tracked at minor granularity, so the absence of patch-level NATS updates is expected rather than a sign of a broken regex.

Grouping is expressed in `packageRules`. The existing `RabbitMQ` group gains `rabbitmq` in `matchPackageNames`; a new `NATS` group is added covering the NATS transport packages introduced by #82 plus `nats`. Docker Hub official images are matched by their bare name — no `library/` prefix — so the names the manager captures are what the grouping rules use directly.

Note that `major` updates are gated behind `dependencyDashboardApproval` repo-wide, so major image bumps will surface on the dashboard instead of as automatic PRs; no additional configuration is needed for that.

Verification: run `renovate-config-validator` for schema correctness, then a `renovate --dry-run` against the repo with debug logging to confirm the managers actually extract `rabbitmq` and `nats` with the expected tags. Schema validation alone is insufficient — a regex that matches nothing still validates. The dry-run output is also what confirms the `depName` values the grouping rules must match. Both are run locally; no CI step is added for this. Both tools ship with the `renovate` npm package and can be run ad hoc via `npx --yes`; the repo has no Node project of its own. No GitHub token is required: `RENOVATE_PLATFORM=local` runs against the working directory, and `RENOVATE_DRY_RUN=extract` is enough to confirm extraction. No automated test coverage applies to this change.
