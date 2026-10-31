# Root Agents.md

Brilliant Messaging is a lightweight framework for modern cloud-native applications. It consists of an asynchronous messaging system and a Saga framework / workflow engine.

## General Rules for the Code Base

- Implicit usings or global usings are not allowed - use explicit using statements for clarity.
- The library is not published in a stable version yet, you can make breaking changes.
- `<TreatWarningsAsErrors>` is enabled in Release builds, so your code changes must not generate warnings.
- Prefer `public` over `internal` when designing types. Users of the library should have access to the same APIs as we do. Hide lower level APIs in plain sight, see https://blog.ploeh.dk/2015/09/21/public-types-hidden-in-plain-sight/.

## Feedback Loops

- `dotnet build BrilliantMessaging.slnx --configuration Release`: compiles the solution with `<TreatWarningsAsErrors>`, so it verifies that the code compiles without compiler or analyzer warnings.
- `dotnet test BrilliantMessaging.slnx --configuration Release`: runs all automated tests after a Release build. The RabbitMQ, NATS, and transport integration tests start containers via Testcontainers and need a running Docker daemon. Without Docker, run Docker-free test projects individually, e.g., `dotnet test --project tests/BrilliantMessaging.Core.Tests/BrilliantMessaging.Core.Tests.csproj --configuration Release`.
- Code coverage: follow the two-step `dotnet test ... --coverage` and `reportgenerator` commands in `tests/AGENTS.md` to verify the coverage threshold.
- `./scripts/release-version.test.sh`: verifies the release version validation script used by the NuGet release workflow.
- `dotnet run --configuration Release --project benchmarks/BrilliantMessaging.Benchmarks/BrilliantMessaging.Benchmarks.csproj -- --filter '*'`: runs the BenchmarkDotNet micro benchmarks to verify performance and allocation characteristics.
- After finishing a plan, run `./scripts/inspect-code.sh SUGGESTION`. It restores the local ReSharper command line tool, runs solution-wide analysis against the Release configuration, and writes an XML report to `artifacts/inspect-code/inspection-report.xml`. The severity argument is optional and defaults to `WARNING`. Address or report findings introduced by your changes. The report also contains pre-existing findings, so compare them against `main` before acting on them.

## Guided Coding

This repository uses Guided Coding. Read [ai-plans/AGENTS.md](./ai-plans/AGENTS.md) for the plan file naming conventions, the rules for working with Frozen Plans, and details on how to write plans.

## Test Rules

Read ./tests/AGENTS.md for details on how to write tests.

## Here is Your Space

If you encounter something worth noting while you are working on this code base, write it down here in this section. Once you are finished, I will discuss it with you and we can decide where to put your notes.
