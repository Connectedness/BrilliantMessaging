# AGENTS.md for Tests

## General Rules

- Please do not use mocking frameworks like Moq or NSubstitute for test doubles, use hand-crafted test doubles instead.
- Do not write nested test classes. All tests should reside in a class which is directly placed in a namespace.
- Use FluentAssertions instead of xunit's `Assert` class.
- Prefer Sociable Tests instead of Solitary Tests. Create as much test coverage as possible by calling higher level production APIs. Only write solitary tests to cover lower level APIs that are hard to test with Sociable Tests.
- Keep test coverage at least above 90%.
- `dotnet test --solution USF.slnx --no-build` currently forwards `--report-trx`, which the xUnit v3 Microsoft Testing Platform runner rejects. Running test projects directly with `dotnet run --project <test-project> --no-build` works.
