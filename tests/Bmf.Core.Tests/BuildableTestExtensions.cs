using Bmf.Core.Messaging;

namespace Bmf.Core.Tests;

/// <summary>
/// Test-only convenience for compiling a builder through <see cref="IBuildable{TResult}" />.
/// </summary>
/// <remarks>
/// Production code reaches the explicitly implemented <see cref="IBuildable{TResult}.Build" /> through an interface
/// cast (the framework's composition step). Tests that own a builder directly use this extension to obtain its
/// compiled result without repeating that cast at every call site.
/// </remarks>
public static class BuildableTestExtensions
{
    public static TResult Build<TResult>(this IBuildable<TResult> buildable)
    {
        return buildable.Build();
    }
}
