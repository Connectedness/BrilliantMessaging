namespace BrilliantMessaging.Core.Messaging;

/// <summary>
/// Represents the abstraction of the Build method on a fluent builder.
/// </summary>
/// <remarks>
/// Builders implement this member explicitly so <see cref="Build" /> stays out of the way (and out of IntelliSense)
/// while the builder is configured through a callback, yet remains available to callers that own the builder and
/// need its compiled output by accessing it through this interface.
/// </remarks>
/// <typeparam name="TResult">The type produced when the builder is compiled.</typeparam>
public interface IBuildable<out TResult>
{
    /// <summary>
    /// Creates the builder's accumulated configuration into its result.
    /// </summary>
    /// <returns>The compiled result.</returns>
    TResult Build();
}
