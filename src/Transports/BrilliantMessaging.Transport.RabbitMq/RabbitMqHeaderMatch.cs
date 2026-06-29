namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Controls how a headers exchange matches the headers on a message against the headers configured on a binding.
/// </summary>
/// <remarks>
/// <para>
/// The four values map to the broker's <c>x-match</c> binding argument. <see cref="All" /> and <see cref="Any" />
/// are the classic AMQP 0-9-1 modes: <see cref="All" /> requires every non-<c>x-</c>-prefixed configured header
/// to match, while <see cref="Any" /> requires at least one. Both <see cref="All" /> and <see cref="Any" />
/// exclude <c>x-</c>-prefixed headers from matching. If every configured predicate is <c>x-</c>-prefixed,
/// <see cref="All" /> can therefore match every message (zero effective predicates), while <see cref="Any" />
/// matches none. Active topology bindings that combine <see cref="All" /> or <see cref="Any" /> (or omit
/// <c>x-match</c>) with <c>x-</c>-prefixed predicates are rejected by the topology compiler.
/// </para>
/// <para>
/// <see cref="AllWithX" /> and <see cref="AnyWithX" /> are the RabbitMQ extensions that include
/// <c>x-</c>-prefixed headers in matching. Use them when the framework's own header conventions (which use
/// <c>x-</c> names, for example <c>x-tenant</c>) must participate in routing. The <c>x-match</c> argument itself
/// is never treated as a match predicate, regardless of the selected mode.
/// </para>
/// </remarks>
public enum RabbitMqHeaderMatch
{
    /// <summary>
    /// Match all configured headers (logical AND). <c>x-</c>-prefixed headers are excluded from matching.
    /// </summary>
    All = 0,

    /// <summary>
    /// Match any configured header (logical OR). <c>x-</c>-prefixed headers are excluded from matching.
    /// </summary>
    Any,

    /// <summary>
    /// Match all configured headers (logical AND), including <c>x-</c>-prefixed headers.
    /// </summary>
    AllWithX,

    /// <summary>
    /// Match any configured header (logical OR), including <c>x-</c>-prefixed headers.
    /// </summary>
    AnyWithX
}
