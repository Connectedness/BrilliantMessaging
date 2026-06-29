namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// A single recorded handler invocation.
/// </summary>
/// <param name="Route">The topic the delivery arrived on.</param>
/// <param name="EndpointName">The endpoint name that handled the delivery.</param>
/// <param name="Message">The deserialized message.</param>
/// <param name="DeliveryAttempt">The one-based delivery attempt.</param>
public sealed record HandlerInvocation(string Route, string EndpointName, object Message, int DeliveryAttempt);
