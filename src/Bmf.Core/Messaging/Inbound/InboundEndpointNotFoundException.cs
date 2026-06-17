using System;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Thrown when no inbound endpoint is registered under a requested name.
/// </summary>
public sealed class InboundEndpointNotFoundException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InboundEndpointNotFoundException" /> class.
    /// </summary>
    /// <param name="endpointName">The endpoint name that was not found.</param>
    public InboundEndpointNotFoundException(string endpointName)
        : base($"Inbound endpoint '{endpointName}' is not registered.")
    {
        EndpointName = endpointName;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="InboundEndpointNotFoundException" /> class with an underlying
    /// cause.
    /// </summary>
    /// <param name="endpointName">The endpoint name that was not found.</param>
    /// <param name="innerException">The underlying exception.</param>
    public InboundEndpointNotFoundException(string endpointName, Exception innerException)
        : base($"Inbound endpoint '{endpointName}' is not registered.", innerException)
    {
        EndpointName = endpointName;
    }

    /// <summary>
    /// Gets the endpoint name that was not found.
    /// </summary>
    public string EndpointName { get; }
}
