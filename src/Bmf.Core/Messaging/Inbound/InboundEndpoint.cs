using System;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Describes a registered inbound endpoint: the binding of a transport source and CloudEvents discriminator to
/// a message type, handler, deserializer, and acknowledgement mode, plus the composed pipeline that dispatches a
/// deserialized message to its handler.
/// </summary>
public abstract class InboundEndpoint
{
    private readonly MessageDelegate _handlerInvocation;

    /// <summary>
    /// Initializes a new instance of the <see cref="InboundEndpoint" /> class.
    /// </summary>
    /// <param name="name">The logical name of the endpoint.</param>
    /// <param name="transportName">The name of the transport that backs the endpoint.</param>
    /// <param name="topologyName">The name of the topology the endpoint belongs to.</param>
    /// <param name="messageType">The message type the endpoint handles.</param>
    /// <param name="handlerType">The handler type, which must implement <see cref="IMessageHandler{TMessage}" /> for the message type.</param>
    /// <param name="deserializerType">The deserializer type, which must implement <see cref="IMessageDeserializer" />.</param>
    /// <param name="discriminator">The CloudEvents discriminator the endpoint is bound to.</param>
    /// <param name="handlerInvocation">The composed pipeline delegate that dispatches the message to the handler.</param>
    /// <param name="ackMode">The acknowledgement mode for the endpoint.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="messageType" />, <paramref name="handlerType" />, <paramref name="deserializerType" />, or <paramref name="handlerInvocation" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when a string argument is null or whitespace, or when <paramref name="deserializerType" /> does not implement <see cref="IMessageDeserializer" />.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="ackMode" /> is not a defined value.</exception>
    protected InboundEndpoint(
        string name,
        string transportName,
        string topologyName,
        Type messageType,
        Type handlerType,
        Type deserializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
        MessageAckMode ackMode
    )
    {
        Name = RequireText(name, nameof(name));
        TransportName = RequireText(transportName, nameof(transportName));
        TopologyName = RequireText(topologyName, nameof(topologyName));
        MessageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
        HandlerType = handlerType ?? throw new ArgumentNullException(nameof(handlerType));
        DeserializerType = deserializerType ?? throw new ArgumentNullException(nameof(deserializerType));
        Discriminator = RequireText(discriminator, nameof(discriminator));
        _handlerInvocation = handlerInvocation ?? throw new ArgumentNullException(nameof(handlerInvocation));

        if (!typeof(IMessageDeserializer).IsAssignableFrom(DeserializerType))
        {
            throw new ArgumentException(
                $"Deserializer type '{DeserializerType}' must implement '{typeof(IMessageDeserializer)}'.",
                nameof(deserializerType)
            );
        }

        if (!Enum.IsDefined(typeof(MessageAckMode), ackMode))
        {
            throw new ArgumentOutOfRangeException(nameof(ackMode), ackMode, "Unsupported acknowledgement mode.");
        }

        AckMode = ackMode;
    }

    /// <summary>
    /// Gets the logical name of the endpoint.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the name of the transport that backs the endpoint.
    /// </summary>
    public string TransportName { get; }

    /// <summary>
    /// Gets the name of the topology the endpoint belongs to.
    /// </summary>
    public string TopologyName { get; }

    /// <summary>
    /// Gets the message type the endpoint handles.
    /// </summary>
    public Type MessageType { get; }

    /// <summary>
    /// Gets the handler type for the endpoint.
    /// </summary>
    public Type HandlerType { get; }

    /// <summary>
    /// Gets the deserializer type for the endpoint.
    /// </summary>
    public Type DeserializerType { get; }

    /// <summary>
    /// Gets the CloudEvents discriminator the endpoint is bound to.
    /// </summary>
    public string Discriminator { get; }

    /// <summary>
    /// Gets the acknowledgement mode for the endpoint.
    /// </summary>
    public MessageAckMode AckMode { get; }

    /// <summary>
    /// Invokes the endpoint's pipeline to dispatch an already-deserialized message to its handler.
    /// </summary>
    /// <param name="context">The context for the message; its <see cref="IncomingMessageContext.Message" /> must already be set.</param>
    /// <returns>A task that completes when the handler (and surrounding pipeline) has finished.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="context" /> is <see langword="null" />.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the message has not been deserialized.</exception>
    public Task InvokeHandlerAsync(IncomingMessageContext context)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (context.Message is null)
        {
            throw new InvalidOperationException("The inbound message has not been deserialized.");
        }

        return _handlerInvocation(context);
    }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}

/// <summary>
/// A strongly typed <see cref="InboundEndpoint" /> for messages of type <typeparamref name="TMessage" /> that
/// validates the handler type implements <see cref="IMessageHandler{TMessage}" />.
/// </summary>
/// <typeparam name="TMessage">The message type the endpoint handles.</typeparam>
public class InboundEndpoint<TMessage> : InboundEndpoint
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InboundEndpoint{TMessage}" /> class.
    /// </summary>
    /// <param name="name">The logical name of the endpoint.</param>
    /// <param name="transportName">The name of the transport that backs the endpoint.</param>
    /// <param name="topologyName">The name of the topology the endpoint belongs to.</param>
    /// <param name="handlerType">The handler type, which must implement <see cref="IMessageHandler{TMessage}" />.</param>
    /// <param name="deserializerType">The deserializer type, which must implement <see cref="IMessageDeserializer" />.</param>
    /// <param name="discriminator">The CloudEvents discriminator the endpoint is bound to.</param>
    /// <param name="handlerInvocation">The composed pipeline delegate that dispatches the message to the handler.</param>
    /// <param name="ackMode">The acknowledgement mode for the endpoint; defaults to <see cref="MessageAckMode.Auto" />.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="handlerType" /> does not implement <see cref="IMessageHandler{TMessage}" />.</exception>
    public InboundEndpoint(
        string name,
        string transportName,
        string topologyName,
        Type handlerType,
        Type deserializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
        MessageAckMode ackMode = MessageAckMode.Auto
    )
        : base(
            name,
            transportName,
            topologyName,
            typeof(TMessage),
            handlerType,
            deserializerType,
            discriminator,
            handlerInvocation,
            ackMode
        )
    {
        if (!typeof(IMessageHandler<TMessage>).IsAssignableFrom(handlerType))
        {
            throw new ArgumentException(
                $"Handler type '{handlerType}' must implement '{typeof(IMessageHandler<TMessage>)}'.",
                nameof(handlerType)
            );
        }
    }
}
