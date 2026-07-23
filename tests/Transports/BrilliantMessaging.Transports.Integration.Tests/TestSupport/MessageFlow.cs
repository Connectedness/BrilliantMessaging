using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Abstractions;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;

namespace BrilliantMessaging.Transports.Integration.Tests.TestSupport;

public sealed record IncomingOrder : BaseCloudEvent
{
    public string OrderId { get; init; } = string.Empty;
}

public sealed record TransformedOrder : BaseCloudEvent
{
    public string OrderId { get; init; } = string.Empty;

    public string Description { get; init; } = string.Empty;
}

public sealed record RejectedOrderReport : BaseCloudEvent
{
    public string OrderId { get; init; } = string.Empty;

    public string RejectedDescription { get; init; } = string.Empty;

    public string Outcome { get; init; } = string.Empty;
}

public sealed record MessageFlowTopologies(string RabbitMq, string Nats);

public sealed class MessageFlowProbe
{
    private readonly TaskCompletionSource<RejectedOrderReport> _report =
        new (TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly TaskCompletionSource<TransformedOrder> _transformed =
        new (TaskCreationOptions.RunContinuationsAsynchronously);

    public void RecordTransformed(TransformedOrder message)
    {
        _transformed.TrySetResult(message);
    }

    public void RecordReport(RejectedOrderReport message)
    {
        _report.TrySetResult(message);
    }

    public Task<TransformedOrder> WaitForTransformedAsync(CancellationToken cancellationToken)
    {
        return _transformed.Task.WaitAsync(cancellationToken);
    }

    public Task<RejectedOrderReport> WaitForReportAsync(CancellationToken cancellationToken)
    {
        return _report.Task.WaitAsync(cancellationToken);
    }
}

public sealed class TransformingOrderHandler : IMessageHandler<IncomingOrder>
{
    private readonly MessageFlowProbe _probe;
    private readonly IMessagePublisher _publisher;
    private readonly MessageFlowTopologies _topologies;

    public TransformingOrderHandler(
        IMessagePublisher publisher,
        MessageFlowProbe probe,
        MessageFlowTopologies topologies
    )
    {
        _publisher = publisher;
        _probe = probe;
        _topologies = topologies;
    }

    public async Task HandleAsync(
        IncomingOrder message,
        IncomingMessageContext context,
        CancellationToken cancellationToken
    )
    {
        TransformedOrder transformed = new ()
        {
            OrderId = message.OrderId,
            Description = $"transformed:{message.OrderId}"
        };
        _probe.RecordTransformed(transformed);
        await _publisher
           .ForTopology(_topologies.RabbitMq)
           .PublishMessageAsync(transformed, cancellationToken: cancellationToken)
           .ConfigureAwait(false);
    }
}

public sealed class RejectingOrderHandler : IMessageHandler<TransformedOrder>
{
    public Task HandleAsync(
        TransformedOrder message,
        IncomingMessageContext context,
        CancellationToken cancellationToken
    )
    {
        throw new RejectMessageException("The test rejects the transformed order.");
    }
}

public sealed class DeadLetterReportingHandler : IMessageHandler<TransformedOrder>
{
    private readonly IMessagePublisher _publisher;
    private readonly MessageFlowTopologies _topologies;

    public DeadLetterReportingHandler(IMessagePublisher publisher, MessageFlowTopologies topologies)
    {
        _publisher = publisher;
        _topologies = topologies;
    }

    public async Task HandleAsync(
        TransformedOrder message,
        IncomingMessageContext context,
        CancellationToken cancellationToken
    )
    {
        RejectedOrderReport report = new ()
        {
            OrderId = message.OrderId,
            RejectedDescription = message.Description,
            Outcome = "dead-lettered"
        };
        await _publisher
           .ForTopology(_topologies.Nats)
           .PublishMessageAsync(report, cancellationToken: cancellationToken)
           .ConfigureAwait(false);
    }
}

public sealed class ReportRecordingHandler : IMessageHandler<RejectedOrderReport>
{
    private readonly MessageFlowProbe _probe;

    public ReportRecordingHandler(MessageFlowProbe probe)
    {
        _probe = probe;
    }

    public Task HandleAsync(
        RejectedOrderReport message,
        IncomingMessageContext context,
        CancellationToken cancellationToken
    )
    {
        _probe.RecordReport(message);
        return Task.CompletedTask;
    }
}
