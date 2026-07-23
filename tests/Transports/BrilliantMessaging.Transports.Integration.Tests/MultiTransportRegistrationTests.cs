using System;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Transport.Nats;
using BrilliantMessaging.Transport.RabbitMq;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BrilliantMessaging.Transports.Integration.Tests;

public sealed class MultiTransportRegistrationTests
{
    [Fact]
    public void ParameterlessTransportRegistrations_RejectDuplicateDefaultTopologyName()
    {
        ServiceCollection services = [];

        Action act = () => services
           .AddBrilliantMessaging()
           .AddRabbitMqTopology(static _ => { })
           .AddNatsTopology(static _ => { });

        act.Should()
           .Throw<InvalidOperationException>()
           .WithMessage("Topology 'default' is already registered. Registered topologies: default.");
    }
}
