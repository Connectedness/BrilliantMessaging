using Xunit;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;

[CollectionDefinition]
public sealed class RabbitMqCollection : ICollectionFixture<RabbitMqFixture>;
