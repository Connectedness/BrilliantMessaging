using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.TestSupport;

[CollectionDefinition]
public sealed class NatsCollection : ICollectionFixture<NatsFixture>;
