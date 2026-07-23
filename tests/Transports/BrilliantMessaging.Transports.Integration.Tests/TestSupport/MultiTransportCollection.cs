using Xunit;

namespace BrilliantMessaging.Transports.Integration.Tests.TestSupport;

[CollectionDefinition]
public sealed class MultiTransportCollection : ICollectionFixture<MultiTransportFixture>;
