using Xunit;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

[CollectionDefinition]
public sealed class RabbitMqCollection : ICollectionFixture<RabbitMqFixture>;
