namespace BrilliantMessaging.Core.Tests.Messaging.TestSupport;

public sealed record DerivedSampleMessage(string Value, string Detail) : BaseSampleMessage(Value);
