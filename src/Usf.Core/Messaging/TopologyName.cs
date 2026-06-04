using System;

namespace Usf.Core.Messaging;

public readonly record struct TopologyName
{
    public TopologyName(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(value));
        }

        Value = value;
    }

    public static TopologyName Default { get; } = new ("default");

    public string Value =>
        field ?? throw new InvalidOperationException("Topology name must not be the default instance");

    public static implicit operator TopologyName(string value)
    {
        return new TopologyName(value);
    }

    public override string ToString()
    {
        return Value;
    }
}
