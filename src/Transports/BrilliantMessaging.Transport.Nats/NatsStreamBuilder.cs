using System;
using System.Collections.Generic;
using BrilliantMessaging.Core.Messaging;

namespace BrilliantMessaging.Transport.Nats;

/// <summary>
/// Fluent builder for a JetStream stream declaration.
/// </summary>
public sealed class NatsStreamBuilder : IBuildable<NatsStreamDefinition>
{
    private readonly string _name;
    private readonly List<string> _subjects = [];
    private TimeSpan? _duplicateWindow;
    private TimeSpan? _maxAge;
    private int? _maxMessageSize;
    private int _replicas = 1;
    private NatsStreamRetention _retention = NatsStreamRetention.Limits;
    private NatsStreamStorage _storage = NatsStreamStorage.File;

    /// <summary>
    /// Initializes a new instance of the <see cref="NatsStreamBuilder" /> class.
    /// </summary>
    public NatsStreamBuilder(string name)
    {
        _name = RequireText(name, nameof(name));
    }

    /// <inheritdoc />
    NatsStreamDefinition IBuildable<NatsStreamDefinition>.Build()
    {
        return new NatsStreamDefinition(
            _name,
            _subjects.AsReadOnly(),
            _duplicateWindow,
            _maxAge,
            _maxMessageSize,
            _storage,
            _retention,
            _replicas
        );
    }

    /// <summary>
    /// Adds a NATS subject pattern. Stream patterns may include <c>*</c> and <c>&gt;</c> wildcards.
    /// </summary>
    public NatsStreamBuilder Subject(string subjectPattern)
    {
        _subjects.Add(RequireText(subjectPattern, nameof(subjectPattern)));
        return this;
    }

    /// <summary>
    /// Configures the JetStream duplicate window used with NATS message-id deduplication.
    /// </summary>
    public NatsStreamBuilder DuplicateWindow(TimeSpan duplicateWindow)
    {
        if (duplicateWindow <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(duplicateWindow),
                duplicateWindow,
                "The value must be positive."
            );
        }

        _duplicateWindow = duplicateWindow;
        return this;
    }

    /// <summary>
    /// Configures the stream maximum message age.
    /// </summary>
    public NatsStreamBuilder MaxAge(TimeSpan maxAge)
    {
        if (maxAge <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(maxAge), maxAge, "The value must be positive.");
        }

        _maxAge = maxAge;
        return this;
    }

    /// <summary>
    /// Configures the stream maximum message size in bytes. NATS defaults to 1 MB when no server override exists.
    /// </summary>
    public NatsStreamBuilder MaxMessageSize(int bytes)
    {
        if (bytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bytes), bytes, "The value must be positive.");
        }

        _maxMessageSize = bytes;
        return this;
    }

    /// <summary>
    /// Configures the stream storage policy.
    /// </summary>
    public NatsStreamBuilder Storage(NatsStreamStorage storage)
    {
        if (!Enum.IsDefined(typeof(NatsStreamStorage), storage))
        {
            throw new ArgumentOutOfRangeException(nameof(storage), storage, "Unsupported NATS stream storage.");
        }

        _storage = storage;
        return this;
    }

    /// <summary>
    /// Configures the stream retention policy.
    /// </summary>
    public NatsStreamBuilder Retention(NatsStreamRetention retention)
    {
        if (!Enum.IsDefined(typeof(NatsStreamRetention), retention))
        {
            throw new ArgumentOutOfRangeException(nameof(retention), retention, "Unsupported NATS stream retention.");
        }

        _retention = retention;
        return this;
    }

    /// <summary>
    /// Configures the stream replica count.
    /// </summary>
    public NatsStreamBuilder Replicas(int replicas)
    {
        if (replicas <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(replicas), replicas, "The value must be positive.");
        }

        _replicas = replicas;
        return this;
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
