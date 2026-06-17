namespace Bmf.Abstractions;

/// <summary>
/// Defines the transport-neutral CloudEvents v1.0 attribute names.
/// </summary>
public static class CloudEventAttributeNames
{
    /// <summary>
    /// The name of the <c>datacontenttype</c> attribute, describing the content type of the event data.
    /// </summary>
    public const string DataContentType = "datacontenttype";

    /// <summary>
    /// The name of the <c>dataschema</c> attribute, identifying the schema the event data adheres to.
    /// </summary>
    public const string DataSchema = "dataschema";

    /// <summary>
    /// The name of the <c>id</c> attribute, which together with <see cref="Source" /> uniquely identifies the event.
    /// </summary>
    public const string Id = "id";

    /// <summary>
    /// The name of the <c>source</c> attribute, identifying the context in which the event happened.
    /// </summary>
    public const string Source = "source";

    /// <summary>
    /// The name of the <c>specversion</c> attribute, identifying the CloudEvents specification version.
    /// </summary>
    public const string SpecVersion = "specversion";

    /// <summary>
    /// The name of the <c>subject</c> attribute, describing the subject of the event in the context of its source.
    /// </summary>
    public const string Subject = "subject";

    /// <summary>
    /// The name of the <c>time</c> attribute, carrying the timestamp at which the event occurred.
    /// </summary>
    public const string Time = "time";

    /// <summary>
    /// The name of the <c>type</c> attribute, describing the type of event related to the originating occurrence.
    /// </summary>
    public const string Type = "type";
}
