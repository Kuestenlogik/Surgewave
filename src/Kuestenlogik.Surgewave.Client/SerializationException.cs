using Kuestenlogik.Surgewave.Client.Diagnostics;

namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// Direction of serialization operation.
/// </summary>
public enum SerializationDirection
{
    Serialize,
    Deserialize
}

/// <summary>
/// Exception thrown when serialization or deserialization fails.
/// </summary>
public class SerializationException : SurgewaveClientException, IRecoverableException
{
    /// <summary>
    /// The type being serialized/deserialized.
    /// </summary>
    public Type? TargetType { get; }

    /// <summary>
    /// The topic context of the serialization.
    /// </summary>
    public string? Topic { get; }

    /// <summary>
    /// Whether this was a serialize or deserialize operation.
    /// </summary>
    public SerializationDirection Direction { get; }

    /// <summary>
    /// Gets a suggestion for how to recover from this error.
    /// </summary>
    public string? RecoverySuggestion => Diagnostics.RecoverySuggestion.ForSerializationError(TargetType, Direction);

    public SerializationException() { }

    public SerializationException(string message) : base(message) { }

    public SerializationException(string message, Exception innerException) : base(message, innerException) { }

    public SerializationException(SerializationDirection direction, Type? targetType, string? topic, Exception? innerException = null)
        : base(FormatMessage(direction, targetType, topic), innerException!)
    {
        Direction = direction;
        TargetType = targetType;
        Topic = topic;
    }

    public SerializationException(string message, SerializationDirection direction, Type? targetType, string? topic)
        : base(message)
    {
        Direction = direction;
        TargetType = targetType;
        Topic = topic;
    }

    private static string FormatMessage(SerializationDirection direction, Type? targetType, string? topic)
    {
        var typeName = targetType?.Name ?? "unknown type";
        var topicInfo = topic != null ? $" for topic '{topic}'" : "";
        return direction == SerializationDirection.Serialize
            ? $"Failed to serialize {typeName}{topicInfo}"
            : $"Failed to deserialize to {typeName}{topicInfo}";
    }
}
