namespace Kuestenlogik.Surgewave.Schema.Registry.Client;

/// <summary>
/// Strategy for determining the schema subject name.
/// Subject names are used by the Schema Registry to identify schemas.
/// </summary>
public interface ISubjectNameStrategy
{
    /// <summary>
    /// Get the subject name for a schema.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="isKey">True if this is for the key schema, false for value schema.</param>
    /// <param name="recordName">Optional fully-qualified record name (namespace.type) for record-based strategies.</param>
    /// <returns>The subject name to use with the Schema Registry.</returns>
    string GetSubjectName(string topic, bool isKey, string? recordName = null);
}

/// <summary>
/// Standard subject name strategy types matching Confluent Schema Registry.
/// </summary>
public enum SubjectNameStrategyType
{
    /// <summary>
    /// Subject name is based on topic name: {topic}-key or {topic}-value.
    /// This is the default strategy and most commonly used.
    /// </summary>
    TopicName,

    /// <summary>
    /// Subject name is the fully-qualified record name.
    /// Useful when the same schema is used across multiple topics.
    /// </summary>
    RecordName,

    /// <summary>
    /// Subject name combines topic and record name: {topic}-{recordName}.
    /// Useful for topic-specific schema evolution of the same record type.
    /// </summary>
    TopicRecordName
}

/// <summary>
/// Topic name strategy: {topic}-key or {topic}-value.
/// This is the default and most common strategy.
/// </summary>
public sealed class TopicNameStrategy : ISubjectNameStrategy
{
    /// <summary>
    /// Singleton instance.
    /// </summary>
    public static readonly TopicNameStrategy Instance = new();

    private TopicNameStrategy() { }

    public string GetSubjectName(string topic, bool isKey, string? recordName = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        return isKey ? $"{topic}-key" : $"{topic}-value";
    }
}

/// <summary>
/// Record name strategy: uses the fully-qualified record name as subject.
/// Requires recordName to be provided.
/// </summary>
public sealed class RecordNameStrategy : ISubjectNameStrategy
{
    /// <summary>
    /// Singleton instance.
    /// </summary>
    public static readonly RecordNameStrategy Instance = new();

    private RecordNameStrategy() { }

    public string GetSubjectName(string topic, bool isKey, string? recordName = null)
    {
        if (string.IsNullOrEmpty(recordName))
            throw new ArgumentException("Record name is required for RecordNameStrategy", nameof(recordName));

        return recordName;
    }
}

/// <summary>
/// Topic record name strategy: {topic}-{recordName}.
/// Combines topic and record name for topic-specific schemas.
/// </summary>
public sealed class TopicRecordNameStrategy : ISubjectNameStrategy
{
    /// <summary>
    /// Singleton instance.
    /// </summary>
    public static readonly TopicRecordNameStrategy Instance = new();

    private TopicRecordNameStrategy() { }

    public string GetSubjectName(string topic, bool isKey, string? recordName = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        if (string.IsNullOrEmpty(recordName))
            throw new ArgumentException("Record name is required for TopicRecordNameStrategy", nameof(recordName));

        return $"{topic}-{recordName}";
    }
}

/// <summary>
/// Factory for creating subject name strategies.
/// </summary>
public static class SubjectNameStrategies
{
    /// <summary>
    /// Get a subject name strategy by type.
    /// </summary>
    public static ISubjectNameStrategy Get(SubjectNameStrategyType type) => type switch
    {
        SubjectNameStrategyType.TopicName => TopicNameStrategy.Instance,
        SubjectNameStrategyType.RecordName => RecordNameStrategy.Instance,
        SubjectNameStrategyType.TopicRecordName => TopicRecordNameStrategy.Instance,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown subject name strategy type")
    };

    /// <summary>
    /// The default subject name strategy (TopicName).
    /// </summary>
    public static ISubjectNameStrategy Default => TopicNameStrategy.Instance;
}
