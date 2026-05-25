using Kuestenlogik.Surgewave.Client.Diagnostics;

namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// Exception thrown when a topic or partition does not exist.
/// </summary>
public class TopicPartitionException : SurgewaveClientException, IRecoverableException
{
    public string? Topic { get; }
    public int? Partition { get; }

    /// <summary>
    /// Gets a suggestion for how to recover from this error.
    /// </summary>
    public string? RecoverySuggestion => Diagnostics.RecoverySuggestion.ForTopicPartitionError(Topic, Partition);

    public TopicPartitionException() { }

    public TopicPartitionException(string message) : base(message) { }

    public TopicPartitionException(string message, Exception innerException) : base(message, innerException) { }

    public TopicPartitionException(string topic, int? partition = null)
        : base(partition.HasValue
            ? $"Topic '{topic}' partition {partition} does not exist"
            : $"Topic '{topic}' does not exist")
    {
        Topic = topic;
        Partition = partition;
    }
}
