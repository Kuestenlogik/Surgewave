namespace Kuestenlogik.Surgewave.Schema.Registry.Inference;

/// <summary>
/// Abstraction for sampling raw message values from topics.
/// Implemented by the broker to bridge between the Schema Registry and topic storage.
/// </summary>
public interface ITopicMessageSampler
{
    /// <summary>
    /// Get the list of available topic names.
    /// </summary>
    IReadOnlyList<string> GetTopics();

    /// <summary>
    /// Sample up to <paramref name="maxMessages"/> raw message values from a topic.
    /// Returns the raw value bytes of each message.
    /// </summary>
    /// <param name="topicName">Name of the topic to sample from.</param>
    /// <param name="maxMessages">Maximum number of messages to sample.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of raw message value bytes.</returns>
    Task<IReadOnlyList<ReadOnlyMemory<byte>>> SampleMessagesAsync(
        string topicName,
        int maxMessages,
        CancellationToken cancellationToken = default);
}
