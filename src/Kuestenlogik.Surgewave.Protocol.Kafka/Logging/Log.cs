using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Source-generated high-performance logging for the relocated Kafka wire loop
/// (<see cref="KafkaConnectionHandler"/>) and control-plane handlers (Metadata / TopicAdmin).
/// Moved into the Kafka plugin alongside the code that emits it (#59 b5); message templates
/// are byte-identical to the broker's former <c>Log</c> entries so operator-facing log output
/// is unchanged.
/// </summary>
internal static partial class Log
{
    // ── Kafka connection loop ────────────────────────────────────────────────
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] Waiting for request")]
    public static partial void WaitingForRequest(ILogger logger, object? endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] Received {ApiKey} request (size: {Size}, correlationId: {CorrelationId})")]
    public static partial void RequestReceived(ILogger logger, object? endpoint, object apiKey, int size, int correlationId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] Sending response for correlationId: {CorrelationId}")]
    public static partial void SendingResponse(ILogger logger, object? endpoint, int correlationId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] Response sent successfully")]
    public static partial void ResponseSent(ILogger logger, object? endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] End of stream")]
    public static partial void EndOfStream(ILogger logger, object? endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}] IO error")]
    public static partial void IoError(ILogger logger, Exception ex, object? endpoint);

    // ── Metadata handling ────────────────────────────────────────────────────
    [LoggerMessage(Level = LogLevel.Debug, Message = "Metadata request for ALL topics, found {TopicCount} topics")]
    public static partial void MetadataAllTopics(ILogger logger, int topicCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Metadata request for empty topic list (treating as ALL), found {TopicCount} topics")]
    public static partial void MetadataEmptyList(ILogger logger, int topicCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Metadata request for specific topics: {Topics}")]
    public static partial void MetadataSpecificTopics(ILogger logger, string topics);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Processing metadata for topic: {Topic}")]
    public static partial void ProcessingMetadata(ILogger logger, string topic);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Auto-creating topic: {Topic}")]
    public static partial void AutoCreatingTopic(ILogger logger, string topic);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Topic created: {Topic}, partitions: {PartitionCount}")]
    public static partial void TopicCreated(ILogger logger, string topic, int partitionCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Metadata response: Broker[0] NodeId={NodeId}, Host={Host}, Port={Port}")]
    public static partial void MetadataResponse(ILogger logger, int nodeId, string host, int port);

    [LoggerMessage(Level = LogLevel.Debug, Message = "  Topic: {TopicName}, ErrorCode={ErrorCode}, Partitions={PartitionCount}")]
    public static partial void MetadataTopicInfo(ILogger logger, string topicName, object errorCode, int partitionCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "    Partition {PartitionId}: Leader={Leader}, Replicas=[{Replicas}], ISR=[{Isr}], ErrorCode={ErrorCode}")]
    public static partial void MetadataPartitionInfo(ILogger logger, int partitionId, int leader, string replicas, string isr, object errorCode);

    // ── Topic administration ─────────────────────────────────────────────────
    [LoggerMessage(Level = LogLevel.Information, Message = "Topic deleted: {Topic}")]
    public static partial void TopicDeleted(ILogger logger, string topic);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error creating topic: {Topic}")]
    public static partial void CreateTopicError(ILogger logger, Exception ex, string topic);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error deleting topic: {Topic}")]
    public static partial void DeleteTopicError(ILogger logger, Exception ex, string topic);
}
