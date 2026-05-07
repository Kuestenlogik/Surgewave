using Kuestenlogik.Surgewave.Protocol.Kafka;

namespace Kuestenlogik.Surgewave.Client.Diagnostics;

/// <summary>
/// Provides recovery suggestions for common errors to help users resolve issues.
/// </summary>
public static class RecoverySuggestion
{
    /// <summary>
    /// Gets a recovery suggestion for the given Kafka error code.
    /// </summary>
    public static string? ForErrorCode(ErrorCode errorCode) => errorCode switch
    {
        ErrorCode.UnknownTopicOrPartition => "Create the topic with: surgewave topic create <name> --partitions <n>",
        ErrorCode.LeaderNotAvailable => "Wait for leader election to complete, or check cluster health with: surgewave cluster status",
        ErrorCode.NotLeaderForPartition => "Refresh metadata and retry - leader election may be in progress",
        ErrorCode.RequestTimedOut => "Increase timeout or check network connectivity to broker",
        ErrorCode.BrokerNotAvailable => "Check broker is running: surgewave broker status",
        ErrorCode.ReplicaNotAvailable => "Wait for replica to become available, or check broker health",
        ErrorCode.MessageTooLarge => "Reduce message size or increase max.message.bytes on broker",
        ErrorCode.OffsetOutOfRange => "Reset offset: consumer.Seek(topic, partition, Offset.Beginning) or Offset.End",
        ErrorCode.CoordinatorLoadInProgress => "Wait for coordinator to initialize and retry",
        ErrorCode.CoordinatorNotAvailable => "Coordinator broker may be restarting - retry after a short delay",
        ErrorCode.NotCoordinator => "Refresh coordinator and retry: consumer.RefreshCoordinator()",
        ErrorCode.InvalidTopicException => "Topic name must be 1-249 characters, alphanumeric with '.', '_', '-' only",
        ErrorCode.NotEnoughReplicas => "Increase available replicas or reduce min.insync.replicas",
        ErrorCode.NotEnoughReplicasAfterAppend => "Check cluster health - some replicas may be unavailable",
        ErrorCode.IllegalGeneration => "Consumer generation is stale - rejoin the consumer group",
        ErrorCode.InconsistentGroupProtocol => "All consumers in group must use the same partition assignment protocol",
        ErrorCode.InvalidGroupId => "Group ID must be non-empty and valid",
        ErrorCode.UnknownMemberId => "Consumer was removed from group - rejoin required",
        ErrorCode.InvalidSessionTimeout => "Session timeout must be between group.min.session.timeout.ms and group.max.session.timeout.ms",
        ErrorCode.RebalanceInProgress => "Wait for rebalance to complete before committing offsets",
        ErrorCode.TopicAuthorizationFailed => "Check ACLs: surgewave acl list --topic <name>",
        ErrorCode.GroupAuthorizationFailed => "Check ACLs: surgewave acl list --group <name>",
        ErrorCode.ClusterAuthorizationFailed => "Check cluster-level ACLs or authentication credentials",
        ErrorCode.UnsupportedSaslMechanism => "Configure a supported SASL mechanism: PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512",
        ErrorCode.SaslAuthenticationFailed => "Check username/password or SASL credentials",
        ErrorCode.TopicAlreadyExists => "Topic already exists - use surgewave topic describe <name> to view",
        ErrorCode.InvalidPartitions => "Partition count must be >= 1",
        ErrorCode.InvalidReplicationFactor => "Replication factor must be >= 1 and <= number of brokers",
        ErrorCode.InvalidConfig => "Check configuration docs: docs/setup/configuration.md",
        ErrorCode.InvalidTxnState => "Transaction is in an invalid state - abort and start a new transaction",
        ErrorCode.ConcurrentTransactions => "Only one transaction per producer at a time - complete current transaction first",
        ErrorCode.TransactionalIdAuthorizationFailed => "Check transactional.id ACLs",
        ErrorCode.NonEmptyGroup => "Consumer group has active members - stop all consumers first",
        ErrorCode.UnsupportedCompressionType => "Use a supported compression: none, gzip, snappy, lz4, or zstd",
        _ => null
    };

    /// <summary>
    /// Gets a recovery suggestion for connection errors.
    /// </summary>
    public static string ForConnectionError(string? host, int? port)
    {
        var endpoint = (host, port) switch
        {
            (not null, not null) => $"{host}:{port}",
            (not null, _) => host,
            _ => "the broker"
        };
        return $"Verify {endpoint} is reachable. Check: 1) Broker is running (surgewave broker status), " +
               "2) Firewall allows connection, 3) Host/port are correct";
    }

    /// <summary>
    /// Gets a recovery suggestion for configuration errors.
    /// </summary>
    public static string ForConfigurationError(string propertyName) => propertyName switch
    {
        "BootstrapServers" => "Set BootstrapServers to broker address, e.g., 'localhost:9092'",
        "GroupId" => "Consumer group ID is required for group-based consumption",
        "TransactionalId" => "TransactionalId is required for exactly-once semantics",
        "ClientId" => "ClientId should be a descriptive identifier for your application",
        _ => $"Check configuration documentation for {propertyName}"
    };

    /// <summary>
    /// Gets a recovery suggestion for serialization errors.
    /// </summary>
    public static string ForSerializationError(Type? targetType, SerializationDirection direction)
    {
        var typeName = targetType?.Name ?? "the message";
        return direction == SerializationDirection.Serialize
            ? $"Ensure {typeName} is serializable. For custom types, configure an appropriate serializer."
            : $"Ensure the deserializer matches the producer's serializer for {typeName}.";
    }

    /// <summary>
    /// Gets a recovery suggestion for topic/partition errors.
    /// </summary>
    public static string ForTopicPartitionError(string? topic, int? partition)
    {
        if (partition.HasValue)
            return $"Verify partition {partition} exists: surgewave topic describe {topic}";
        return $"Create the topic: surgewave topic create {topic} --partitions <n>";
    }
}
