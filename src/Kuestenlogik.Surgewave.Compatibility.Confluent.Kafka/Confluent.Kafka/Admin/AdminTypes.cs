namespace Confluent.Kafka.Admin;

/// <summary>
/// Specification for creating a topic.
/// </summary>
public class TopicSpecification
{
    /// <summary>Topic name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Number of partitions.</summary>
    public int NumPartitions { get; set; } = 1;

    /// <summary>Replication factor.</summary>
    public short ReplicationFactor { get; set; } = 1;

    /// <summary>Replica assignments (alternative to NumPartitions/ReplicationFactor).</summary>
    public Dictionary<int, List<int>>? ReplicasAssignments { get; set; }

    /// <summary>Topic configuration.</summary>
    public Dictionary<string, string>? Configs { get; set; }
}

/// <summary>
/// Specification for creating partitions.
/// </summary>
public class PartitionsSpecification
{
    /// <summary>Topic name.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>Target total partition count.</summary>
    public int IncreaseTo { get; set; }

    /// <summary>Replica assignments for new partitions.</summary>
    public List<List<int>>? ReplicaAssignments { get; set; }
}

/// <summary>
/// Options for creating topics.
/// </summary>
public class CreateTopicsOptions
{
    /// <summary>Operation timeout.</summary>
    public TimeSpan? RequestTimeout { get; set; }

    /// <summary>Validate only without actually creating.</summary>
    public bool ValidateOnly { get; set; }
}

/// <summary>
/// Options for deleting topics.
/// </summary>
public class DeleteTopicsOptions
{
    /// <summary>Operation timeout.</summary>
    public TimeSpan? RequestTimeout { get; set; }
}

/// <summary>
/// Options for creating partitions.
/// </summary>
public class CreatePartitionsOptions
{
    /// <summary>Operation timeout.</summary>
    public TimeSpan? RequestTimeout { get; set; }

    /// <summary>Validate only without actually creating.</summary>
    public bool ValidateOnly { get; set; }
}

/// <summary>
/// Options for describing cluster.
/// </summary>
public class DescribeClusterOptions
{
    /// <summary>Operation timeout.</summary>
    public TimeSpan? RequestTimeout { get; set; }

    /// <summary>Include authorized operations.</summary>
    public bool IncludeAuthorizedOperations { get; set; }
}

/// <summary>
/// Collection of topics for describe operations.
/// </summary>
public class TopicCollection
{
    private TopicCollection(IEnumerable<string> names) => TopicNames = names.ToList();

    /// <summary>Topic names.</summary>
    public List<string> TopicNames { get; }

    /// <summary>Create from topic names.</summary>
    public static TopicCollection OfTopicNames(IEnumerable<string> names) => new(names);
}

/// <summary>
/// Options for describing topics.
/// </summary>
public class DescribeTopicsOptions
{
    /// <summary>Operation timeout.</summary>
    public TimeSpan? RequestTimeout { get; set; }

    /// <summary>Include authorized operations.</summary>
    public bool IncludeAuthorizedOperations { get; set; }
}

/// <summary>
/// Options for listing consumer groups.
/// </summary>
public class ListConsumerGroupsOptions
{
    /// <summary>Operation timeout.</summary>
    public TimeSpan? RequestTimeout { get; set; }

    /// <summary>Filter by states.</summary>
    public IEnumerable<ConsumerGroupState>? MatchStates { get; set; }
}

/// <summary>
/// Options for describing consumer groups.
/// </summary>
public class DescribeConsumerGroupsOptions
{
    /// <summary>Operation timeout.</summary>
    public TimeSpan? RequestTimeout { get; set; }

    /// <summary>Include authorized operations.</summary>
    public bool IncludeAuthorizedOperations { get; set; }
}

/// <summary>
/// Options for deleting groups.
/// </summary>
public class DeleteGroupsOptions
{
    /// <summary>Operation timeout.</summary>
    public TimeSpan? RequestTimeout { get; set; }
}

/// <summary>
/// Options for listing consumer group offsets.
/// </summary>
public class ListConsumerGroupOffsetsOptions
{
    /// <summary>Operation timeout.</summary>
    public TimeSpan? RequestTimeout { get; set; }

    /// <summary>Require stable offsets.</summary>
    public bool RequireStableOffsets { get; set; }
}

/// <summary>
/// Options for altering consumer group offsets.
/// </summary>
public class AlterConsumerGroupOffsetsOptions
{
    /// <summary>Operation timeout.</summary>
    public TimeSpan? RequestTimeout { get; set; }
}

/// <summary>
/// Options for deleting records.
/// </summary>
public class DeleteRecordsOptions
{
    /// <summary>Operation timeout.</summary>
    public TimeSpan? RequestTimeout { get; set; }
}

/// <summary>
/// Consumer group state.
/// </summary>
public enum ConsumerGroupState
{
    /// <summary>Unknown.</summary>
    Unknown,
    /// <summary>Preparing rebalance.</summary>
    PreparingRebalance,
    /// <summary>Completing rebalance.</summary>
    CompletingRebalance,
    /// <summary>Stable.</summary>
    Stable,
    /// <summary>Dead.</summary>
    Dead,
    /// <summary>Empty.</summary>
    Empty
}

/// <summary>
/// Consumer group and partitions.
/// </summary>
public class ConsumerGroupTopicPartitions
{
    /// <summary>Group ID.</summary>
    public string Group { get; set; } = string.Empty;

    /// <summary>Partitions (null for all).</summary>
    public IEnumerable<TopicPartition>? Partitions { get; set; }
}

/// <summary>
/// Consumer group with topic partition offsets.
/// </summary>
public class ConsumerGroupTopicPartitionOffsets
{
    /// <summary>Group ID.</summary>
    public string Group { get; set; } = string.Empty;

    /// <summary>Partition offsets.</summary>
    public IEnumerable<TopicPartitionOffset> Partitions { get; set; } = [];
}

/// <summary>
/// Result of cluster describe.
/// </summary>
public class DescribeClusterResult
{
    /// <summary>Cluster ID.</summary>
    public string ClusterId { get; set; } = string.Empty;

    /// <summary>Controller broker.</summary>
    public Node? Controller { get; set; }

    /// <summary>All brokers.</summary>
    public List<Node> Nodes { get; set; } = [];

    /// <summary>Authorized operations.</summary>
    public List<AclOperation>? AuthorizedOperations { get; set; }
}

/// <summary>
/// Result of topic describe.
/// </summary>
public class DescribeTopicsResult
{
    /// <summary>Topic descriptions.</summary>
    public List<TopicDescription> TopicDescriptions { get; set; } = [];
}

/// <summary>
/// Topic description.
/// </summary>
public class TopicDescription
{
    /// <summary>Topic name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Whether topic is internal.</summary>
    public bool IsInternal { get; set; }

    /// <summary>Partition information.</summary>
    public List<TopicPartitionInfo> Partitions { get; set; } = [];

    /// <summary>Authorized operations.</summary>
    public List<AclOperation>? AuthorizedOperations { get; set; }

    /// <summary>Any error.</summary>
    public Error? Error { get; set; }
}

/// <summary>
/// Topic partition information.
/// </summary>
public class TopicPartitionInfo
{
    /// <summary>Partition ID.</summary>
    public int Partition { get; set; }

    /// <summary>Leader node.</summary>
    public Node? Leader { get; set; }

    /// <summary>Replica nodes.</summary>
    public List<Node> Replicas { get; set; } = [];

    /// <summary>In-sync replica nodes.</summary>
    public List<Node> Isr { get; set; } = [];
}

/// <summary>
/// Broker node.
/// </summary>
public class Node
{
    /// <summary>Node ID.</summary>
    public int Id { get; set; }

    /// <summary>Host name.</summary>
    public string Host { get; set; } = string.Empty;

    /// <summary>Port number.</summary>
    public int Port { get; set; }

    /// <summary>Rack.</summary>
    public string? Rack { get; set; }
}

/// <summary>
/// ACL operation type.
/// </summary>
public enum AclOperation
{
    /// <summary>Unknown.</summary>
    Unknown,
    /// <summary>Any.</summary>
    Any,
    /// <summary>All.</summary>
    All,
    /// <summary>Read.</summary>
    Read,
    /// <summary>Write.</summary>
    Write,
    /// <summary>Create.</summary>
    Create,
    /// <summary>Delete.</summary>
    Delete,
    /// <summary>Alter.</summary>
    Alter,
    /// <summary>Describe.</summary>
    Describe,
    /// <summary>ClusterAction.</summary>
    ClusterAction,
    /// <summary>DescribeConfigs.</summary>
    DescribeConfigs,
    /// <summary>AlterConfigs.</summary>
    AlterConfigs,
    /// <summary>IdempotentWrite.</summary>
    IdempotentWrite
}

/// <summary>
/// Result of list consumer groups.
/// </summary>
public class ListConsumerGroupsResult
{
    /// <summary>Consumer group listings.</summary>
    public List<ConsumerGroupListing> Valid { get; set; } = [];

    /// <summary>Errors.</summary>
    public List<Error> Errors { get; set; } = [];
}

/// <summary>
/// Consumer group listing.
/// </summary>
public class ConsumerGroupListing
{
    /// <summary>Group ID.</summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>Is simple consumer group.</summary>
    public bool IsSimpleConsumerGroup { get; set; }

    /// <summary>Group state.</summary>
    public ConsumerGroupState State { get; set; }
}

/// <summary>
/// Result of describe consumer groups.
/// </summary>
public class DescribeConsumerGroupsResult
{
    /// <summary>Consumer group descriptions.</summary>
    public List<ConsumerGroupDescription> ConsumerGroupDescriptions { get; set; } = [];
}

/// <summary>
/// Consumer group description.
/// </summary>
public class ConsumerGroupDescription
{
    /// <summary>Group ID.</summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>Is simple consumer group.</summary>
    public bool IsSimpleConsumerGroup { get; set; }

    /// <summary>Members.</summary>
    public List<MemberDescription> Members { get; set; } = [];

    /// <summary>Partition assignor.</summary>
    public string PartitionAssignor { get; set; } = string.Empty;

    /// <summary>Group state.</summary>
    public ConsumerGroupState State { get; set; }

    /// <summary>Coordinator.</summary>
    public Node? Coordinator { get; set; }

    /// <summary>Any error.</summary>
    public Error? Error { get; set; }
}

/// <summary>
/// Consumer group member description.
/// </summary>
public class MemberDescription
{
    /// <summary>Member ID.</summary>
    public string MemberId { get; set; } = string.Empty;

    /// <summary>Client ID.</summary>
    public string ClientId { get; set; } = string.Empty;

    /// <summary>Host.</summary>
    public string Host { get; set; } = string.Empty;

    /// <summary>Group instance ID.</summary>
    public string? GroupInstanceId { get; set; }

    /// <summary>Assignment.</summary>
    public MemberAssignment? Assignment { get; set; }
}

/// <summary>
/// Member assignment.
/// </summary>
public class MemberAssignment
{
    /// <summary>Assigned partitions.</summary>
    public List<TopicPartition> TopicPartitions { get; set; } = [];
}

/// <summary>
/// Result of list consumer group offsets.
/// </summary>
public class ListConsumerGroupOffsetsResult
{
    /// <summary>Groups with their offsets.</summary>
    public List<ConsumerGroupTopicPartitionOffsets> Groups { get; set; } = [];
}

/// <summary>
/// Result of delete records.
/// </summary>
public class DeleteRecordsResult
{
    /// <summary>Topic partition.</summary>
    public TopicPartition Partition { get; set; } = null!;

    /// <summary>New low watermark.</summary>
    public Offset Offset { get; set; }

    /// <summary>Any error.</summary>
    public Error? Error { get; set; }
}

/// <summary>
/// Cluster metadata.
/// </summary>
public class Metadata
{
    /// <summary>Broker metadata.</summary>
    public List<BrokerMetadata> Brokers { get; set; } = [];

    /// <summary>Topic metadata.</summary>
    public List<TopicMetadata> Topics { get; set; } = [];

    /// <summary>Originating broker ID.</summary>
    public int OriginatingBrokerId { get; set; }

    /// <summary>Originating broker name.</summary>
    public string OriginatingBrokerName { get; set; } = string.Empty;
}

/// <summary>
/// Broker metadata.
/// </summary>
public class BrokerMetadata
{
    /// <summary>Broker ID.</summary>
    public int BrokerId { get; set; }

    /// <summary>Host.</summary>
    public string Host { get; set; } = string.Empty;

    /// <summary>Port.</summary>
    public int Port { get; set; }
}

/// <summary>
/// Topic metadata.
/// </summary>
public class TopicMetadata
{
    /// <summary>Topic name.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>Partition metadata.</summary>
    public List<PartitionMetadata> Partitions { get; set; } = [];

    /// <summary>Any error.</summary>
    public Error? Error { get; set; }
}

/// <summary>
/// Partition metadata.
/// </summary>
public class PartitionMetadata
{
    /// <summary>Partition ID.</summary>
    public int PartitionId { get; set; }

    /// <summary>Leader broker ID.</summary>
    public int Leader { get; set; }

    /// <summary>Replica broker IDs.</summary>
    public int[] Replicas { get; set; } = [];

    /// <summary>In-sync replica broker IDs.</summary>
    public int[] InSyncReplicas { get; set; } = [];

    /// <summary>Any error.</summary>
    public Error? Error { get; set; }
}
