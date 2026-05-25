namespace Kuestenlogik.Surgewave.Control.Models;

public record ClusterInfo(
    string ClusterId,
    int ControllerId,
    int BrokerCount,
    int TopicCount,
    int PartitionCount);

public record BrokerInfo(
    int BrokerId,
    string Host,
    int Port,
    string? Rack,
    bool IsController,
    string PeerTransport = "tcp");

public record TopicInfo(
    string Name,
    int PartitionCount,
    int ReplicationFactor,
    bool IsInternal);

public record TopicDescription(
    string Name,
    int PartitionCount,
    int ReplicationFactor,
    bool IsInternal,
    IReadOnlyList<PartitionInfo> Partitions,
    IReadOnlyDictionary<string, string> Configs);

public record PartitionInfo(
    int PartitionId,
    int Leader,
    IReadOnlyList<int> Replicas,
    IReadOnlyList<int> Isr,
    long HighWatermark,
    long LogStartOffset);

public record ConsumerGroupInfo(
    string GroupId,
    string State,
    string ProtocolType,
    int MemberCount);

public record ConsumerGroupDescription(
    string GroupId,
    string State,
    string ProtocolType,
    string Protocol,
    int CoordinatorId,
    IReadOnlyList<MemberInfo> Members);

public record MemberInfo(
    string MemberId,
    string ClientId,
    string ClientHost,
    IReadOnlyList<TopicPartitionAssignment> Assignments);

public record TopicPartitionAssignment(
    string Topic,
    IReadOnlyList<int> Partitions);

public record CreateTopicRequest(
    string Name,
    int NumPartitions,
    short ReplicationFactor,
    IDictionary<string, string>? Configs = null);

public record HealthDetails
{
    public string Status { get; init; } = "Unknown";
    public DateTime Timestamp { get; init; }
    public int BrokerId { get; init; }
    public string Host { get; init; } = "";
    public int Port { get; init; }
    public int GrpcPort { get; init; }
    public int TopicsCount { get; init; }
    public int BrokersCount { get; init; }
    public int ControllerId { get; init; }
    public bool RaftEnabled { get; init; }
    public string? RaftState { get; init; }
    public int? RaftLeaderId { get; init; }
    public HealthChecks? Checks { get; init; }
}

public record HealthChecks
{
    public string Broker { get; init; } = "Unknown";
    public string Grpc { get; init; } = "Unknown";
    public string Storage { get; init; } = "Unknown";
}
