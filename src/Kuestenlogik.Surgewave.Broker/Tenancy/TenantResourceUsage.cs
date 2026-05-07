namespace Kuestenlogik.Surgewave.Broker.Tenancy;

public sealed record TenantResourceUsage(
    TenantId TenantId,
    int TopicCount,
    int PartitionCount,
    int ConsumerGroupCount,
    long StorageBytesEstimate,
    long ProduceBytesPerSecond,
    long FetchBytesPerSecond,
    int ActiveConnections);
