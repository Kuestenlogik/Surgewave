using Kuestenlogik.Surgewave.Api.Grpc.Server;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// Holders for late-binding gRPC service implementations.
/// Required because the gRPC services need dependencies that are only available after app.Build().
/// </summary>
public static class GrpcServiceHolders
{
    public static ProducerServiceImpl? Producer { get; set; }
    public static ConsumerServiceImpl? Consumer { get; set; }
    public static TopicServiceImpl? Topic { get; set; }
    public static AdminServiceImpl? Admin { get; set; }
    public static ConsumerGroupServiceImpl? ConsumerGroup { get; set; }
    public static ClusterServiceImpl? Cluster { get; set; }
    public static TransactionServiceImpl? Transaction { get; set; }
    public static QuotaServiceImpl? Quota { get; set; }
    public static SecurityServiceImpl? Security { get; set; }
    public static SchemaRegistryServiceImpl? SchemaRegistry { get; set; }
    public static ConnectServiceImpl? Connect { get; set; }
}

// Individual holder classes for backward compatibility with DI registration
internal static class ProducerServiceImplHolder
{
    public static ProducerServiceImpl? Instance
    {
        get => GrpcServiceHolders.Producer;
        set => GrpcServiceHolders.Producer = value;
    }
}

internal static class ConsumerServiceImplHolder
{
    public static ConsumerServiceImpl? Instance
    {
        get => GrpcServiceHolders.Consumer;
        set => GrpcServiceHolders.Consumer = value;
    }
}

internal static class TopicServiceImplHolder
{
    public static TopicServiceImpl? Instance
    {
        get => GrpcServiceHolders.Topic;
        set => GrpcServiceHolders.Topic = value;
    }
}

internal static class AdminServiceImplHolder
{
    public static AdminServiceImpl? Instance
    {
        get => GrpcServiceHolders.Admin;
        set => GrpcServiceHolders.Admin = value;
    }
}

internal static class ConsumerGroupServiceImplHolder
{
    public static ConsumerGroupServiceImpl? Instance
    {
        get => GrpcServiceHolders.ConsumerGroup;
        set => GrpcServiceHolders.ConsumerGroup = value;
    }
}

internal static class ClusterServiceImplHolder
{
    public static ClusterServiceImpl? Instance
    {
        get => GrpcServiceHolders.Cluster;
        set => GrpcServiceHolders.Cluster = value;
    }
}

internal static class TransactionServiceImplHolder
{
    public static TransactionServiceImpl? Instance
    {
        get => GrpcServiceHolders.Transaction;
        set => GrpcServiceHolders.Transaction = value;
    }
}

internal static class QuotaServiceImplHolder
{
    public static QuotaServiceImpl? Instance
    {
        get => GrpcServiceHolders.Quota;
        set => GrpcServiceHolders.Quota = value;
    }
}

internal static class SecurityServiceImplHolder
{
    public static SecurityServiceImpl? Instance
    {
        get => GrpcServiceHolders.Security;
        set => GrpcServiceHolders.Security = value;
    }
}

internal static class SchemaRegistryServiceImplHolder
{
    public static SchemaRegistryServiceImpl? Instance
    {
        get => GrpcServiceHolders.SchemaRegistry;
        set => GrpcServiceHolders.SchemaRegistry = value;
    }
}

internal static class ConnectServiceImplHolder
{
    public static ConnectServiceImpl? Instance
    {
        get => GrpcServiceHolders.Connect;
        set => GrpcServiceHolders.Connect = value;
    }
}
