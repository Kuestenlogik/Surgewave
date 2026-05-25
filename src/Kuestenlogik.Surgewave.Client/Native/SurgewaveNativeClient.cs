using Kuestenlogik.Surgewave.Client.Native.Operations.Admin;
using Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;
using Kuestenlogik.Surgewave.Client.Native.Operations.Connect;
using Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;
using Kuestenlogik.Surgewave.Client.Native.Operations.Plugins;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Client.Native.Operations.Topics;
using Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Surgewave native protocol client for high-performance message operations.
/// Provides optimized binary protocol communication with Surgewave brokers.
/// Uses pipelined request/response for maximum throughput.
///
/// Usage:
/// <code>
/// await using var client = new SurgewaveNativeClient("localhost", 9092);
/// await client.ConnectAsync();
///
/// // Topic operations
/// await client.Topics.CreateAsync("my-topic", partitions: 3);
/// var topics = await client.Topics.ListAsync();
///
/// // Messaging operations
/// await client.Messaging.SendBatchAsync("my-topic", 0, messages);
/// var result = await client.Messaging.ReceiveAsync("my-topic", 0, offset: 0);
///
/// // Consumer group operations
/// await client.Groups.JoinAsync(groupId, memberId, ...);
///
/// // Transaction operations
/// var producerId = await client.Transactions.InitProducerIdAsync(transactionalId);
///
/// // Admin operations (quotas, ACLs, elections, broker config)
/// await client.Admin.DescribeAclsAsync();
///
/// // Schema Registry operations
/// await client.Schema.RegisterSchemaAsync(subject, schema, "AVRO");
///
/// // Connect operations
/// await client.Connect.CreateConnectorAsync(name, config);
/// </code>
/// </summary>
public sealed class SurgewaveNativeClient : IAsyncDisposable
{
    private readonly SurgewaveConnectionManager _connectionManager;

    // Lazy-initialized operation handlers (composition pattern)
    private SurgewaveTopicOperations? _topics;
    private SurgewaveMessagingOperations? _messaging;
    private SurgewaveClusterOperations? _cluster;
    private SurgewaveConsumerGroupOperations? _groups;
    private SurgewaveTransactionOperations? _transactions;
    private SurgewaveCrossTopicTransactionOperations? _crossTopicTransactions;
    private SurgewaveAdminOperations? _admin;
    private SurgewaveSchemaRegistryOperations? _schema;
    private SurgewaveConnectOperations? _connect;
    private SurgewavePluginOperations? _plugins;

    /// <summary>Topic management operations.</summary>
    public SurgewaveTopicOperations Topics => _topics ??= new(this);

    /// <summary>Produce and fetch operations.</summary>
    public SurgewaveMessagingOperations Messaging => _messaging ??= new(this);

    /// <summary>Cluster management operations.</summary>
    public SurgewaveClusterOperations Cluster => _cluster ??= new(this);

    /// <summary>Consumer group operations.</summary>
    public SurgewaveConsumerGroupOperations Groups => _groups ??= new(this);

    /// <summary>Transaction operations.</summary>
    public SurgewaveTransactionOperations Transactions => _transactions ??= new(this);

    /// <summary>Cross-topic transaction operations (atomic writes across multiple topics).</summary>
    public SurgewaveCrossTopicTransactionOperations CrossTopicTransactions => _crossTopicTransactions ??= new(this);

    /// <summary>Admin operations (quotas, ACLs, elections, broker config).</summary>
    public SurgewaveAdminOperations Admin => _admin ??= new(this);

    /// <summary>Schema Registry operations.</summary>
    public SurgewaveSchemaRegistryOperations Schema => _schema ??= new(this);

    /// <summary>Kafka Connect operations.</summary>
    public SurgewaveConnectOperations Connect => _connect ??= new(this);

    /// <summary>Plugin/Marketplace operations.</summary>
    public SurgewavePluginOperations Plugins => _plugins ??= new(this);

    /// <summary>
    /// Creates a client with the specified host and port.
    /// </summary>
#pragma warning disable CA2000 // Connection manager is disposed in DisposeAsync
    public SurgewaveNativeClient(string host, int port, bool enablePipelining = true)
        : this(new SurgewaveConnectionManager(host, port, enablePipelining))
    {
    }
#pragma warning restore CA2000

    /// <summary>
    /// Creates a client with the specified transport type.
    /// </summary>
#pragma warning disable CA2000 // Connection manager is disposed in DisposeAsync
    public SurgewaveNativeClient(
        string host,
        int port,
        SurgewaveTransportType transportType,
        bool enablePipelining = true,
        ILogger? logger = null)
        : this(new SurgewaveConnectionManager(host, port, transportType, enablePipelining, logger))
    {
    }
#pragma warning restore CA2000

    /// <summary>
    /// Creates a client with a custom transport.
    /// </summary>
#pragma warning disable CA2000 // Connection manager is disposed in DisposeAsync
    public SurgewaveNativeClient(ISurgewaveTransport transport)
        : this(new SurgewaveConnectionManager(transport))
    {
    }
#pragma warning restore CA2000

    /// <summary>
    /// Creates a client with a connection manager.
    /// </summary>
    internal SurgewaveNativeClient(SurgewaveConnectionManager connectionManager)
    {
        _connectionManager = connectionManager;
    }

    /// <summary>
    /// Connect to the Surgewave broker using native protocol.
    /// </summary>
    public Task ConnectAsync(CancellationToken cancellationToken = default)
        => _connectionManager.ConnectAsync(cancellationToken);

    /// <summary>
    /// Whether the client is currently connected.
    /// </summary>
    public bool IsConnected => _connectionManager.IsConnected;

    /// <summary>
    /// Enable or disable compression for large payloads (default: enabled).
    /// </summary>
    public bool CompressionEnabled
    {
        get => _connectionManager.CompressionEnabled;
        set => _connectionManager.CompressionEnabled = value;
    }

    /// <summary>
    /// Send a request and receive a response.
    /// Used by operation handlers to communicate with the broker.
    /// </summary>
    internal Task<(SurgewaveResponseHeader header, ReadOnlyMemory<byte> payload)> SendRequestAsync(
        SurgewaveOpCode opCode,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
        => _connectionManager.SendRequestAsync(opCode, payload, cancellationToken);

    /// <summary>
    /// Register a handler for unsolicited server-push messages.
    /// Used internally by SurgewaveStreamingConsumer.
    /// </summary>
    internal void RegisterPushHandler(SurgewaveOpCode opCode, Func<SurgewaveResponseHeader, ReadOnlyMemory<byte>, Task> handler)
        => _connectionManager.RegisterPushHandler(opCode, handler);

    /// <summary>
    /// Remove a previously registered push handler.
    /// </summary>
    internal void UnregisterPushHandler(SurgewaveOpCode opCode)
        => _connectionManager.UnregisterPushHandler(opCode);

    public ValueTask DisposeAsync() => _connectionManager.DisposeAsync();
}
