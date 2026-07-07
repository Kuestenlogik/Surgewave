using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Broker.KeyValue;
using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Broker.ShareGroups;
using Kuestenlogik.Surgewave.Broker.StreamsGroups;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Surgewave broker server - Kafka wire-compatible message broker
/// </summary>
public sealed class SurgewaveBroker : IAsyncDisposable, ISurgewaveStreamHandler
{
    private readonly BrokerConfig _config;
    private readonly LogManager _logManager;
    private readonly ILogger<SurgewaveBroker> _logger;
    private readonly IProtocolHandler _protocolHandler;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly List<Task> _clientTasks = [];
    private bool _disposed;

    // Request dispatcher (O(1) frozen dictionary lookup). Volatile because
    // cluster startup swaps in a new dispatcher via AddHandler once the
    // inter-broker components exist (#69), after the accept loop is already
    // serving; the frozen map can't be mutated in place, so we publish a
    // fresh instance and let readers pick it up on their next dispatch.
    private volatile RequestDispatcher _dispatcher;

    // Consumer group coordination (delegated handlers use these directly)
    private readonly ConsumerGroupCoordinator _consumerGroupCoordinator;
    private readonly ShareGroupCoordinator _shareGroupCoordinator;
    private readonly ConsumerGroupV2Coordinator _consumerGroupV2Coordinator;
    private readonly StreamsGroupCoordinator _streamsGroupCoordinator;
    private readonly TransactionCoordinator _transactionCoordinator;

    // Metrics
    private readonly BrokerMetrics _metrics;

    // TLS handler
    private readonly TlsHandler? _tlsHandler;

    // Surgewave native protocol handler
    private readonly SurgewaveNativeHandler _nativeHandler;
    private readonly ILoggerFactory _nativeLoggerFactory;
    private readonly ConnectorRepositoryManager _connectorRepositoryManager;

    /// <summary>
    /// Connector-repository manager backing the Native <c>SearchPlugins</c>
    /// operation. Exposed so Program.cs can call
    /// <see cref="ConnectorRepositoryManager.SyncFromStore"/> with the
    /// broker's <c>RepositoryStore</c> singleton at startup — without that
    /// hop the manager would keep its hard-coded NuGet.org default and
    /// silently ignore everything the operator edits in
    /// <c>/plugins/sources</c>.
    /// </summary>
    public ConnectorRepositoryManager ConnectorRepositoryManager => _connectorRepositoryManager;

    // Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory
    // private readonly SharedMemoryNativeHandler? _sharedMemoryHandler;

    public SurgewaveBroker(
        BrokerConfig config,
        LogManager logManager,
        RecordBatchSerializer serializer,
        ConsumerGroupCoordinator consumerGroupCoordinator,
        ShareGroupCoordinator shareGroupCoordinator,
        NativeGroupCoordinator nativeGroupCoordinator,
        TransactionCoordinator transactionCoordinator,
        QuotaManager quotaManager,
        IProtocolHandler protocolHandler,
        BrokerMetrics metrics,
        RequestDispatcher dispatcher,
        ILogger<SurgewaveBroker> logger,
        ConsumerGroupV2Coordinator? consumerGroupV2Coordinator = null,
        StreamsGroupCoordinator? streamsGroupCoordinator = null,
        SchemaStore? schemaStore = null,
        CompatibilityChecker? compatibilityChecker = null,
        ConnectWorker? connectWorker = null,
        PluginDiscovery? pluginDiscovery = null,
        DlqManager? dlqManager = null,
        Transactions.CrossTopicTransactionManager? crossTopicTxnManager = null,
        KvBucketManager? kvBucketManager = null,
        Kuestenlogik.Surgewave.Core.Monitoring.ILagCalculator? lagCalculator = null)
    {
        _config = config;
        _logManager = logManager;
        _consumerGroupCoordinator = consumerGroupCoordinator;
        _shareGroupCoordinator = shareGroupCoordinator;
        _consumerGroupV2Coordinator = consumerGroupV2Coordinator!;
        _streamsGroupCoordinator = streamsGroupCoordinator!;
        _transactionCoordinator = transactionCoordinator;
        _protocolHandler = protocolHandler;
        _metrics = metrics;
        _dispatcher = dispatcher;
        _logger = logger;

        // Register state accessors for observable gauges
        _metrics.RegisterStateAccessors(
            () => _logManager.ListTopics().Count(),
            () => _logManager.ListTopics().Sum(t =>
            {
                long totalSize = 0;
                for (int i = 0; i < t.PartitionCount; i++)
                {
                    var log = _logManager.GetLog(new Core.Models.TopicPartition { Topic = t.Name, Partition = i });
                    if (log != null) totalSize += log.TotalSize;
                }
                return totalSize;
            }));

        // Create listener - use dual-stack for localhost if enabled
        if (_config.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase))
        {
            if (_config.EnableDualMode)
            {
                // Dual-stack: accept both IPv4 and IPv6 connections
                _listener = new TcpListener(IPAddress.IPv6Any, _config.Port);
                _listener.Server.DualMode = true;
            }
            else
            {
                // IPv4 only: bind to 127.0.0.1 and advertise the resolved address
                // so that clients (librdkafka) don't resolve "localhost" to ::1 (IPv6)
                _listener = new TcpListener(IPAddress.Loopback, _config.Port);
                _config.Host = "127.0.0.1";
            }
        }
        else
        {
            var endpoint = new IPEndPoint(IPAddress.Parse(_config.Host), _config.Port);
            _listener = new TcpListener(endpoint);
        }

        // Initialize TLS handler if enabled
        if (_config.Security.TlsEnabled)
        {
            _tlsHandler = new TlsHandler(_config.Security);
        }

        // Initialize Surgewave native protocol handler
        _nativeLoggerFactory = LoggerFactory.Create(b => b.AddConsole());
        // Connector-Repository-Manager fuer SearchPlugins/InstallPlugin Native-Ops.
        // Verwendet den konfigurierten PluginsDirectory (gleich der vom Connect-Plugin
        // gescannt wird), so dass Control's Plugin-Marketplace die installierten
        // Connectoren als "installed" listet.
        var pluginsDirAbsolute = Path.GetFullPath(_config.Connect.PluginsDirectory ?? "plugins");
        _connectorRepositoryManager = new ConnectorRepositoryManager(pluginsDirAbsolute);
        _nativeHandler = new SurgewaveNativeHandler(
            _logManager,
            serializer,
            nativeGroupCoordinator,
            _config,
            _nativeLoggerFactory.CreateLogger<SurgewaveNativeHandler>(),
            transactionCoordinator: transactionCoordinator,
            quotaManager: quotaManager,
            schemaStore: schemaStore,
            compatibilityChecker: compatibilityChecker,
            connectWorker: connectWorker,
            pluginDiscovery: pluginDiscovery,
            dlqManager: dlqManager,
            crossTopicTxnManager: crossTopicTxnManager,
            kvBucketManager: kvBucketManager,
            repositoryManager: _connectorRepositoryManager,
            lagCalculator: lagCalculator);

        // Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory
        // Shared memory handler requires the Kuestenlogik.Surgewave.Transport.SharedMemory package.
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        Log.BrokerStarting(_logger, _config.Host, _config.Port);
        Log.DataDirectory(_logger, _config.DataDirectory);

        // Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory

        _listener.Start();
        Log.BrokerStarted(_logger);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);

        while (!linkedCts.Token.IsCancellationRequested)
        {
            try
            {
                var client = await _listener.AcceptTcpClientAsync(linkedCts.Token);
                var clientTask = HandleClientAsync(client, _shutdownCts.Token);
                _clientTasks.Add(clientTask);
                _clientTasks.RemoveAll(t => t.IsCompleted);
            }
            catch (OperationCanceledException) { break; }
            catch (SocketException) { break; } // Listener was stopped
            catch (ObjectDisposedException) { break; } // Listener was disposed
            catch (Exception ex) { Log.ErrorAcceptingClient(_logger, ex); }
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        using var _ = client;
        var endpoint = client.Client.RemoteEndPoint;
        Log.ClientConnected(_logger, endpoint);

        _metrics.RecordConnectionOpened();
        _metrics.IncrementActiveConnections();

        var clientHost = endpoint is IPEndPoint ipEndpoint ? ipEndpoint.Address.ToString() : "*";

        try
        {
#pragma warning disable CA2000
            Stream stream;
            if (_tlsHandler != null)
            {
                try
                {
                    Log.TlsHandshakeStarting(_logger, endpoint);
                    stream = await _tlsHandler.AuthenticateAsServerAsync(client.GetStream(), cancellationToken);
#pragma warning restore CA2000
                    Log.TlsHandshakeCompleted(_logger, endpoint);
                }
                catch (Exception ex)
                {
                    Log.TlsHandshakeFailed(_logger, ex, endpoint);
                    _metrics.RecordError("tls_handshake_failed");
                    return;
                }
            }
            else
            {
                stream = client.GetStream();
            }

            await using var _stream = stream;
            await HandleAsync(stream, clientHost, endpoint, cancellationToken);
        }
        catch (Exception ex)
        {
            Log.ClientError(_logger, ex, endpoint);
            _metrics.RecordError("client_error");
        }
        finally
        {
            _metrics.DecrementActiveConnections();
            Log.ClientDisconnected(_logger, endpoint);
        }
    }

    /// <summary>
    /// Transport-neutral entry point: auto-detects the wire protocol from the first
    /// four bytes on <paramref name="stream"/> and dispatches to the Surgewave native or
    /// Kafka handler accordingly. Used both by the broker's own TCP accept loop and
    /// by alternative transports (QUIC, shared memory) that plug in via
    /// <see cref="SurgewaveStreamHandlerHolder"/>.
    /// </summary>
    public async Task HandleAsync(
        Stream stream,
        string clientHost,
        EndPoint? endpoint,
        CancellationToken cancellationToken)
    {
        // Read exactly 4 bytes for the magic-byte probe. Short reads happen on slow
        // links (and especially on QUIC where each datagram may deliver 1–2 bytes
        // depending on pacing), so loop until we've got all four.
        var magicBuffer = new byte[4];
        var totalRead = 0;
        while (totalRead < 4)
        {
            var n = await stream.ReadAsync(magicBuffer.AsMemory(totalRead, 4 - totalRead), cancellationToken);
            if (n == 0)
            {
                Log.EndOfStream(_logger, endpoint);
                return;
            }
            totalRead += n;
        }

        if (magicBuffer.AsSpan().SequenceEqual(SurgewaveNativeProtocol.Magic))
        {
            await _nativeHandler.HandleConnectionAsync(stream, cancellationToken);
            return;
        }

        var connectionState = new ConnectionState(clientHost);
        await HandleKafkaConnectionAsync(stream, magicBuffer, connectionState, endpoint, cancellationToken);
    }

    private async Task HandleKafkaConnectionAsync(
        Stream stream,
        byte[] initialBytes,
        ConnectionState connectionState,
        EndPoint? endpoint,
        CancellationToken cancellationToken)
    {
        // Channel-based pipeline: reader task reads ahead while processor handles current request.
        // This reduces latency by overlapping network I/O with request processing.
        // Requests are processed in order (SingleReader) to maintain partition ordering guarantees.
        var requestChannel = Channel.CreateBounded<PendingKafkaRequest>(new BoundedChannelOptions(_config.KafkaPipelineDepth)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        // PipeWriter batches response writes — same as PipeReader batches reads.
        // minimumBufferSize: 4096 keeps small responses in a single buffer segment.
        var pipeWriter = System.IO.Pipelines.PipeWriter.Create(stream,
            new System.IO.Pipelines.StreamPipeWriterOptions(minimumBufferSize: 4096));

        // Start reader and processor as concurrent tasks
        var readerTask = ReadKafkaRequestsAsync(stream, initialBytes, requestChannel.Writer, endpoint, cancellationToken);
        var processorTask = ProcessKafkaRequestsAsync(pipeWriter, connectionState, requestChannel.Reader, endpoint, cancellationToken);

        // Wait for either to complete (reader finishes on disconnect, processor on channel completion)
        await Task.WhenAny(readerTask, processorTask);

        // Ensure channel is completed so processor can drain and exit
        requestChannel.Writer.TryComplete();

        // Wait for both to finish
        try { await Task.WhenAll(readerTask, processorTask); }
        catch (OperationCanceledException) { /* Expected on shutdown */ }
    }

    /// <summary>
    /// Reader task: reads Kafka requests from PipeReader and enqueues them into the channel.
    /// PipeReader batches socket reads internally (65KB buffer), reducing syscalls by 10x+
    /// compared to the previous ReadExactlyAsync approach. Same pattern as SurgewaveNativeHandler.
    /// </summary>
    private async Task ReadKafkaRequestsAsync(
        Stream stream,
        byte[] initialBytes,
        ChannelWriter<PendingKafkaRequest> writer,
        EndPoint? endpoint,
        CancellationToken cancellationToken)
    {
        using var prefixedStream = new PrefixedStream(stream, initialBytes);
        var pipeReader = System.IO.Pipelines.PipeReader.Create(prefixedStream,
            new System.IO.Pipelines.StreamPipeReaderOptions(bufferSize: 65536, minimumReadSize: 4));

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                Log.WaitingForRequest(_logger, endpoint);

                // Read at least 4 bytes for the Kafka message size prefix
                var readResult = await pipeReader.ReadAtLeastAsync(4, cancellationToken);
                if (readResult.IsCompleted && readResult.Buffer.Length < 4)
                {
                    Log.EndOfStream(_logger, endpoint);
                    break;
                }

                var buffer = readResult.Buffer;

                // Parse 4-byte big-endian size prefix from the pipe buffer
                int size;
                if (buffer.FirstSpan.Length >= 4)
                {
                    // Fast path: first segment has at least 4 bytes (common case)
                    size = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(buffer.FirstSpan);
                }
                else
                {
                    // Slow path: size prefix spans two segments (rare)
                    Span<byte> sizeSpan = [0, 0, 0, 0];
                    buffer.Slice(0, 4).CopyTo(sizeSpan);
                    size = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(sizeSpan);
                }

                if (size <= 0 || size > 100 * 1024 * 1024)
                {
                    _logger.LogWarning("Invalid Kafka request size {Size} from {Endpoint}", size, endpoint);
                    pipeReader.AdvanceTo(buffer.Start);
                    break;
                }

                var totalFrameSize = 4 + size;

                // Ensure the complete frame (size prefix + body) is buffered
                if (buffer.Length < totalFrameSize)
                {
                    pipeReader.AdvanceTo(buffer.Start, buffer.End);
                    readResult = await pipeReader.ReadAtLeastAsync(totalFrameSize, cancellationToken);
                    if (readResult.IsCompleted && readResult.Buffer.Length < totalFrameSize)
                    {
                        Log.EndOfStream(_logger, endpoint);
                        break;
                    }
                    buffer = readResult.Buffer;
                }

                // Copy request body into a pooled buffer for zero-copy parsing
                var requestBody = buffer.Slice(4, size);
                var rentedBuffer = ArrayPool<byte>.Shared.Rent(size);
                requestBody.CopyTo(rentedBuffer);

                // Parse using the Kafka protocol parser
                KafkaRequest? request;
                try
                {
                    using var memoryStream = new MemoryStream(rentedBuffer, 0, size, writable: false, publiclyVisible: true);
                    using var reader = new BinaryReader(memoryStream);
                    request = KafkaProtocolHandler.ParseRequestFromReader(reader, size);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to parse Kafka request ({Size} bytes) from {Endpoint}", size, endpoint);
                    ArrayPool<byte>.Shared.Return(rentedBuffer);
                    pipeReader.AdvanceTo(buffer.GetPosition(totalFrameSize));
                    continue;
                }

                // Advance pipe past the consumed frame
                pipeReader.AdvanceTo(buffer.GetPosition(totalFrameSize));

                Log.RequestReceived(_logger, endpoint, request.ApiKey, size, request.CorrelationId);

                await writer.WriteAsync(new PendingKafkaRequest(request, size, rentedBuffer), cancellationToken);
            }
        }
        catch (EndOfStreamException) { Log.EndOfStream(_logger, endpoint); }
        catch (IOException ex) { Log.IoError(_logger, ex, endpoint); _metrics.RecordError("io_error"); }
        catch (OperationCanceledException) { /* Shutdown */ }
        finally
        {
            writer.TryComplete();
            await pipeReader.CompleteAsync();
        }
    }

    /// <summary>
    /// Processor task: dequeues Kafka requests from channel and processes them in order.
    /// Sequential processing maintains partition ordering guarantees.
    /// </summary>
    private async Task ProcessKafkaRequestsAsync(
        System.IO.Pipelines.PipeWriter pipeWriter,
        ConnectionState connectionState,
        ChannelReader<PendingKafkaRequest> reader,
        EndPoint? endpoint,
        CancellationToken cancellationToken)
    {
        await foreach (var pending in reader.ReadAllAsync(cancellationToken))
        {
            try
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var response = await ProcessRequestAsync(pending.Request, connectionState, cancellationToken);
                sw.Stop();

                _metrics.RecordRequest(pending.Request.ApiKey.ToString(), sw.Elapsed.TotalMilliseconds);
                Log.SendingResponse(_logger, endpoint, response.CorrelationId);

                // Write response directly into PipeWriter — batches multiple responses
                // into a single socket write, reducing syscalls on the response path.
                WriteResponseToPipeWriter(pipeWriter, response);
                await pipeWriter.FlushAsync(cancellationToken);
                Log.ResponseSent(_logger, endpoint);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // Don't kill the connection for a single request failure — log and continue
                // so subsequent requests on the same connection still work. Without this, an
                // unhandled exception from any handler or response serialiser would tear down
                // the socket and surface as "Broker transport failure" on the client, masking
                // the real error.
                _logger.LogError(ex, "Unhandled exception processing {ApiKey} (correlationId={CorrelationId})",
                    pending.Request.ApiKey, pending.Request.CorrelationId);
            }
            finally
            {
                // Return rented buffer to pool
                ArrayPool<byte>.Shared.Return(pending.RentedBuffer);
            }
        }
    }

    /// <summary>
    /// Writes a Kafka response directly into a PipeWriter's buffer — zero intermediate
    /// allocation. The response body is serialized into a thread-local KafkaProtocolWriter,
    /// then the 4-byte size prefix + body are written into the PipeWriter's Span in one shot.
    /// </summary>
    private static void WriteResponseToPipeWriter(System.IO.Pipelines.PipeWriter pipeWriter, KafkaResponse response)
    {
        // Serialize response body into thread-local writer (no allocation)
        var bodyWriter = KafkaProtocolHandler.GetThreadLocalWriter();
        bodyWriter.Reset();
        response.WriteTo(bodyWriter);

        var bodySpan = bodyWriter.WrittenSpan;
        var totalLength = 4 + bodySpan.Length;

        // Write size prefix + body directly into PipeWriter's buffer (zero-copy)
        var destination = pipeWriter.GetSpan(totalLength);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(destination, bodySpan.Length);
        bodySpan.CopyTo(destination.Slice(4));
        pipeWriter.Advance(totalLength);
    }

    private async Task<KafkaResponse> ProcessRequestAsync(KafkaRequest request, ConnectionState connectionState, CancellationToken cancellationToken)
    {
        // Handle consumer group APIs directly (they use sync methods that return Tasks)
        // This is faster than going through the dispatcher for these common operations
        switch (request)
        {
            case OffsetCommitRequest offsetCommitRequest:
                return _consumerGroupCoordinator.HandleOffsetCommit(offsetCommitRequest);
            case OffsetFetchRequest offsetFetchRequest:
                return _consumerGroupCoordinator.HandleOffsetFetch(offsetFetchRequest);
            case JoinGroupRequest joinGroupRequest:
                return _consumerGroupCoordinator.HandleJoinGroup(joinGroupRequest);
            case SyncGroupRequest syncGroupRequest:
                return _consumerGroupCoordinator.HandleSyncGroup(syncGroupRequest);
            case HeartbeatRequest heartbeatRequest:
                return _consumerGroupCoordinator.HandleHeartbeat(heartbeatRequest);
            case LeaveGroupRequest leaveGroupRequest:
                return _consumerGroupCoordinator.HandleLeaveGroup(leaveGroupRequest);
            case DescribeGroupsRequest describeGroupsRequest:
                return _consumerGroupCoordinator.HandleDescribeGroups(describeGroupsRequest);
            case ListGroupsRequest listGroupsRequest:
                return _consumerGroupCoordinator.HandleListGroups(listGroupsRequest);

            // Handle consumer group v2 APIs directly (fast-path for KIP-848)
            case ConsumerGroupHeartbeatRequest consumerGroupHeartbeatRequest:
                return _consumerGroupV2Coordinator.HandleConsumerGroupHeartbeat(consumerGroupHeartbeatRequest);
            case ConsumerGroupDescribeRequest consumerGroupDescribeRequest:
                return _consumerGroupV2Coordinator.HandleConsumerGroupDescribe(consumerGroupDescribeRequest);

            // Handle streams group APIs directly (fast-path for KIP-1071)
            case StreamsGroupHeartbeatRequest streamsGroupHeartbeatRequest:
                return _streamsGroupCoordinator.HandleStreamsGroupHeartbeat(streamsGroupHeartbeatRequest);
            case StreamsGroupDescribeRequest streamsGroupDescribeRequest:
                return _streamsGroupCoordinator.HandleStreamsGroupDescribe(streamsGroupDescribeRequest);

            // Handle share group APIs directly (fast-path for common operations)
            case ShareGroupHeartbeatRequest shareGroupHeartbeatRequest:
                return _shareGroupCoordinator.HandleShareGroupHeartbeat(shareGroupHeartbeatRequest);
            case ShareFetchRequest shareFetchRequest:
                return await _shareGroupCoordinator.HandleShareFetch(shareFetchRequest, cancellationToken);
            case ShareAcknowledgeRequest shareAcknowledgeRequest:
                return _shareGroupCoordinator.HandleShareAcknowledge(shareAcknowledgeRequest);

            // Handle transaction APIs directly
            case InitProducerIdRequest initProducerIdRequest:
                return await _transactionCoordinator.HandleInitProducerIdAsync(initProducerIdRequest, cancellationToken);
            case AddPartitionsToTxnRequest addPartitionsToTxnRequest:
                return _transactionCoordinator.HandleAddPartitionsToTxn(addPartitionsToTxnRequest);
            case AddOffsetsToTxnRequest addOffsetsToTxnRequest:
                return _transactionCoordinator.HandleAddOffsetsToTxn(addOffsetsToTxnRequest);
            case EndTxnRequest endTxnRequest:
                return await _transactionCoordinator.HandleEndTxnAsync(endTxnRequest, cancellationToken);
            case TxnOffsetCommitRequest txnOffsetCommitRequest:
                return _transactionCoordinator.HandleTxnOffsetCommit(txnOffsetCommitRequest);
            case DescribeProducersRequest describeProducersRequest:
                return _transactionCoordinator.HandleDescribeProducers(describeProducersRequest);
            case DescribeTransactionsRequest describeTransactionsRequest:
                return _transactionCoordinator.HandleDescribeTransactions(describeTransactionsRequest);
            case ListTransactionsRequest listTransactionsRequest:
                return _transactionCoordinator.HandleListTransactions(listTransactionsRequest);
        }

        // Use dispatcher for all other requests (O(1) frozen dictionary lookup)
        var context = new RequestContext
        {
            ConnectionState = connectionState,
            ClientId = request.ClientId
        };

        return await _dispatcher.DispatchAsync(request, context, cancellationToken);
    }

    /// <summary>
    /// Register an additional request handler after the broker is already
    /// serving. Used by cluster startup to add the inter-broker handler
    /// (LeaderAndIsr / StopReplica / UpdateMetadata) once the cluster
    /// components have been created — those depend on the broker being up
    /// first, so the handler can't be part of the initial frozen dispatcher.
    /// Publishes a fresh dispatcher; in-flight dispatches keep using the old
    /// one and the next request picks up the new map (#69).
    /// </summary>
    public void AddHandler(IKafkaRequestHandler handler)
    {
        _dispatcher = _dispatcher.WithAdditionalHandler(handler);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        Log.ShutdownStarting(_logger);
        _listener.Stop();
        Log.ShutdownStoppedListener(_logger);

        if (_clientTasks.Count > 0)
        {
            Log.ShutdownWaitingForClients(_logger, _clientTasks.Count);
            var shutdownTimeout = TimeSpan.FromSeconds(_config.ShutdownTimeoutSeconds);

            try { await Task.WhenAll(_clientTasks).WaitAsync(shutdownTimeout); }
            catch (TimeoutException) { Log.ShutdownClientTimeout(_logger, _clientTasks.Count(t => !t.IsCompleted)); }
            catch (Exception ex) { Log.ShutdownClientError(_logger, ex); }
        }

        _shutdownCts.Cancel();

        Log.ShutdownDisposingResources(_logger);
        _logManager.Dispose();
        _tlsHandler?.Dispose();
        // Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory
        _nativeLoggerFactory.Dispose();
        _connectorRepositoryManager.Dispose();
        _shutdownCts.Dispose();
        _listener.Dispose();

        _disposed = true;
        Log.ShutdownComplete(_logger);
    }
}
