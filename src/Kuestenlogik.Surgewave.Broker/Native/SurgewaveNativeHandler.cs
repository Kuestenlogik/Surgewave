using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Broker.KeyValue;
using Kuestenlogik.Surgewave.Broker.Native.Handlers;
using Kuestenlogik.Surgewave.Broker.Native.Streaming;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Native;

/// <summary>
/// Handles Surgewave native protocol connections.
/// Provides optimized binary protocol for high-performance clients.
/// Uses FrozenDictionary-based dispatcher for O(1) handler lookup.
/// </summary>
public sealed class SurgewaveNativeHandler
{
    private readonly NativeRequestDispatcher _dispatcher;
    private readonly BrokerConfig _config;
    private readonly ILogger<SurgewaveNativeHandler> _logger;
    private readonly LogManager? _logManager;
    private readonly RecordBatchSerializer? _recordBatchSerializer;

    /// <summary>
    /// Gets the request dispatcher used by this handler.
    /// Can be shared with SharedMemoryNativeHandler for consistent request handling.
    /// </summary>
    public NativeRequestDispatcher Dispatcher => _dispatcher;

    private readonly NativeDataHandler? _dataHandler;

    /// <summary>
    /// Passes the leadership view to the data handler once clustering exists (#164).
    /// </summary>
    /// <remarks>
    /// Null on the dispatcher-injecting constructor, which builds no handlers of its own —
    /// a caller that supplies its own dispatcher owns what is in it.
    /// </remarks>
    public void SetPartitionLeadership(Kuestenlogik.Surgewave.Broker.Abstractions.Routing.IPartitionLeadership? leadership)
        => _dataHandler?.SetPartitionLeadership(leadership);

    private Kuestenlogik.Surgewave.Clustering.Cluster.ClusterView? _clusterView;

    /// <summary>
    /// Passes the view of the cluster to the handlers that describe it, once clustering exists (#272): the cluster
    /// info, the broker list and a topic's partitions are then the cluster's and not the asked broker's alone.
    /// </summary>
    public void SetClusterView(Kuestenlogik.Surgewave.Clustering.Cluster.ClusterView? cluster)
        => Volatile.Write(ref _clusterView, cluster);

    public SurgewaveNativeHandler(
        LogManager logManager,
        RecordBatchSerializer recordBatchSerializer,
        NativeGroupCoordinator groupCoordinator,
        BrokerConfig config,
        ILoggerFactory loggerFactory,
        PartitionReassignmentManager? reassignmentManager = null,
        TransactionCoordinator? transactionCoordinator = null,
        QuotaManager? quotaManager = null,
        AclAuthorizer? aclAuthorizer = null,
        SchemaStore? schemaStore = null,
        CompatibilityChecker? compatibilityChecker = null,
        ConnectWorker? connectWorker = null,
        PluginDiscovery? pluginDiscovery = null,
        ConnectorRepositoryManager? repositoryManager = null,
        DlqManager? dlqManager = null,
        CrossTopicTransactionManager? crossTopicTxnManager = null,
        KvBucketManager? kvBucketManager = null,
        Kuestenlogik.Surgewave.Core.Monitoring.ILagCalculator? lagCalculator = null)
    {
        _config = config;
        _logger = loggerFactory.CreateLogger<SurgewaveNativeHandler>();
        _logManager = logManager;
        _recordBatchSerializer = recordBatchSerializer;

        // Each handler logs under its own category, from the host's factory. They were handed this handler's logger
        // cast to their own type (`logger as ILogger<NativeDataHandler>`), which it never is, and so wrote to a
        // NullLogger: no line of the data, the key-value or the streaming handler ever appeared (#281).
        _dataHandler = new NativeDataHandler(config, logManager, recordBatchSerializer, groupCoordinator,
            loggerFactory.CreateLogger<NativeDataHandler>(),
            dlqManager: dlqManager);

        var handlers = new List<INativeRequestHandler>
        {
            _dataHandler,
            new NativeMetadataHandler(logManager),
            new NativeTopicHandler(logManager, () => Volatile.Read(ref _clusterView)),
            new NativeConsumerGroupHandler(groupCoordinator, lagCalculator),
            new NativeClusterHandler(logManager, reassignmentManager, () => Volatile.Read(ref _clusterView)),
            new NativeTransactionHandler(transactionCoordinator),
            new NativeQuotaHandler(quotaManager),
            new NativeSecurityHandler(aclAuthorizer, config),
            new NativeAdminHandler(logManager, config)
        };

        // Add Schema Registry handler if schema store is available
        if (schemaStore != null && compatibilityChecker != null)
        {
            handlers.Add(new NativeSchemaRegistryHandler(schemaStore, compatibilityChecker));
        }

        // Add Connect handler if connect worker is available
        if (connectWorker != null)
        {
            handlers.Add(new NativeConnectHandler(connectWorker, pluginDiscovery, connectEnabled: true));
        }

        // Add Plugin handler if repository manager is available
        if (repositoryManager != null)
        {
            handlers.Add(new NativePluginHandler(repositoryManager, pluginsEnabled: true, pluginDiscovery: pluginDiscovery));
        }

        // Add Cross-Topic Transaction handler
        handlers.Add(new NativeCrossTopicTxnHandler(crossTopicTxnManager));

        // Add KV Store handler if bucket manager is available
        if (kvBucketManager != null)
        {
            handlers.Add(new NativeKvStoreHandler(kvBucketManager, loggerFactory.CreateLogger<NativeKvStoreHandler>()));
        }

        // Add Streaming handler (always enabled â€” manager is per-connection, created in HandleConnectionAsync)
        handlers.Add(new NativeStreamingHandler(loggerFactory.CreateLogger<NativeStreamingHandler>()));

        _dispatcher = new NativeRequestDispatcher(handlers);
    }

    /// <summary>
    /// Alternative constructor accepting pre-built dispatcher for testing or custom handler configurations.
    /// </summary>
    public SurgewaveNativeHandler(
        NativeRequestDispatcher dispatcher,
        BrokerConfig config,
        ILogger<SurgewaveNativeHandler> logger)
    {
        _dispatcher = dispatcher;
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Handle a native protocol client connection.
    /// Called after magic bytes "SRWV" have been detected and consumed.
    /// Uses a Channel-based pipeline: reader task reads ahead while processor handles current request.
    /// A SemaphoreSlim serializes all PipeWriter access between the request-processing loop and push-streaming tasks.
    /// </summary>
    public async Task HandleConnectionAsync(Stream stream, CancellationToken cancellationToken)
    {
        // Create PipeWriter for efficient response writes (managed buffer pool, zero-allocation)
        var pipeWriter = PipeWriter.Create(stream, new StreamPipeWriterOptions(minimumBufferSize: 4096));

        // SemaphoreSlim(1,1) serializes all writes to pipeWriter from the processor loop
        // AND from concurrent push-streaming background tasks.
        var writeLock = new SemaphoreSlim(1, 1);

        // One responder per connection — holds the PipeWriter/lock that used to be captured in a
        // fresh closure for every single request (#83).
        var responder = new NativeConnectionResponder(pipeWriter, writeLock);

        // Per-connection subscription manager â€” null if logManager not available
        StreamSubscriptionManager? subscriptionManager = null;
        if (_logManager != null && _recordBatchSerializer != null)
        {
            subscriptionManager = new StreamSubscriptionManager(
                _logManager,
                _recordBatchSerializer,
                _logger,
                _config.MaxStreamingSubscriptionsPerConnection);
        }

        try
        {
            // Read protocol version (1 byte after magic)
            var versionBuffer = new byte[1];
            await stream.ReadExactlyAsync(versionBuffer, cancellationToken);
            var version = versionBuffer[0];

            if (version != SurgewaveNativeProtocol.Version)
            {
                _logger.LogWarning("Unsupported Surgewave native protocol version: {Version}", version);
                await responder.SendErrorAsync(0, SurgewaveOpCode.Handshake, SurgewaveErrorCode.InvalidRequest,
                    $"Unsupported protocol version: {version}", cancellationToken);
                return;
            }

            _logger.LogInformation("Surgewave native client connected, protocol version {Version}", version);

            // Send handshake response and determine if client supports compression
            await SendHandshakeResponseAsync(responder, cancellationToken);

            // Create PipeReader for efficient batched network reads.
            // PipeReader accumulates data in internal buffers, reducing syscalls.
            var pipeReader = PipeReader.Create(stream, new StreamPipeReaderOptions(
                bufferSize: 65536,
                minimumReadSize: SurgewaveNativeProtocol.HeaderSize));

            try
            {
                // Pipeline: PipeReader feeds channel, PipeWriter handles response writes
                var requestChannel = Channel.CreateBounded<PendingNativeRequest>(new BoundedChannelOptions(_config.NativeProtocolPipelineDepth)
                {
                    SingleWriter = true,
                    SingleReader = true,
                    FullMode = BoundedChannelFullMode.Wait
                });

                // Start reader and processor as concurrent tasks. The reader has a token of its own: it reads for
                // as long as the processor answers.
                using var readerStop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var readerTask = ReadRequestsAsync(pipeReader, requestChannel.Writer, readerStop.Token);
                var processorTask = ProcessPipelinedRequestsAsync(responder, subscriptionManager, requestChannel.Reader, cancellationToken);

                // Wait for either to complete (reader finishes on disconnect, processor on channel completion)
                var first = await Task.WhenAny(readerTask, processorTask);

                // Ensure channel is completed so processor can drain and exit
                requestChannel.Writer.TryComplete();

                // A processor that ended before the reader ended on what it could not answer or write. Nothing
                // answers the requests still to come, so the reader stops and the caller closes the connection: the
                // reader used to go on waiting for the client's next request, and a client waiting for the answer to
                // its last one sends none (#281).
                if (first == processorTask)
                {
                    await readerStop.CancelAsync();
                }

                // Wait for both to finish (processor drains remaining items)
                try { await Task.WhenAll(readerTask, processorTask); }
                catch (OperationCanceledException) { /* Expected on shutdown */ }
            }
            finally
            {
                await pipeReader.CompleteAsync();
            }
        }
        finally
        {
            // Stop all push subscriptions before closing the connection
            if (subscriptionManager != null)
            {
                await subscriptionManager.UnsubscribeAllAsync();
                await subscriptionManager.DisposeAsync();
            }

            writeLock.Dispose();
            await pipeWriter.CompleteAsync();
        }
    }

    /// <summary>
    /// Reader task: reads requests from PipeReader and enqueues them into the channel.
    /// PipeReader batches socket reads internally, reducing syscalls.
    /// Runs concurrently with the processor, enabling read-ahead pipelining.
    /// </summary>
    private async Task ReadRequestsAsync(
        PipeReader pipeReader,
        ChannelWriter<PendingNativeRequest> writer,
        CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Read at least the header from the pipe
                var result = await pipeReader.ReadAtLeastAsync(SurgewaveNativeProtocol.HeaderSize, cancellationToken);
                if (result.IsCompleted && result.Buffer.Length < SurgewaveNativeProtocol.HeaderSize)
                    break; // Client disconnected

                var buffer = result.Buffer;

                // Parse header directly from pipe buffer (zero-copy for single-segment)
                var header = ParseHeader(buffer);

                // Validate payload size - if invalid, we've lost framing and must disconnect
                if (header.PayloadLength < 0 || header.PayloadLength > SurgewaveNativeProtocol.MaxPayloadSize)
                {
                    _logger.LogWarning("Invalid payload size {Size} from native client, disconnecting", header.PayloadLength);
                    pipeReader.AdvanceTo(buffer.Start);
                    break;
                }

                var totalFrameSize = SurgewaveNativeProtocol.HeaderSize + header.PayloadLength;

                // Ensure we have the complete frame (header + payload)
                if (buffer.Length < totalFrameSize)
                {
                    // Tell PipeReader: nothing consumed yet, everything examined
                    pipeReader.AdvanceTo(buffer.Start, buffer.End);
                    result = await pipeReader.ReadAtLeastAsync(totalFrameSize, cancellationToken);
                    if (result.IsCompleted && result.Buffer.Length < totalFrameSize)
                        break;
                    buffer = result.Buffer;
                }

                // Copy payload to ArrayPool buffer for Channel ownership transfer
                var rentedPayload = ArrayPool<byte>.Shared.Rent(header.PayloadLength);
                try
                {
                    buffer.Slice(SurgewaveNativeProtocol.HeaderSize, header.PayloadLength).CopyTo(rentedPayload);

                    // Advance pipe past the consumed frame
                    pipeReader.AdvanceTo(buffer.GetPosition(totalFrameSize));

                    // Decompress if needed
                    byte[]? decompressedPayload = null;
                    if ((header.Flags & SurgewaveProtocolFlags.Compressed) != 0)
                    {
                        decompressedPayload = NativeCompressionCodec.DecompressWithHeader(
                            rentedPayload.AsSpan(0, header.PayloadLength));
                    }

                    // Enqueue for processing (blocks if channel is full = backpressure)
                    await writer.WriteAsync(new PendingNativeRequest(header, rentedPayload, header.PayloadLength, decompressedPayload), cancellationToken);
                }
                catch
                {
                    // Return buffer on failure before re-throwing
                    ArrayPool<byte>.Shared.Return(rentedPayload);
                    throw;
                }
            }
        }
        catch (InvalidOperationException)
        {
            // PipeReader completed (e.g., stream closed)
            _logger.LogDebug("Native client disconnected");
        }
        catch (IOException ex)
        {
            _logger.LogDebug(ex, "Native client I/O error");
        }
        catch (OperationCanceledException) { /* Shutdown */ }
        finally
        {
            writer.TryComplete();
        }
    }

    /// <summary>
    /// Parse request header from a ReadOnlySequence. Fast path for single-segment buffers (common case).
    /// </summary>
    private static SurgewaveRequestHeader ParseHeader(ReadOnlySequence<byte> buffer)
    {
        // Fast path: header fits in first segment (almost always true with 64KB buffer)
        if (buffer.FirstSpan.Length >= SurgewaveNativeProtocol.HeaderSize)
        {
            return SurgewaveRequestHeader.ReadFrom(buffer.FirstSpan);
        }

        // Slow path: header spans segments â€” copy to stack
        Span<byte> headerBytes = stackalloc byte[SurgewaveNativeProtocol.HeaderSize];
        buffer.Slice(0, SurgewaveNativeProtocol.HeaderSize).CopyTo(headerBytes);
        return SurgewaveRequestHeader.ReadFrom(headerBytes);
    }

    /// <summary>
    /// Processor task: dequeues requests from the channel and processes them.
    /// The writeLock serializes PipeWriter access between this loop and push-streaming background tasks.
    /// </summary>
    /// <remarks>
    /// Every request is answered or the connection is closed (#281). Whatever a handler throws that it did not
    /// turn into an answer itself is answered here with <see cref="SurgewaveErrorCode.InternalError"/> and logged
    /// with the request it belongs to; the request's frame was read completely, so the connection goes on. Two
    /// things a handler throws are not the broker's failure and are answered as what they are (#290): a payload it
    /// could not read is the client's, <see cref="SurgewaveErrorCode.InvalidRequest"/>, and a topic no controller
    /// took is nobody's, <see cref="SurgewaveErrorCode.NotController"/>, which a client asks again on. Neither is
    /// logged as an error: a client that sends what cannot be read would write the broker's log by itself. Two
    /// things end the loop instead, and with it the connection: the broker stopping, which cancels the handlers and
    /// is not answered request by request, and an answer that cannot be written — a client that went while its
    /// request was being carried out is no failed request and is not logged as one.
    /// </remarks>
    private async Task ProcessPipelinedRequestsAsync(
        NativeConnectionResponder responder,
        StreamSubscriptionManager? subscriptionManager,
        ChannelReader<PendingNativeRequest> reader,
        CancellationToken cancellationToken)
    {
        // One reusable context per connection: this loop is the single reader and awaits each
        // handler to completion, so only Header changes between requests (#83).
        var context = new NativeRequestContext(responder, _config, subscriptionManager);

        await foreach (var request in reader.ReadAllAsync(cancellationToken))
        {
            try
            {
                // Determine actual payload (decompressed or original slice)
                ReadOnlyMemory<byte> actualPayload = request.DecompressedPayload != null
                    ? request.DecompressedPayload
                    : request.RentedPayload.AsMemory(0, request.PayloadLength);

                context.SetHeader(request.Header);

                // Dispatch to appropriate handler using O(1) lookup
                bool handled;
                try
                {
                    handled = await _dispatcher.TryDispatchAsync(context, actualPayload, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception) when (responder.WriteFailed)
                {
                    // The handler failed on writing its answer: the client has gone, or a frame is partly on
                    // the wire. That is the connection's end, which the caller logs, not a failed request.
                    throw;
                }
                catch (SurgewavePayloadException ex)
                {
                    _logger.LogDebug("Native request {RequestId} ({OpCode}) could not be read and was answered {ErrorCode}: {Reason}",
                        request.Header.RequestId, request.Header.OpCode, SurgewaveErrorCode.InvalidRequest, ex.Message);
                    await responder.SendErrorAsync(request.Header.RequestId, request.Header.OpCode,
                        SurgewaveErrorCode.InvalidRequest,
                        $"The payload of {request.Header.OpCode} (request {request.Header.RequestId}) could not be read: {ex.Message}",
                        cancellationToken);
                    continue;
                }
                catch (TopicNotCreatedException ex)
                {
                    _logger.LogInformation("Native request {RequestId} ({OpCode}) was answered {ErrorCode}: {Reason}",
                        request.Header.RequestId, request.Header.OpCode, SurgewaveErrorCode.NotController, ex.Message);
                    await responder.SendErrorAsync(request.Header.RequestId, request.Header.OpCode,
                        SurgewaveErrorCode.NotController, ex.Message, cancellationToken);
                    continue;
                }
                catch (Exception ex)
                {
                    // The exception is the operator's; the client gets the request to ask the operator about.
                    _logger.LogError(ex, "Native request {RequestId} ({OpCode}) failed in its handler and was answered {ErrorCode}",
                        request.Header.RequestId, request.Header.OpCode, SurgewaveErrorCode.InternalError);
                    await responder.SendErrorAsync(request.Header.RequestId, request.Header.OpCode,
                        SurgewaveErrorCode.InternalError,
                        $"The broker failed to carry out {request.Header.OpCode} (request {request.Header.RequestId}); the cause is in the broker's log",
                        cancellationToken);
                    continue;
                }

                if (!handled)
                {
                    await responder.SendErrorAsync(request.Header.RequestId, request.Header.OpCode,
                        SurgewaveErrorCode.InvalidRequest, $"Unknown opcode: {request.Header.OpCode}", cancellationToken);
                }
            }
            finally
            {
                // Always return rented buffer to pool
                ArrayPool<byte>.Shared.Return(request.RentedPayload);
            }
        }
    }

    private async Task SendHandshakeResponseAsync(NativeConnectionResponder responder, CancellationToken cancellationToken)
    {
        // Response: version(1) + capabilities(4)
        var payload = new byte[5];
        payload[0] = SurgewaveNativeProtocol.Version;
        // Capabilities flags - only advertise compression if enabled in config
        payload[1] = (byte)(_config.NativeProtocolCompressionEnabled ? 1 : 0); // compression support (LZ4)
        payload[2] = 1; // streaming support
        payload[3] = 0; // reserved
        payload[4] = 0; // reserved

        // Sent uncompressed: the client only learns compression is available from this response.
        await responder.SendResponseAsync(0, SurgewaveOpCode.Handshake, SurgewaveErrorCode.None, payload, cancellationToken);
        responder.ClientSupportsCompression = _config.NativeProtocolCompressionEnabled;
    }

}
