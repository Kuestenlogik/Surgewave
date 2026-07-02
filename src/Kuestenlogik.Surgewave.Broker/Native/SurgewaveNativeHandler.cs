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

    public SurgewaveNativeHandler(
        LogManager logManager,
        RecordBatchSerializer recordBatchSerializer,
        NativeGroupCoordinator groupCoordinator,
        BrokerConfig config,
        ILogger<SurgewaveNativeHandler> logger,
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
        _logger = logger;
        _logManager = logManager;
        _recordBatchSerializer = recordBatchSerializer;

        // Create handlers
        var handlers = new List<INativeRequestHandler>
        {
            new NativeDataHandler(config, logManager, recordBatchSerializer, groupCoordinator,
                logger as ILogger<NativeDataHandler> ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<NativeDataHandler>.Instance,
                dlqManager: dlqManager),
            new NativeMetadataHandler(logManager),
            new NativeTopicHandler(logManager),
            new NativeConsumerGroupHandler(groupCoordinator, lagCalculator),
            new NativeClusterHandler(logManager, reassignmentManager),
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
            handlers.Add(new NativeKvStoreHandler(kvBucketManager,
                logger as ILogger<NativeKvStoreHandler> ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<NativeKvStoreHandler>.Instance));
        }

        // Add Streaming handler (always enabled â€” manager is per-connection, created in HandleConnectionAsync)
        handlers.Add(new NativeStreamingHandler(
            logger as ILogger<NativeStreamingHandler> ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<NativeStreamingHandler>.Instance));

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
                await SendErrorAsync(pipeWriter, writeLock, false, 0, SurgewaveOpCode.Handshake, SurgewaveErrorCode.InvalidRequest,
                    $"Unsupported protocol version: {version}", cancellationToken);
                return;
            }

            _logger.LogInformation("Surgewave native client connected, protocol version {Version}", version);

            // Send handshake response and determine if client supports compression
            var clientSupportsCompression = await SendHandshakeResponseAsync(pipeWriter, writeLock, cancellationToken);

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

                // Start reader and processor as concurrent tasks
                var readerTask = ReadRequestsAsync(pipeReader, requestChannel.Writer, cancellationToken);
                var processorTask = ProcessPipelinedRequestsAsync(pipeWriter, writeLock, clientSupportsCompression, subscriptionManager, requestChannel.Reader, cancellationToken);

                // Wait for either to complete (reader finishes on disconnect, processor on channel completion)
                await Task.WhenAny(readerTask, processorTask);

                // Ensure channel is completed so processor can drain and exit
                requestChannel.Writer.TryComplete();

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
    private async Task ProcessPipelinedRequestsAsync(
        PipeWriter pipeWriter,
        SemaphoreSlim writeLock,
        bool clientSupportsCompression,
        StreamSubscriptionManager? subscriptionManager,
        ChannelReader<PendingNativeRequest> reader,
        CancellationToken cancellationToken)
    {
        await foreach (var request in reader.ReadAllAsync(cancellationToken))
        {
            try
            {
                // Determine actual payload (decompressed or original slice)
                ReadOnlyMemory<byte> actualPayload = request.DecompressedPayload != null
                    ? request.DecompressedPayload
                    : request.RentedPayload.AsMemory(0, request.PayloadLength);

                await ProcessRequestAsync(pipeWriter, writeLock, clientSupportsCompression, subscriptionManager, request.Header, actualPayload, cancellationToken);
            }
            finally
            {
                // Always return rented buffer to pool
                ArrayPool<byte>.Shared.Return(request.RentedPayload);
            }
        }
    }

    private async Task ProcessRequestAsync(
        PipeWriter pipeWriter,
        SemaphoreSlim writeLock,
        bool clientSupportsCompression,
        StreamSubscriptionManager? subscriptionManager,
        SurgewaveRequestHeader header,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        // Create context for handlers â€” PipeWriter is captured in the closures.
        // writeLock ensures push-streaming tasks and response writes don't race on the PipeWriter.
        var context = new NativeRequestContext
        {
            Header = header,
            Config = _config,
            SendResponseAsync = (reqId, opCode, errorCode, p, ct) =>
                SendResponseAsync(pipeWriter, writeLock, clientSupportsCompression, reqId, opCode, errorCode, p, ct),
            SendErrorAsync = (reqId, opCode, errorCode, msg, ct) =>
                SendErrorAsync(pipeWriter, writeLock, clientSupportsCompression, reqId, opCode, errorCode, msg, ct),
            ClientSupportsCompression = clientSupportsCompression,
            SubscriptionManager = subscriptionManager
        };

        // Dispatch to appropriate handler using O(1) lookup
        var handled = await _dispatcher.TryDispatchAsync(context, payload, cancellationToken);

        if (!handled)
        {
            await SendErrorAsync(pipeWriter, writeLock, clientSupportsCompression, header.RequestId, header.OpCode,
                SurgewaveErrorCode.InvalidRequest, $"Unknown opcode: {header.OpCode}", cancellationToken);
        }
    }

    private async Task<bool> SendHandshakeResponseAsync(PipeWriter pipeWriter, SemaphoreSlim writeLock, CancellationToken cancellationToken)
    {
        // Response: version(1) + capabilities(4)
        var payload = new byte[5];
        payload[0] = SurgewaveNativeProtocol.Version;
        // Capabilities flags - only advertise compression if enabled in config
        payload[1] = (byte)(_config.NativeProtocolCompressionEnabled ? 1 : 0); // compression support (LZ4)
        payload[2] = 1; // streaming support
        payload[3] = 0; // reserved
        payload[4] = 0; // reserved

        var clientSupportsCompression = _config.NativeProtocolCompressionEnabled;

        await SendResponseAsync(pipeWriter, writeLock, false, 0, SurgewaveOpCode.Handshake, SurgewaveErrorCode.None, payload, cancellationToken);
        return clientSupportsCompression;
    }

    private async Task SendResponseAsync(PipeWriter pipeWriter, SemaphoreSlim writeLock, bool clientSupportsCompression,
        uint requestId, SurgewaveOpCode opCode, SurgewaveErrorCode errorCode, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var flags = SurgewaveProtocolFlags.None;
        ReadOnlyMemory<byte> actualPayload = payload;

        // Compress large responses if client supports it
        if (clientSupportsCompression && payload.Length >= NativeCompressionCodec.MinCompressionSize)
        {
            var (compressed, wasCompressed) = NativeCompressionCodec.CompressWithHeader(payload.Span);
            if (wasCompressed)
            {
                actualPayload = compressed;
                flags |= SurgewaveProtocolFlags.Compressed;
            }
        }

        var header = new SurgewaveResponseHeader
        {
            Flags = flags,
            RequestId = requestId,
            OpCode = opCode,
            ErrorCode = errorCode,
            PayloadLength = actualPayload.Length
        };

        // Serialize writes: both the processor loop and push-streaming tasks share this PipeWriter.
        await writeLock.WaitAsync(cancellationToken);
        try
        {
            // Write header + payload directly into PipeWriter's managed buffer (zero-allocation)
            var totalLength = SurgewaveResponseHeader.Size + actualPayload.Length;
            var span = pipeWriter.GetSpan(totalLength);
            header.WriteTo(span);
            if (actualPayload.Length > 0)
            {
                actualPayload.Span.CopyTo(span.Slice(SurgewaveResponseHeader.Size));
            }
            pipeWriter.Advance(totalLength);

            // Flush to push data to the socket
            await pipeWriter.FlushAsync(cancellationToken);
        }
        finally
        {
            writeLock.Release();
        }
    }

    private Task SendErrorAsync(PipeWriter pipeWriter, SemaphoreSlim writeLock, bool clientSupportsCompression,
        uint requestId, SurgewaveOpCode opCode, SurgewaveErrorCode errorCode, string message, CancellationToken cancellationToken)
    {
        var messageBytes = System.Text.Encoding.UTF8.GetBytes(message);
        var payload = new byte[2 + messageBytes.Length];
        BinaryPrimitives.WriteInt16BigEndian(payload.AsSpan(0, 2), (short)messageBytes.Length);
        messageBytes.CopyTo(payload.AsSpan(2));

        return SendResponseAsync(pipeWriter, writeLock, clientSupportsCompression, requestId, SurgewaveOpCode.Error, errorCode, payload, cancellationToken);
    }
}
