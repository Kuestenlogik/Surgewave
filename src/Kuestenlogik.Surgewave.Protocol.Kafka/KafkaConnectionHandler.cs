using System.Buffers;
using System.Net;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Broker;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Kafka wire-protocol connection handler (#59 b5). Owns the complete Kafka request/response
/// loop that previously lived inline in <c>SurgewaveBroker</c>: a channel-split reader/processor
/// pipeline (network read overlaps request processing), zero-copy <see cref="System.IO.Pipelines.PipeReader"/>
/// framing on the read side and <see cref="System.IO.Pipelines.PipeWriter"/> batching on the write
/// side, an <c>ArrayPool</c>-rented body buffer per request, and an O(1) FrozenDictionary dispatch
/// (<see cref="RequestDispatcher"/>) to a single <see cref="IKafkaRequestHandler"/>. The lift is
/// verbatim — no new virtualization or allocation on the hot path. Registered as the catch-all
/// <see cref="IConnectionHandler"/> (<see cref="Order"/> = <see cref="int.MaxValue"/>,
/// <see cref="CanHandle"/> = <c>true</c>) so it claims any connection no more specific handler wanted.
/// </summary>
public sealed class KafkaConnectionHandler : IConnectionHandler
{
    // Volatile: the embedded runtime hot-adds the inter-broker handler once cluster components
    // exist (#69). The frozen map can't be mutated in place, so AddHandler publishes a fresh
    // dispatcher and in-flight dispatches keep using the old one until their next request.
    private volatile RequestDispatcher _dispatcher;
    private readonly IBrokerMetrics? _metrics;
    private readonly int _pipelineDepth;
    private readonly ILogger<KafkaConnectionHandler> _logger;

    public KafkaConnectionHandler(
        IEnumerable<IKafkaRequestHandler> handlers,
        int pipelineDepth,
        IBrokerMetrics? metrics,
        ILogger<KafkaConnectionHandler> logger,
        ILogger<RequestDispatcher>? dispatcherLogger = null)
    {
        _dispatcher = new RequestDispatcher(handlers, dispatcherLogger);
        _pipelineDepth = pipelineDepth <= 0 ? 1 : pipelineDepth;
        _metrics = metrics;
        _logger = logger;
    }

    /// <summary>Catch-all: Kafka claims connections only after all specific handlers declined.</summary>
    public int Order => int.MaxValue;

    /// <summary>The Kafka framing prefix IS the request size header, so any peeked bytes are accepted.</summary>
    public bool CanHandle(ReadOnlySpan<byte> magic) => true;

    /// <summary>
    /// Hot-add a request handler after construction, rebuilding the frozen dispatcher. Used by the
    /// embedded runtime to register the inter-broker handler (LeaderAndIsr / StopReplica /
    /// UpdateMetadata / AlterPartition) once the cluster components exist (#69). In the DI host path
    /// every handler is registered up-front, so this is not called there.
    /// </summary>
    public void AddHandler(IKafkaRequestHandler handler)
        => _dispatcher = _dispatcher.WithAdditionalHandler(handler);

    public Task HandleConnectionAsync(
        Stream stream,
        ReadOnlyMemory<byte> magic,
        ConnectionContext context,
        CancellationToken cancellationToken)
        => HandleKafkaConnectionAsync(
            stream, magic.ToArray(), new ConnectionState(context.ClientHost), context.Endpoint, cancellationToken);

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
        var requestChannel = Channel.CreateBounded<PendingKafkaRequest>(new BoundedChannelOptions(_pipelineDepth)
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
    /// compared to the previous ReadExactlyAsync approach.
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
        catch (IOException ex) { Log.IoError(_logger, ex, endpoint); _metrics?.RecordError("io_error"); }
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

                _metrics?.RecordRequest(pending.Request.ApiKey.ToString(), sw.Elapsed.TotalMilliseconds);
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
                // so subsequent requests on the same connection still work.
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
        // All coordinator APIs route through the dispatcher to their protocol-neutral ApiHandler
        // adapters, which own the Kafka<->neutral conversion (#59). Dispatch via the frozen
        // dictionary (O(1) lookup).
        var context = new RequestContext
        {
            ConnectionState = connectionState,
            ClientId = request.ClientId
        };

        return await _dispatcher.DispatchAsync(request, context, cancellationToken);
    }
}
