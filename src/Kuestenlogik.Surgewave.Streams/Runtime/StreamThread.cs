using System.Threading.Channels;
using Kuestenlogik.Surgewave.Streams.Processors;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// A stream thread that owns a subset of partitions and processes records independently.
/// Each StreamThread runs its own processing loop reading from a bounded Channel.
/// Kafka Streams equivalent: StreamThread.java — but using lock-free System.Threading.Channels.
/// </summary>
internal sealed class StreamThread : IAsyncDisposable
{
    private readonly int _threadId;
    private readonly StreamsConfig _config;
    private readonly Topology _topology;
    private readonly ILogger _logger;
    private readonly Channel<ConsumerRecord> _inputChannel;
    private readonly ProcessorContext _context;
    private readonly TaskManager _taskManager;
    private readonly CancellationTokenSource _cts = new();
    private Task? _processingTask;
    private long _processedRecords;

    public int ThreadId => _threadId;
    public string ThreadName { get; }
    public StreamThreadState State { get; private set; } = StreamThreadState.Created;
    public long ProcessedRecords => Interlocked.Read(ref _processedRecords);
    public ProcessorContext Context => _context;
    public TaskManager Tasks => _taskManager;

    /// <summary>
    /// Fired when this thread encounters a fatal (uncaught) exception.
    /// The StreamsApplication uses this to invoke the UncaughtExceptionHandler.
    /// </summary>
    internal event Action<StreamThread, Exception>? OnUncaughtException;

    /// <summary>
    /// The input channel writer for dispatching records to this thread.
    /// </summary>
    public ChannelWriter<ConsumerRecord> Writer => _inputChannel.Writer;

    public StreamThread(
        int threadId,
        StreamsConfig config,
        Topology topology,
        ILoggerFactory loggerFactory)
    {
        _threadId = threadId;
        _config = config;
        _topology = topology;
        ThreadName = $"{config.ApplicationId}-StreamThread-{threadId}";
        _logger = loggerFactory.CreateLogger($"StreamThread-{threadId}");

        // Per-thread ProcessorContext for isolation
        var metrics = new StreamsMetrics();
        _context = new ProcessorContext(config, metrics, _logger);

        // Per-thread TaskManager
        _taskManager = new TaskManager(topology, config, _context, _logger);

        // Bounded channel: backpressure when thread can't keep up
        _inputChannel = Channel.CreateBounded<ConsumerRecord>(
            new BoundedChannelOptions(config.Backpressure.MaxBufferedRecords)
            {
                SingleWriter = false, // Multiple partitions may write
                SingleReader = true,  // This thread is the sole reader
                FullMode = BoundedChannelFullMode.Wait
            });

        // Initialize state stores for this thread's context
        foreach (var supplier in topology.StateStoreSuppliers)
        {
            var getMethod = supplier.GetType().GetMethod("Get");
            if (getMethod != null)
            {
                var store = (IStateStore)getMethod.Invoke(supplier, null)!;
                _context.RegisterStateStore(store);
            }
        }

        // Initialize processor nodes
        foreach (var source in topology.Sources)
        {
            InitializeNode(source);
        }
    }

    private void InitializeNode(ProcessorNode node)
    {
        node.Init(_context);
        foreach (var child in node.Children)
        {
            InitializeNode(child);
        }
    }

    /// <summary>
    /// Starts the stream thread's processing loop.
    /// </summary>
    public void Start()
    {
        if (State != StreamThreadState.Created)
            throw new InvalidOperationException($"Cannot start StreamThread-{_threadId} from state {State}");

        _logger.LogInformation("Starting {ThreadName}", ThreadName);
        State = StreamThreadState.Running;
        _processingTask = Task.Factory.StartNew(
            () => ProcessingLoopAsync(_cts.Token),
            _cts.Token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();
    }

    private async Task ProcessingLoopAsync(CancellationToken token)
    {
        try
        {
            await foreach (var record in _inputChannel.Reader.ReadAllAsync(token))
            {
                try
                {
                    _taskManager.Process(
                        record.Topic,
                        record.Partition,
                        record.Key,
                        record.Value,
                        record.Timestamp,
                        record.Offset);

                    Interlocked.Increment(ref _processedRecords);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex, "{ThreadName}: Error processing record from {Topic}-{Partition}",
                        ThreadName, record.Topic, record.Partition);
                }

                // Punctuate after each record batch
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _taskManager.Punctuate(now);
                _taskManager.MaybeCommitAll();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{ThreadName}: Fatal error in processing loop", ThreadName);
            State = StreamThreadState.Dead;
            OnUncaughtException?.Invoke(this, ex);
        }
    }

    /// <summary>
    /// Handles partition assignment by creating tasks for assigned partitions.
    /// </summary>
    public void OnPartitionsAssigned(IEnumerable<TopicPartition> partitions)
    {
        _taskManager.OnPartitionsAssigned(partitions);
    }

    /// <summary>
    /// Handles partition revocation by closing tasks for revoked partitions.
    /// </summary>
    public void OnPartitionsRevoked(IEnumerable<TopicPartition> partitions)
    {
        _taskManager.OnPartitionsRevoked(partitions);
    }

    /// <summary>
    /// Tries to write a record to this thread's channel without blocking.
    /// </summary>
    public bool TryWrite(in ConsumerRecord record)
    {
        return _inputChannel.Writer.TryWrite(record);
    }

    /// <summary>
    /// Writes a record to this thread's channel, waiting if necessary.
    /// </summary>
    public ValueTask WriteAsync(ConsumerRecord record, CancellationToken token = default)
    {
        return _inputChannel.Writer.WriteAsync(record, token);
    }

    public async ValueTask DisposeAsync()
    {
        if (State == StreamThreadState.Dead || State == StreamThreadState.Created)
        {
            _taskManager.Dispose();
            _context.Metrics.Dispose();
            _cts.Dispose();
            return;
        }

        _logger.LogInformation("Shutting down {ThreadName}", ThreadName);
        State = StreamThreadState.PendingShutdown;

        // Complete the channel — no more writes accepted
        _inputChannel.Writer.Complete();
        _cts.Cancel();

        if (_processingTask != null)
        {
            try
            {
                await _processingTask.WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (OperationCanceledException) { }
            catch (TimeoutException)
            {
                _logger.LogWarning("{ThreadName}: Processing loop did not complete within timeout", ThreadName);
            }
        }

        // Commit and close all tasks
        _taskManager.CommitAll();
        _taskManager.CloseAll();
        _taskManager.Dispose();
        _context.Metrics.Dispose();
        _cts.Dispose();

        State = StreamThreadState.Dead;
        _logger.LogInformation("{ThreadName} shut down. Processed {Count} records", ThreadName, ProcessedRecords);
    }
}

/// <summary>
/// State of a stream thread.
/// </summary>
internal enum StreamThreadState
{
    Created,
    Running,
    PendingShutdown,
    Dead
}
