using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Performance;

/// <summary>
/// High-performance prefetching consumer that fetches messages in the background.
/// Uses batch-based buffering with Channel for lock-free batch passing between fetcher and consumer.
/// Designed to match or exceed librdkafka consumer performance.
/// </summary>
public sealed class SurgewavePrefetchingConsumer : IAsyncDisposable
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _topic;
    private readonly int _partition;
    private readonly int _maxBytesPerFetch;
    private readonly Channel<ReceiveResult> _batchChannel;
    private readonly Task _fetcherTask;
    private readonly CancellationTokenSource _cts = new();
    private long _nextFetchOffset;
    private long _highWatermark;
    private volatile bool _endOfPartition;
    private volatile int _bufferedBatches;

    // Current batch state - only touched by consumer thread
    private ReceiveResult? _currentBatch;
    private int _currentIndex;

    public SurgewavePrefetchingConsumer(
        SurgewaveNativeClient client,
        string topic,
        int partition,
        long startOffset = 0,
        int prefetchCount = 50000,
        int maxBytesPerFetch = 1024 * 1024)
    {
        _client = client;
        _topic = topic;
        _partition = partition;
        _nextFetchOffset = startOffset;
        _maxBytesPerFetch = maxBytesPerFetch;

        // Buffer batches (not individual messages) - much lower overhead
        // Each batch contains ~10K messages with 1MB fetch, so 10 batches = ~100K messages
        var batchBufferSize = Math.Max(10, prefetchCount / 10000);
        _batchChannel = Channel.CreateBounded<ReceiveResult>(new BoundedChannelOptions(batchBufferSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = true
        });

        _fetcherTask = Task.Run(FetcherLoopAsync);
    }

    public long HighWatermark => Volatile.Read(ref _highWatermark);
    public bool EndOfPartition => _endOfPartition;
    public int BufferedBatches => _bufferedBatches;

    /// <summary>
    /// Consume a single message. Returns null if no message available.
    /// Ultra-fast hot path - just array index increment.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReceivedMessage? Consume()
    {
        // Hot path: return next message from current batch
        if (_currentBatch != null && _currentIndex < _currentBatch.Messages.Count)
        {
            return _currentBatch.Messages[_currentIndex++];
        }

        // Need new batch - try without blocking
        return TryGetNextBatchMessage();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private ReceivedMessage? TryGetNextBatchMessage()
    {
        // Try to get a new batch without blocking
        if (_batchChannel.Reader.TryRead(out var batch))
        {
            Interlocked.Decrement(ref _bufferedBatches);
            _currentBatch = batch;
            _currentIndex = 0;

            if (batch.Messages.Count > 0)
            {
                return batch.Messages[_currentIndex++];
            }
        }

        _currentBatch = null;
        return null;
    }

    /// <summary>
    /// Consume a single message asynchronously.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<ReceivedMessage?> ConsumeAsync(CancellationToken cancellationToken = default)
    {
        // Hot path: return next message from current batch
        if (_currentBatch != null && _currentIndex < _currentBatch.Messages.Count)
        {
            return new ValueTask<ReceivedMessage?>(_currentBatch.Messages[_currentIndex++]);
        }

        return ConsumeSlowPathAsync(cancellationToken);
    }

    private async ValueTask<ReceivedMessage?> ConsumeSlowPathAsync(CancellationToken cancellationToken)
    {
        // Try sync path first
        var msg = TryGetNextBatchMessage();
        if (msg != null) return msg;

        // Wait for new batch
        try
        {
            if (await _batchChannel.Reader.WaitToReadAsync(cancellationToken))
            {
                if (_batchChannel.Reader.TryRead(out var batch))
                {
                    Interlocked.Decrement(ref _bufferedBatches);
                    _currentBatch = batch;
                    _currentIndex = 0;

                    if (batch.Messages.Count > 0)
                    {
                        return batch.Messages[_currentIndex++];
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on cancellation
        }
        catch (ChannelClosedException)
        {
            // Channel completed
        }

        return null;
    }

    /// <summary>
    /// Consume a batch of messages at once for maximum throughput.
    /// Returns the messages directly from internal buffer - avoid per-message overhead.
    /// </summary>
    public ReceiveResult? ConsumeBatch()
    {
        // First exhaust current batch
        if (_currentBatch != null)
        {
            var batch = _currentBatch;
            _currentBatch = null;
            _currentIndex = 0;
            return batch;
        }

        // Get new batch
        if (_batchChannel.Reader.TryRead(out var newBatch))
        {
            Interlocked.Decrement(ref _bufferedBatches);
            return newBatch;
        }

        return null;
    }

    /// <summary>
    /// Consume a batch of messages asynchronously.
    /// </summary>
    public async ValueTask<ReceiveResult?> ConsumeBatchAsync(CancellationToken cancellationToken = default)
    {
        var batch = ConsumeBatch();
        if (batch != null) return batch;

        try
        {
            if (await _batchChannel.Reader.WaitToReadAsync(cancellationToken))
            {
                if (_batchChannel.Reader.TryRead(out var newBatch))
                {
                    Interlocked.Decrement(ref _bufferedBatches);
                    return newBatch;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        catch (ChannelClosedException)
        {
            // Channel completed
        }

        return null;
    }

    private async Task FetcherLoopAsync()
    {
        var consecutiveEmptyFetches = 0;
        const int maxConsecutiveEmpty = 10;

        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = await _client.Messaging.ReceiveAsync(
                        _topic, _partition, _nextFetchOffset, _maxBytesPerFetch, maxWaitMs: 5000, _cts.Token);

                    Volatile.Write(ref _highWatermark, result.HighWatermark);

                    if (result.Messages.Count == 0)
                    {
                        consecutiveEmptyFetches++;

                        if (_nextFetchOffset >= result.HighWatermark)
                        {
                            _endOfPartition = true;
                        }

                        var delayMs = Math.Min(consecutiveEmptyFetches, maxConsecutiveEmpty);
                        if (delayMs > 0)
                        {
                            await Task.Delay(delayMs, _cts.Token);
                        }
                        continue;
                    }

                    consecutiveEmptyFetches = 0;
                    _endOfPartition = false;

                    // Write entire batch to channel - single channel operation per batch!
                    await _batchChannel.Writer.WriteAsync(result, _cts.Token);
                    Interlocked.Increment(ref _bufferedBatches);

                    _nextFetchOffset = result.Messages[^1].Offset + 1;
                }
                catch (OperationCanceledException) when (_cts.Token.IsCancellationRequested)
                {
                    break;
                }
                catch (ChannelClosedException)
                {
                    break;
                }
                catch (Exception)
                {
                    try
                    {
                        await Task.Delay(100, _cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            _batchChannel.Writer.TryComplete();
        }
    }

    public async Task WaitForBufferAsync(int minMessageCount, CancellationToken cancellationToken = default)
    {
        // Estimate batches needed (assuming ~10K messages per batch with 1MB fetch)
        var minBatches = Math.Max(1, minMessageCount / 10000);
        while (_bufferedBatches < minBatches && !_endOfPartition && !cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(1, cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _batchChannel.Writer.TryComplete();

        try
        {
            await _fetcherTask.WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (TimeoutException)
        {
            // Force completion
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        catch (ChannelClosedException)
        {
            // Expected
        }

        _cts.Dispose();
    }
}
