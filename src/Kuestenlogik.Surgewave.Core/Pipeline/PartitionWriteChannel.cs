using System.Buffers;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Core.Pipeline;

/// <summary>
/// Dedicated write channel for a single partition.
/// Provides zero-contention writes with automatic batching.
/// Uses dedicated thread with CPU affinity for predictable low latency.
/// </summary>
public sealed class PartitionWriteChannel : IAsyncDisposable
{
    private readonly TopicPartition _topicPartition;
    private readonly Channel<PartitionWriteRequest> _channel;
    private readonly DedicatedThread _workerThread;
    private readonly Func<TopicPartition, IPartitionLog> _logProvider;
    private readonly int _batchSize;
    private bool _disposed;

    // Pre-allocated batch list for reuse
    private readonly List<PartitionWriteRequest> _batchBuffer;

    public TopicPartition TopicPartition => _topicPartition;

    /// <summary>
    /// Creates a partition write channel with optional CPU affinity.
    /// </summary>
    /// <param name="topicPartition">The topic-partition this channel writes to</param>
    /// <param name="logProvider">Function to get the partition log</param>
    /// <param name="channelCapacity">Max pending write requests</param>
    /// <param name="batchSize">Max requests to batch together</param>
    /// <param name="cpuAffinity">CPU core to pin writer thread to (-1 for auto, null for no affinity)</param>
    public PartitionWriteChannel(
        TopicPartition topicPartition,
        Func<TopicPartition, IPartitionLog> logProvider,
        int channelCapacity = 1000,
        int batchSize = 100,
        int? cpuAffinity = -1)
    {
        _topicPartition = topicPartition;
        _logProvider = logProvider;
        _batchSize = batchSize;
        _batchBuffer = new List<PartitionWriteRequest>(batchSize);

        // Single-reader channel for maximum performance
        _channel = Channel.CreateBounded<PartitionWriteRequest>(new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,  // Only our worker reads
            SingleWriter = false, // Multiple producers can write
            AllowSynchronousContinuations = true // Reduce context switches
        });

        // Use dedicated thread with CPU affinity for consistent low latency
        _workerThread = new DedicatedThread(
            ct => ProcessWritesSync(ct),
            name: $"PartitionWriter-{topicPartition.Topic}-{topicPartition.Partition}",
            cpuAffinity: cpuAffinity,
            priority: ThreadPriority.AboveNormal);
    }

    /// <summary>
    /// Write a record batch to this partition's channel.
    /// Uses ReadOnlyMemory for zero-copy when possible.
    /// </summary>
    public ValueTask<long> WriteAsync(ReadOnlyMemory<byte> recordBatch, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var request = PartitionWriteRequest.Create(recordBatch, cancellationToken);

        // Fast path: try synchronous write first
        if (_channel.Writer.TryWrite(request))
        {
            return new ValueTask<long>(request.GetResultAsync());
        }

        // Slow path: async write
        return WriteAsyncCore(request, cancellationToken);
    }

    private async ValueTask<long> WriteAsyncCore(PartitionWriteRequest request, CancellationToken cancellationToken)
    {
        await _channel.Writer.WriteAsync(request, cancellationToken);
        return await request.GetResultAsync();
    }

    /// <summary>
    /// Worker that processes writes with batching for optimal throughput.
    /// Runs on dedicated thread with CPU affinity for consistent low latency.
    /// </summary>
    private void ProcessWritesSync(CancellationToken cancellationToken)
    {
        var log = _logProvider(_topicPartition);

        // Synchronous read loop on dedicated thread
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Wait for first request (blocks on dedicated thread - this is intentional)
                if (!_channel.Reader.TryRead(out var request))
                {
                    // Use synchronous wait - we're on a dedicated thread
                    // Fast path: check if ValueTask completed synchronously to avoid .AsTask() allocation
                    var waitTask = _channel.Reader.WaitToReadAsync(cancellationToken);
                    bool canRead;
                    if (waitTask.IsCompletedSuccessfully)
                        canRead = waitTask.Result;
                    else
                        canRead = waitTask.AsTask().GetAwaiter().GetResult();

                    if (!canRead)
                    {
                        break; // Channel completed
                    }
                    continue;
                }

                // Start batch with first request
                _batchBuffer.Add(request);

                // Greedily collect more requests without blocking (up to batch size)
                while (_batchBuffer.Count < _batchSize && _channel.Reader.TryRead(out var nextRequest))
                {
                    _batchBuffer.Add(nextRequest);
                }

                // Process batch - write all to log
                foreach (var req in _batchBuffer)
                {
                    try
                    {
                        // Zero-copy: extract underlying array if available
                        long offset;
                        if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(req.RecordBatch, out var segment))
                        {
                            // Array-backed memory - use slice overload (no allocation)
                            // Fast path: avoid .AsTask() allocation if completed synchronously
                            var appendTask = log.AppendBatchAsync(segment.Array!, segment.Offset, segment.Count, req.CancellationToken);
                            if (appendTask.IsCompletedSuccessfully)
                                offset = appendTask.Result;
                            else
                                offset = appendTask.AsTask().GetAwaiter().GetResult();
                        }
                        else
                        {
                            // Rare case: not array-backed, must copy
                            var appendTask = log.AppendBatchAsync(req.RecordBatch.ToArray(), req.CancellationToken);
                            if (appendTask.IsCompletedSuccessfully)
                                offset = appendTask.Result;
                            else
                                offset = appendTask.AsTask().GetAwaiter().GetResult();
                        }
                        req.SetResult(offset);
                    }
                    catch (Exception ex)
                    {
                        req.SetException(ex);
                    }
                }

                _batchBuffer.Clear();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // Complete all pending requests with error
                foreach (var req in _batchBuffer)
                {
                    req.SetException(ex);
                }
                _batchBuffer.Clear();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        _channel.Writer.Complete();
        _workerThread.Stop();

        try
        {
            await _workerThread.WaitForCompletionAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (TimeoutException)
        {
            // Worker didn't finish in time
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        _workerThread.Dispose();
    }
}

/// <summary>
/// Write request for partition channel using ReadOnlyMemory for zero-copy.
/// Pooled for minimal allocations.
/// </summary>
public sealed class PartitionWriteRequest
{
    private static readonly ObjectPool<PartitionWriteRequest> s_pool = new(() => new PartitionWriteRequest(), 1024);

    private TaskCompletionSource<long>? _tcs;

    public ReadOnlyMemory<byte> RecordBatch { get; private set; }
    public CancellationToken CancellationToken { get; private set; }

    private PartitionWriteRequest() { }

    public static PartitionWriteRequest Create(ReadOnlyMemory<byte> recordBatch, CancellationToken cancellationToken)
    {
        var request = s_pool.Rent();
        request.RecordBatch = recordBatch;
        request.CancellationToken = cancellationToken;
        // TODO: Optimize - consider using PooledCompletionSource<long> here to reduce TCS allocations.
        // Cannot use it currently because PooledCompletionSource.Return() resets the ValueTask source,
        // but the consumer may not have read the result yet when SetResult/ReturnToPool is called.
        // Would require a two-phase return protocol where the consumer signals completion.
        request._tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        return request;
    }

    public Task<long> GetResultAsync() => _tcs!.Task;

    public void SetResult(long offset)
    {
        _tcs!.TrySetResult(offset);
        ReturnToPool();
    }

    public void SetException(Exception ex)
    {
        _tcs!.TrySetException(ex);
        ReturnToPool();
    }

    private void ReturnToPool()
    {
        RecordBatch = default;
        CancellationToken = default;
        _tcs = null;
        s_pool.Return(this);
    }
}

/// <summary>
/// Simple object pool for high-performance scenarios.
/// </summary>
internal sealed class ObjectPool<T> where T : class
{
    private readonly Func<T> _factory;
    private readonly T?[] _items;
    private int _index;

    public ObjectPool(Func<T> factory, int maxSize)
    {
        _factory = factory;
        _items = new T[maxSize];
    }

    public T Rent()
    {
        var items = _items;
        T? item = null;

        // Try to get from pool (lock-free for common case)
        var index = Interlocked.Decrement(ref _index);
        if (index >= 0 && index < items.Length)
        {
            item = Interlocked.Exchange(ref items[index], null);
        }
        else
        {
            Interlocked.Increment(ref _index); // Restore
        }

        return item ?? _factory();
    }

    public void Return(T item)
    {
        var items = _items;
        var index = Interlocked.Increment(ref _index) - 1;
        if (index >= 0 && index < items.Length)
        {
            Interlocked.Exchange(ref items[index], item);
        }
        // else: pool is full, let GC collect
    }
}
