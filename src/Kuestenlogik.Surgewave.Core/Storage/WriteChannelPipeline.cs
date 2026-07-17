using System.Threading.Channels;
using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Pipeline;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Channel-based write pipeline that batches writes per partition for optimal throughput.
/// </summary>
internal sealed class WriteChannelPipeline : IDisposable
{
    private readonly Channel<WriteRequest> _writeChannel;
    private readonly Task[] _writerTasks;
    private readonly CancellationTokenSource _shutdownCts;
    private readonly Func<TopicPartition, IPartitionLog> _getOrCreateLog;
    private readonly int _writeBatchSize;

    public WriteChannelPipeline(
        int workerCount,
        int channelCapacity,
        int batchSize,
        Func<TopicPartition, IPartitionLog> getOrCreateLog,
        CancellationTokenSource shutdownCts)
    {
        _writeBatchSize = batchSize;
        _getOrCreateLog = getOrCreateLog;
        _shutdownCts = shutdownCts;

        // Create bounded write channel with backpressure.
        // AllowSynchronousContinuations: when a writer publishes a request and a reader
        // is already waiting, the reader's continuation runs synchronously on the writer's
        // thread — saving a ThreadPool hop (~1-5µs per write). Safe because the reader
        // continuation (AppendBatchAsync) is non-blocking.
        _writeChannel = Channel.CreateBounded<WriteRequest>(new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
            AllowSynchronousContinuations = true
        });

        // Start write workers
        _writerTasks = Enumerable.Range(0, workerCount)
            .Select(_ => Task.Run(() => WriteWorkerAsync(_shutdownCts.Token)))
            .ToArray();
    }

    public ChannelWriter<WriteRequest> Writer => _writeChannel.Writer;

    /// <summary>
    /// Write worker that batches writes per partition for optimal throughput.
    /// Accumulates requests until batch is full or channel is empty (for low latency when idle).
    /// </summary>
    private async Task WriteWorkerAsync(CancellationToken cancellationToken)
    {
        // Reuse collections to avoid allocations
        var batch = new Dictionary<TopicPartition, List<WriteRequest>>(16);
        var partitionListPool = new List<List<WriteRequest>>(16);
        var totalInBatch = 0;

        await foreach (var request in _writeChannel.Reader.ReadAllAsync(cancellationToken))
        {
            try
            {
                // Add to batch - reuse list from pool if available
                if (!batch.TryGetValue(request.TopicPartition, out var partitionBatch))
                {
                    partitionBatch = partitionListPool.Count > 0
                        ? partitionListPool[partitionListPool.Count - 1]
                        : new List<WriteRequest>(32);
                    if (partitionListPool.Count > 0)
                        partitionListPool.RemoveAt(partitionListPool.Count - 1);
                    batch[request.TopicPartition] = partitionBatch;
                }

                partitionBatch.Add(request);
                totalInBatch++;

                // Keep batching: read more items without blocking until batch is full
                while (totalInBatch < _writeBatchSize && _writeChannel.Reader.TryRead(out var nextRequest))
                {
                    if (!batch.TryGetValue(nextRequest.TopicPartition, out var nextBatch))
                    {
                        nextBatch = partitionListPool.Count > 0
                            ? partitionListPool[partitionListPool.Count - 1]
                            : new List<WriteRequest>(32);
                        if (partitionListPool.Count > 0)
                            partitionListPool.RemoveAt(partitionListPool.Count - 1);
                        batch[nextRequest.TopicPartition] = nextBatch;
                    }
                    nextBatch.Add(nextRequest);
                    totalInBatch++;
                }

                // Flush when: (1) batch is full, OR (2) no more items immediately available
                if (batch.Count > 0)
                {
                    await FlushBatchAsync(batch, cancellationToken);

                    // Return lists to pool for reuse
                    foreach (var list in batch.Values)
                    {
                        list.Clear();
                        if (partitionListPool.Count < 32) // Cap pool size
                            partitionListPool.Add(list);
                    }
                    batch.Clear();
                    totalInBatch = 0;
                }
            }
            catch (Exception ex)
            {
                request.CompletionSource.TrySetException(ex);
            }
        }

        // Flush remaining
        if (batch.Count > 0)
        {
            await FlushBatchAsync(batch, cancellationToken);
        }
    }

    private async Task FlushBatchAsync(
        Dictionary<TopicPartition, List<WriteRequest>> batch,
        CancellationToken cancellationToken)
    {
        // Single-partition batch (the dominant case): flush inline, no Task[] and no fan-out.
        if (batch.Count == 1)
        {
            foreach (var (topicPartition, requests) in batch)
            {
                await FlushPartitionAsync(topicPartition, requests, cancellationToken);
            }

            return;
        }

        // Process partitions in parallel - each partition has its own semaphore lock.
        // A named method instead of a LINQ async lambda: that allocated an iterator, a delegate
        // and a display class capturing this/cancellationToken on every flush (#83).
        var tasks = new Task[batch.Count];
        var i = 0;
        foreach (var (topicPartition, requests) in batch)
        {
            tasks[i++] = FlushPartitionAsync(topicPartition, requests, cancellationToken);
        }

        await Task.WhenAll(tasks);
    }

    private async Task FlushPartitionAsync(
        TopicPartition topicPartition,
        List<WriteRequest> requests,
        CancellationToken cancellationToken)
    {
        try
        {
            var log = _getOrCreateLog(topicPartition);

            // Write each RecordBatch separately within this partition
            foreach (var request in requests)
            {
                try
                {
                    var offset = await log.AppendBatchAsync(
                        request.RecordBatch,
                        request.RecordBatchOffset,
                        request.RecordBatchLength,
                        request.CrcMode,
                        cancellationToken);
                    request.CompletionSource.TrySetResult(offset);
                }
                catch (DataCorruptionException dex)
                {
                    // Only this producer's batch is bad. Faulting the whole list would reject the
                    // valid batches queued behind it on the same partition (#85).
                    request.CompletionSource.TrySetException(dex);
                }
            }
        }
        catch (Exception ex)
        {
            // Fatal for the partition (IO, disposed, cancellation) — everything still pending fails.
            foreach (var request in requests)
            {
                request.CompletionSource.TrySetException(ex);
            }
        }
    }

    public void WaitForCompletion(TimeSpan timeout)
    {
        try
        {
            Task.WaitAll(_writerTasks, timeout);
        }
        catch
        {
            // Ignore timeout
        }
    }

    public void Dispose()
    {
        _writeChannel.Writer.Complete();
        WaitForCompletion(TimeSpan.FromSeconds(30));
    }
}
