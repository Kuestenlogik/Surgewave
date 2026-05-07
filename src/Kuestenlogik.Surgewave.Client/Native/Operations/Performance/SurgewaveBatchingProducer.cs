using System.Buffers;
using System.Threading.Channels;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Performance;

/// <summary>
/// Auto-batching producer that uses Channels for high-throughput message producing.
/// Optimized with pipelining, pooling, and lock-free operations.
/// </summary>
public sealed class SurgewaveBatchingProducer : IAsyncDisposable
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _topic;
    private readonly int _partition;
    private readonly int _maxBatchSize;
    private readonly int _maxBatchBytes;
    private readonly long _lingerTicks;
    private readonly int _maxInFlight;
    private readonly TimeProvider _timeProvider;
    private readonly Channel<ProduceRequest> _channel;
    private readonly Task _batcherTask;
    private readonly CancellationTokenSource _cts = new();

    // Pooled batch lists to avoid allocations
    private readonly Channel<List<ProduceRequest>> _batchPool;

    // In-flight batch tracking for pipelining
    private readonly SemaphoreSlim _inFlightSemaphore;

    public SurgewaveBatchingProducer(
        SurgewaveNativeClient client,
        string topic,
        int partition,
        int maxBatchSize = 1000,
        int maxBatchBytes = 1024 * 1024,
        TimeSpan? lingerTime = null,
        int maxInFlight = 5,
        TimeProvider? timeProvider = null)
    {
        _client = client;
        _topic = topic;
        _partition = partition;
        _maxBatchSize = maxBatchSize;
        _maxBatchBytes = maxBatchBytes;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _lingerTicks = (long)((lingerTime ?? TimeSpan.FromMilliseconds(5)).TotalSeconds * _timeProvider.TimestampFrequency);
        _maxInFlight = maxInFlight;

        _channel = Channel.CreateBounded<ProduceRequest>(new BoundedChannelOptions(maxBatchSize * 10)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

        // Pre-allocate batch list pool
        _batchPool = Channel.CreateBounded<List<ProduceRequest>>(maxInFlight * 2);
        for (int i = 0; i < maxInFlight * 2; i++)
        {
            _batchPool.Writer.TryWrite(new List<ProduceRequest>(maxBatchSize));
        }

        _inFlightSemaphore = new SemaphoreSlim(maxInFlight, maxInFlight);

        _batcherTask = Task.Run(BatcherLoopAsync);
    }

    /// <summary>
    /// Produce a message with automatic batching. Returns immediately after queueing.
    /// </summary>
    public ValueTask ProduceAsync(byte[]? key, byte[] value, CancellationToken cancellationToken = default)
    {
        var request = new ProduceRequest(key, value, null);
        return _channel.Writer.WriteAsync(request, cancellationToken);
    }

    /// <summary>
    /// Produce a message and wait for acknowledgment.
    /// </summary>
    public async Task<long> ProduceAndWaitAsync(byte[]? key, byte[] value, CancellationToken cancellationToken = default)
    {
        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = new ProduceRequest(key, value, tcs);
        await _channel.Writer.WriteAsync(request, cancellationToken);
        return await tcs.Task;
    }

    /// <summary>
    /// Flush all pending messages.
    /// </summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var flushRequest = new ProduceRequest(null, [], tcs, true);
        await _channel.Writer.WriteAsync(flushRequest, cancellationToken);
        await tcs.Task;
    }

    private async Task BatcherLoopAsync()
    {
        // Get first batch from pool
        var batch = await GetBatchFromPoolAsync();
        var batchBytes = 0;
        var lingerDeadline = long.MaxValue;

        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                // Try to read without blocking first (hot path)
                while (_channel.Reader.TryRead(out var request))
                {
                    if (request.IsFlush)
                    {
                        if (batch.Count > 0)
                        {
                            await SendBatchPipelinedAsync(batch);
                            batch = await GetBatchFromPoolAsync();
                            batchBytes = 0;
                            lingerDeadline = long.MaxValue;
                        }
                        request.Completion?.TrySetResult(0);
                        continue;
                    }

                    if (batch.Count == 0)
                    {
                        // Start linger timer on first message
                        lingerDeadline = _timeProvider.GetTimestamp() + _lingerTicks;
                    }

                    batch.Add(request);
                    batchBytes += (request.Key?.Length ?? 0) + request.Value.Length;

                    // Check batch limits
                    if (batch.Count >= _maxBatchSize || batchBytes >= _maxBatchBytes)
                    {
                        await SendBatchPipelinedAsync(batch);
                        batch = await GetBatchFromPoolAsync();
                        batchBytes = 0;
                        lingerDeadline = long.MaxValue;
                    }
                }

                // Check linger timeout
                if (batch.Count > 0)
                {
                    var now = _timeProvider.GetTimestamp();
                    if (now >= lingerDeadline)
                    {
                        await SendBatchPipelinedAsync(batch);
                        batch = await GetBatchFromPoolAsync();
                        batchBytes = 0;
                        lingerDeadline = long.MaxValue;
                        continue;
                    }

                    // Wait with remaining linger time
                    var remainingTicks = lingerDeadline - now;
                    var remainingMs = (int)(remainingTicks * 1000 / _timeProvider.TimestampFrequency);
                    if (remainingMs > 0)
                    {
                        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
                        cts.CancelAfter(remainingMs);
                        try
                        {
                            await _channel.Reader.WaitToReadAsync(cts.Token);
                        }
                        catch (OperationCanceledException) when (!_cts.Token.IsCancellationRequested)
                        {
                            // Linger timeout
                        }
                    }
                }
                else
                {
                    // No messages, wait indefinitely
                    if (!await _channel.Reader.WaitToReadAsync(_cts.Token))
                        break;
                }
            }
            catch (OperationCanceledException) when (_cts.Token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                foreach (var req in batch)
                {
                    req.Completion?.TrySetException(ex);
                }
                batch.Clear();
                batchBytes = 0;
                lingerDeadline = long.MaxValue;
            }
        }

        // Send remaining batch
        if (batch.Count > 0)
        {
            try
            {
                await SendBatchDirectAsync(batch);
            }
            catch (Exception ex)
            {
                foreach (var req in batch)
                {
                    req.Completion?.TrySetException(ex);
                }
            }
        }

        // Drain remaining messages on shutdown
        while (_channel.Reader.TryRead(out var remaining))
        {
            remaining.Completion?.TrySetCanceled();
        }
    }

    private async ValueTask<List<ProduceRequest>> GetBatchFromPoolAsync()
    {
        if (_batchPool.Reader.TryRead(out var batch))
        {
            batch.Clear();
            return batch;
        }
        // Pool exhausted, create new (shouldn't happen often with pipelining)
        return new List<ProduceRequest>(_maxBatchSize);
    }

    private void ReturnBatchToPool(List<ProduceRequest> batch)
    {
        batch.Clear();
        _batchPool.Writer.TryWrite(batch);
    }

    private async Task SendBatchPipelinedAsync(List<ProduceRequest> batch)
    {
        // Wait for in-flight slot
        await _inFlightSemaphore.WaitAsync(_cts.Token);

        // Fire and forget - batch is sent asynchronously
        _ = SendBatchWithCompletionAsync(batch);
    }

    private async Task SendBatchWithCompletionAsync(List<ProduceRequest> batch)
    {
        try
        {
            await SendBatchDirectAsync(batch);
        }
        finally
        {
            ReturnBatchToPool(batch);
            _inFlightSemaphore.Release();
        }
    }

    private async Task SendBatchDirectAsync(List<ProduceRequest> batch)
    {
        // Rent array from pool to avoid per-batch allocation
        var messages = ArrayPool<(byte[]? Key, byte[] Value)>.Shared.Rent(batch.Count);
        try
        {
            // Build message array using rented buffer
            for (int i = 0; i < batch.Count; i++)
            {
                messages[i] = (batch[i].Key, batch[i].Value);
            }

            // Use ArraySegment to pass only the valid portion (IReadOnlyList<T>)
            var baseOffset = await _client.Messaging.SendBatchAsync(_topic, _partition,
                new ArraySegment<(byte[]?, byte[])>(messages, 0, batch.Count));

            // Signal completion to all requests
            for (int i = 0; i < batch.Count; i++)
            {
                batch[i].Completion?.TrySetResult(baseOffset + i);
            }
        }
        catch (Exception ex)
        {
            foreach (var req in batch)
            {
                req.Completion?.TrySetException(ex);
            }
            throw;
        }
        finally
        {
            // Return array to pool
            ArrayPool<(byte[]? Key, byte[] Value)>.Shared.Return(messages, clearArray: true);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.Complete();
        await _cts.CancelAsync();
        try
        {
            await _batcherTask;
        }
        catch (OperationCanceledException) { }

        // Wait for all in-flight batches
        for (int i = 0; i < _maxInFlight; i++)
        {
            await _inFlightSemaphore.WaitAsync();
        }

        _inFlightSemaphore.Dispose();
        _cts.Dispose();
    }

    private readonly record struct ProduceRequest(
        byte[]? Key,
        byte[] Value,
        TaskCompletionSource<long>? Completion,
        bool IsFlush = false);
}
