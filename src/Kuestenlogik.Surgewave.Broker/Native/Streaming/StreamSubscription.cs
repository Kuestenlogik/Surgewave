using System.Buffers;
using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Native.Streaming;

/// <summary>
/// Represents one active push subscription for a topic across one or more partitions.
/// Manages per-partition background push loops that read log data and send it to the client.
/// </summary>
public sealed class StreamSubscription : IAsyncDisposable
{
    private readonly string _subscriptionId;
    private readonly string _topic;
    private readonly int[] _partitions;
    private readonly long[] _currentOffsets;
    private readonly int _maxBytesPerPush;
    private readonly LogManager _logManager;
    private readonly RecordBatchSerializer _recordBatchSerializer;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new();

    // Credit-based flow control: how many bytes the client has credit for
    private long _creditBytes;
    private readonly SemaphoreSlim _creditSignal = new(0, int.MaxValue);

    private Task[]? _pushTasks;

    /// <summary>Gets the subscription ID.</summary>
    public string SubscriptionId => _subscriptionId;

    /// <summary>Gets the topic being subscribed to.</summary>
    public string Topic => _topic;

    /// <summary>Gets whether the subscription is still active.</summary>
    public bool IsActive => !_cts.IsCancellationRequested;

    public StreamSubscription(
        string subscriptionId,
        string topic,
        int[] partitions,
        long[] initialOffsets,
        int maxBytesPerPush,
        LogManager logManager,
        RecordBatchSerializer recordBatchSerializer,
        ILogger logger)
    {
        _subscriptionId = subscriptionId;
        _topic = topic;
        _partitions = partitions;
        _currentOffsets = (long[])initialOffsets.Clone();
        _maxBytesPerPush = maxBytesPerPush > 0 ? maxBytesPerPush : 1024 * 1024;
        _logManager = logManager;
        _recordBatchSerializer = recordBatchSerializer;
        _logger = logger;

        // Start with an initial credit of maxBytesPerPush so the first push doesn't block
        _creditBytes = _maxBytesPerPush;
    }

    /// <summary>
    /// Starts background push loops for each partition.
    /// </summary>
    /// <param name="sendDelegate">
    /// Delegate invoked to send a push frame to the client.
    /// Receives: subscriptionId, partition, highWatermark, messageCount, payload bytes.
    /// </param>
    public void StartAsync(Func<string, int, long, int, ReadOnlyMemory<byte>, CancellationToken, Task> sendDelegate)
    {
        _pushTasks = new Task[_partitions.Length];
        for (var i = 0; i < _partitions.Length; i++)
        {
            var partitionIndex = i; // capture for closure
            _pushTasks[i] = RunPushLoopAsync(_partitions[partitionIndex], partitionIndex, sendDelegate, _cts.Token);
        }
    }

    /// <summary>
    /// Adds credit bytes for flow control.  Called when the client sends a StreamAck.
    /// </summary>
    public void AddCredit(long bytes)
    {
        if (bytes <= 0) return;
        Interlocked.Add(ref _creditBytes, bytes);
        _creditSignal.Release();
    }

    /// <summary>
    /// Stops all push loops and waits for them to complete.
    /// </summary>
    public async ValueTask StopAsync()
    {
        await _cts.CancelAsync();

        // Release credit signal so push loops waiting on it can exit
        _creditSignal.Release(Math.Max(1, _partitions.Length));

        if (_pushTasks != null)
        {
            try
            {
                await Task.WhenAll(_pushTasks);
            }
            catch (OperationCanceledException)
            {
                // Expected on cancellation
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Push loop for subscription {SubscriptionId} exited with error", _subscriptionId);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _cts.Dispose();
        _creditSignal.Dispose();
    }

    private async Task RunPushLoopAsync(
        int partition,
        int partitionIndex,
        Func<string, int, long, int, ReadOnlyMemory<byte>, CancellationToken, Task> sendDelegate,
        CancellationToken cancellationToken)
    {
        var topicPartition = new TopicPartition { Topic = _topic, Partition = partition };

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Wait for credit before pushing more data
                if (Interlocked.Read(ref _creditBytes) <= 0)
                {
                    await _creditSignal.WaitAsync(cancellationToken);
                    if (cancellationToken.IsCancellationRequested) break;
                }

                var currentOffset = Volatile.Read(ref _currentOffsets[partitionIndex]);
                var log = _logManager.GetOrCreateLog(topicPartition);

                var (data, batchOffsets) = await _logManager.ReadBatchesContiguousAsync(
                    topicPartition, currentOffset, _maxBytesPerPush, cancellationToken);

                if (data.Length == 0)
                {
                    // No data available — long-poll until data arrives
                    await log.WaitForDataAsync(currentOffset, TimeSpan.FromSeconds(1), cancellationToken);
                    continue;
                }

                // Deduct credit
                Interlocked.Add(ref _creditBytes, -data.Length);

                // Pre-size like the fetch path: the native re-framing replaces varints with fixed
                // fields, so small records make the output exceed the input (#83).
                var dataSpan = data.Span;
                var estimatedRecords = 0;
                for (var i = 0; i < batchOffsets.Count; i++)
                {
                    var start = batchOffsets[i];
                    var end = i + 1 < batchOffsets.Count ? batchOffsets[i + 1] : data.Length;
                    estimatedRecords += RecordBatchStreamer.PeekRecordCount(dataSpan.Slice(start, end - start));
                }

                // Deserialize and stream to client
                using var writer = BigEndianWriter.Rent(12 + data.Length + estimatedRecords * 24);
                writer.Write(log.HighWatermark);
                var countPos = writer.Length;
                writer.Write(0); // message count placeholder

                var totalMessages = 0;
                for (var i = 0; i < batchOffsets.Count; i++)
                {
                    var batchStart = batchOffsets[i];
                    var batchEnd = i + 1 < batchOffsets.Count ? batchOffsets[i + 1] : data.Length;
                    var batchSpan = dataSpan.Slice(batchStart, batchEnd - batchStart);

                    try
                    {
                        totalMessages += RecordBatchStreamer.StreamBatchRawToWriter(batchSpan, writer);
                    }
                    catch (Exception ex)
                    {
                        if (_logger.IsEnabled(LogLevel.Warning))
                            _logger.LogWarning(ex, "Failed to parse record batch in push loop for {Topic}/{Partition}", _topic, partition);
                    }
                }

                writer.PatchInt32(countPos, totalMessages);

                // Advance offset: last batch offset + messages in it
                if (batchOffsets.Count > 0 && totalMessages > 0)
                {
                    // Advance by the number of messages sent
                    Interlocked.Add(ref _currentOffsets[partitionIndex], totalMessages);
                }

                await sendDelegate(_subscriptionId, partition, log.HighWatermark, totalMessages, writer.AsMemory(), cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Push loop error for subscription {SubscriptionId} partition {Partition}", _subscriptionId, partition);
        }
    }
}
