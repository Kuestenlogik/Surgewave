using System.Threading.Channels;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Core.Pipeline;

/// <summary>
/// Single consumer session with read-ahead channel
/// </summary>
public sealed class ConsumerSession : IDisposable
{
    private readonly string _consumerId;
    private readonly TopicPartition _topicPartition;
    private readonly int _readAheadCount;
    private readonly LogManager _logManager;
    private readonly Channel<Message> _messageChannel;
    private readonly Task _readAheadTask;
    private readonly CancellationTokenSource _sessionCts;
    private long _currentOffset;

    public ConsumerSession(
        string consumerId,
        TopicPartition topicPartition,
        long startOffset,
        int readAheadCount,
        LogManager logManager,
        CancellationToken shutdownToken)
    {
        _consumerId = consumerId;
        _topicPartition = topicPartition;
        _currentOffset = startOffset;
        _readAheadCount = readAheadCount;
        _logManager = logManager;

        _sessionCts = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);

        // Unbounded channel for read-ahead messages
        _messageChannel = Channel.CreateUnbounded<Message>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        // Start read-ahead task
        _readAheadTask = Task.Run(() => ReadAheadAsync(_sessionCts.Token));
    }

    /// <summary>
    /// Get next batch of messages (from cache)
    /// </summary>
    public async ValueTask<List<Message>> GetNextBatchAsync(
        int maxMessages,
        CancellationToken cancellationToken = default)
    {
        var messages = new List<Message>(maxMessages);

        for (int i = 0; i < maxMessages; i++)
        {
            if (await _messageChannel.Reader.WaitToReadAsync(cancellationToken))
            {
                if (_messageChannel.Reader.TryRead(out var message))
                {
                    messages.Add(message);
                    _currentOffset = message.Offset + 1;
                }
            }
            else
            {
                break;
            }
        }

        return messages;
    }

    /// <summary>
    /// Seek to a specific offset (clears cache)
    /// </summary>
    public void Seek(long offset)
    {
        _currentOffset = offset;

        // Drain existing messages
        while (_messageChannel.Reader.TryRead(out _))
        {
            // Discard
        }
    }

    private Task ReadAheadAsync(CancellationToken cancellationToken)
    {
        // Read-ahead not currently implemented - channel completes immediately
        // Future: integrate with batch-based storage API (ReadBatchesAsync)
        _messageChannel.Writer.Complete();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _sessionCts.Cancel();
        _messageChannel.Writer.Complete();

        try
        {
            _readAheadTask.Wait(TimeSpan.FromSeconds(5));
        }
        catch
        {
            // Ignore
        }

        _sessionCts.Dispose();
    }
}
