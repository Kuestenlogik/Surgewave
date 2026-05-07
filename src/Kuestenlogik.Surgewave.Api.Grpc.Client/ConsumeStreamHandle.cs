using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Client;

/// <summary>
/// Handle for bidirectional streaming consume operations with flow control.
/// Allows pausing, resuming, seeking, and acknowledging messages.
/// </summary>
public sealed class ConsumeStreamHandle : IAsyncDisposable
{
    private readonly AsyncDuplexStreamingCall<ConsumeStreamControl, ConsumeResponse> _call;
    private bool _completed;

    internal ConsumeStreamHandle(AsyncDuplexStreamingCall<ConsumeStreamControl, ConsumeResponse> call)
    {
        _call = call;
    }

    /// <summary>
    /// Start consuming from a topic partition at the specified offset.
    /// </summary>
    public async Task StartAsync(
        string topic,
        int partition,
        long offset,
        int maxRecords = 100,
        int maxWaitMs = 5000,
        string? consumerGroup = null,
        CancellationToken cancellationToken = default)
    {
        if (_completed)
            throw new InvalidOperationException("Stream has been completed");

        var control = new ConsumeStreamControl
        {
            Type = ConsumeStreamControl.Types.ControlType.Start,
            Topic = topic,
            Partition = partition,
            Offset = offset,
            MaxRecords = maxRecords,
            MaxWaitMs = maxWaitMs,
            ConsumerGroup = consumerGroup ?? string.Empty
        };

        await _call.RequestStream.WriteAsync(control, cancellationToken);
    }

    /// <summary>
    /// Pause message consumption.
    /// </summary>
    public async Task PauseAsync(CancellationToken cancellationToken = default)
    {
        if (_completed)
            throw new InvalidOperationException("Stream has been completed");

        var control = new ConsumeStreamControl
        {
            Type = ConsumeStreamControl.Types.ControlType.Pause
        };

        await _call.RequestStream.WriteAsync(control, cancellationToken);
    }

    /// <summary>
    /// Resume message consumption after pause.
    /// </summary>
    public async Task ResumeAsync(CancellationToken cancellationToken = default)
    {
        if (_completed)
            throw new InvalidOperationException("Stream has been completed");

        var control = new ConsumeStreamControl
        {
            Type = ConsumeStreamControl.Types.ControlType.Resume
        };

        await _call.RequestStream.WriteAsync(control, cancellationToken);
    }

    /// <summary>
    /// Acknowledge processing of a message at the specified offset.
    /// </summary>
    public async Task AckAsync(long offset, CancellationToken cancellationToken = default)
    {
        if (_completed)
            throw new InvalidOperationException("Stream has been completed");

        var control = new ConsumeStreamControl
        {
            Type = ConsumeStreamControl.Types.ControlType.Ack,
            Offset = offset
        };

        await _call.RequestStream.WriteAsync(control, cancellationToken);
    }

    /// <summary>
    /// Seek to a specific offset.
    /// </summary>
    public async Task SeekAsync(long offset, CancellationToken cancellationToken = default)
    {
        if (_completed)
            throw new InvalidOperationException("Stream has been completed");

        var control = new ConsumeStreamControl
        {
            Type = ConsumeStreamControl.Types.ControlType.Seek,
            Offset = offset
        };

        await _call.RequestStream.WriteAsync(control, cancellationToken);
    }

    /// <summary>
    /// Read responses from the stream as they arrive.
    /// </summary>
    public async IAsyncEnumerable<ConsumeResponse> ReadResponsesAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var response in _call.ResponseStream.ReadAllAsync(cancellationToken))
        {
            yield return response;
        }
    }

    /// <summary>
    /// Complete the control stream, signaling no more control messages will be sent.
    /// </summary>
    public async Task CompleteAsync()
    {
        if (_completed)
            return;

        _completed = true;
        await _call.RequestStream.CompleteAsync();
    }

    /// <summary>
    /// Dispose the stream handle.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!_completed)
        {
            try
            {
                await _call.RequestStream.CompleteAsync();
            }
            catch
            {
                // Ignore errors during disposal
            }
        }

        _call.Dispose();
    }
}
