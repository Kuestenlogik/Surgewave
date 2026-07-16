using System.Net.WebSockets;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Scripted in-memory <see cref="System.Net.WebSockets.WebSocket"/> test double:
/// <see cref="ReceiveAsync"/> replays a fixed sequence of frames (each optionally gated on an
/// awaitable so tests can sequence receives after observed sends), and <see cref="SendAsync"/>
/// records every outgoing frame. Once the script is exhausted, ReceiveAsync reports a close frame.
/// </summary>
internal sealed class ScriptedWebSocket : System.Net.WebSockets.WebSocket
{
    private readonly Queue<ScriptedFrame> _frames = new();
    private readonly List<byte[]> _sent = [];
    private readonly List<(int Threshold, TaskCompletionSource Completion)> _sendWaiters = [];
    private readonly Lock _gate = new();
    private volatile WebSocketState _state = WebSocketState.Open;

    public override WebSocketCloseStatus? CloseStatus => null;

    public override string? CloseStatusDescription => null;

    public override string? SubProtocol => null;

    public override WebSocketState State => _state;

    /// <summary>Queues a text frame containing the given JSON payload.</summary>
    public void EnqueueText(string json, Task? gate = null)
        => _frames.Enqueue(new ScriptedFrame(Encoding.UTF8.GetBytes(json), WebSocketMessageType.Text, gate));

    /// <summary>Queues a binary frame with the given payload.</summary>
    public void EnqueueBinary(byte[] payload, Task? gate = null)
        => _frames.Enqueue(new ScriptedFrame(payload, WebSocketMessageType.Binary, gate));

    /// <summary>Flips the socket into the Closed state so state-polling loops terminate.</summary>
    public void MarkClosed() => _state = WebSocketState.Closed;

    /// <summary>Snapshot of all frames recorded from SendAsync, in send order.</summary>
    public IReadOnlyList<byte[]> SentFrames
    {
        get
        {
            lock (_gate)
            {
                return _sent.ToArray();
            }
        }
    }

    /// <summary>Returns a task that completes once at least <paramref name="count"/> frames were sent.</summary>
    public Task WaitForSentFramesAsync(int count)
    {
        lock (_gate)
        {
            if (_sent.Count >= count)
            {
                return Task.CompletedTask;
            }

            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _sendWaiters.Add((count, completion));
            return completion.Task;
        }
    }

    public override async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
    {
        if (_frames.Count == 0)
        {
            _state = WebSocketState.CloseReceived;
            return new WebSocketReceiveResult(0, WebSocketMessageType.Close, endOfMessage: true);
        }

        var frame = _frames.Dequeue();
        if (frame.Gate is not null)
        {
            await frame.Gate.WaitAsync(cancellationToken);
        }

        frame.Payload.CopyTo(buffer.Array!, buffer.Offset);
        return new WebSocketReceiveResult(frame.Payload.Length, frame.Type, endOfMessage: true);
    }

    public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _sent.Add(buffer.AsSpan().ToArray());
            for (var i = _sendWaiters.Count - 1; i >= 0; i--)
            {
                if (_sent.Count >= _sendWaiters[i].Threshold)
                {
                    _sendWaiters[i].Completion.TrySetResult();
                    _sendWaiters.RemoveAt(i);
                }
            }
        }

        return Task.CompletedTask;
    }

    public override void Abort() => _state = WebSocketState.Aborted;

    public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
    {
        _state = WebSocketState.Closed;
        return Task.CompletedTask;
    }

    public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
        => Task.CompletedTask;

    public override void Dispose()
    {
        // Nothing to release: purely in-memory.
    }

    private sealed record ScriptedFrame(byte[] Payload, WebSocketMessageType Type, Task? Gate);
}
