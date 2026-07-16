using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Drives the multi-topic subscribe endpoint of <see cref="WebSocketProtocolAdapter"/> over a
/// scripted socket: subscribing from earliest streams existing messages with increasing offsets,
/// offset selectors (-1 latest, -2 earliest, explicit) are accepted, unknown actions and null
/// payloads produce error replies.
/// </summary>
public sealed class WebSocketSubscribeLoopTests : IDisposable
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(15);

    private readonly LogManager _logManager = WebSocketAdapterTestHost.CreateInMemoryLogManager();

    public void Dispose() => _logManager.Dispose();

    private Task InvokeSubscribeAsync(ScriptedWebSocket socket, out WebSocketProtocolAdapter adapter)
    {
        adapter = WebSocketAdapterTestHost.CreateAdapter(_logManager);
        var endpoint = WebSocketAdapterTestHost.GetEndpoint(WebSocketAdapterTestHost.MapEndpoints(adapter), "subscribe");
        var context = WebSocketAdapterTestHost.CreateWebSocketContext(socket);
        return endpoint(context);
    }

    [Fact]
    public async Task Subscribe_FromEarliest_StreamsExistingMessagesThenUnsubscribes()
    {
        const string topic = "ws-sub-earliest";
        var tp = new TopicPartition { Topic = topic, Partition = 0 };
        await _logManager.AppendBatchAsync(tp, RecordBatchBuilder.BuildAsciiRecordBatch("SUB-EARLIEST-A"));
        await _logManager.AppendBatchAsync(tp, RecordBatchBuilder.BuildAsciiRecordBatch("SUB-EARLIEST-B"));

        var socket = new ScriptedWebSocket();
        socket.EnqueueText("""{"action":"subscribe","topics":["ws-sub-earliest"],"offsets":{"ws-sub-earliest":-2}}""");
        // Unsubscribe only after both stored messages were pushed to the client.
        socket.EnqueueText(
            """{"action":"unsubscribe","topics":["ws-sub-earliest"]}""",
            gate: socket.WaitForSentFramesAsync(2));

        await InvokeSubscribeAsync(socket, out var adapter).WaitAsync(TestTimeout);

        var frames = socket.SentFrames.Select(WebSocketAdapterTestHost.ParseFrame).ToList();
        Assert.All(frames, f => Assert.False(f.TryGetProperty("error", out _)));
        Assert.Equal(2, frames.Count);

        Assert.Equal(topic, frames[0].GetProperty("topic").GetString());
        Assert.Equal(0L, frames[0].GetProperty("offset").GetInt64());
        Assert.Contains("SUB-EARLIEST-A", frames[0].GetProperty("value").GetString(), StringComparison.Ordinal);

        Assert.Equal(topic, frames[1].GetProperty("topic").GetString());
        Assert.Equal(1L, frames[1].GetProperty("offset").GetInt64());
        Assert.Contains("SUB-EARLIEST-B", frames[1].GetProperty("value").GetString(), StringComparison.Ordinal);

        Assert.Equal(0, adapter.ActiveConnections);
    }

    [Fact]
    public async Task Subscribe_LatestDefaultAndExplicitOffsets_AreAcceptedWithoutBackfill()
    {
        // Topic "a" has one stored message; -1 must start at the high watermark (no backfill).
        var tp = new TopicPartition { Topic = "ws-latest-a", Partition = 0 };
        await _logManager.AppendBatchAsync(tp, RecordBatchBuilder.BuildAsciiRecordBatch("LATEST-A"));

        var socket = new ScriptedWebSocket();
        socket.EnqueueText(
            """{"action":"subscribe","topics":["ws-latest-a","ws-latest-b","ws-latest-c"],"offsets":{"ws-latest-a":-1,"ws-latest-c":5}}""");

        await InvokeSubscribeAsync(socket, out var adapter).WaitAsync(TestTimeout);

        Assert.Empty(socket.SentFrames);
        Assert.Equal(0, adapter.ActiveConnections);
    }

    [Fact]
    public async Task Subscribe_UnknownAction_RepliesInvalidRequest()
    {
        var socket = new ScriptedWebSocket();
        socket.EnqueueText("""{"action":"pause","topics":["orders"]}""");

        await InvokeSubscribeAsync(socket, out _).WaitAsync(TestTimeout);

        var frame = WebSocketAdapterTestHost.ParseFrame(Assert.Single(socket.SentFrames));
        Assert.Equal("invalid_request", frame.GetProperty("error").GetString());
        Assert.Contains("Unknown action", frame.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Subscribe_NullPayload_RepliesInvalidFormat()
    {
        var socket = new ScriptedWebSocket();
        socket.EnqueueText("null");

        await InvokeSubscribeAsync(socket, out _).WaitAsync(TestTimeout);

        var frame = WebSocketAdapterTestHost.ParseFrame(Assert.Single(socket.SentFrames));
        Assert.Equal("invalid_request", frame.GetProperty("error").GetString());
        Assert.Equal("Invalid subscribe message format", frame.GetProperty("message").GetString());
    }
}
