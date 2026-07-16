using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Drives the consume endpoint of <see cref="WebSocketProtocolAdapter"/> over a scripted socket:
/// live appends are tailed to the client with increasing offsets, an already-closed socket
/// releases the connection slot immediately, and storage failures surface as internal_error.
/// </summary>
public sealed class WebSocketConsumeLoopTests : IDisposable
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(15);

    private readonly LogManager _logManager = WebSocketAdapterTestHost.CreateInMemoryLogManager();

    public void Dispose() => _logManager.Dispose();

    [Fact]
    public async Task Consume_TailsLiveAppends_ThenStopsWhenSocketCloses()
    {
        const string topic = "ws-consume-tail";
        var adapter = WebSocketAdapterTestHost.CreateAdapter(_logManager);
        var endpoint = WebSocketAdapterTestHost.GetEndpoint(WebSocketAdapterTestHost.MapEndpoints(adapter), "/consume/");
        var socket = new ScriptedWebSocket();
        var context = WebSocketAdapterTestHost.CreateWebSocketContext(socket, topic);

        // The handler runs synchronously up to its first poll delay, so by the time the
        // invocation returns it has captured start offset 0 (topic does not exist yet).
        var handler = endpoint(context);

        var tp = new TopicPartition { Topic = topic, Partition = 0 };
        await _logManager.AppendBatchAsync(tp, RecordBatchBuilder.BuildAsciiRecordBatch("TAIL-MARKER-A"));
        await _logManager.AppendBatchAsync(tp, RecordBatchBuilder.BuildAsciiRecordBatch("TAIL-MARKER-B"));

        await socket.WaitForSentFramesAsync(2).WaitAsync(TestTimeout);
        socket.MarkClosed();
        await handler.WaitAsync(TestTimeout);

        var frames = socket.SentFrames.Select(WebSocketAdapterTestHost.ParseFrame).ToList();
        Assert.All(frames, f => Assert.False(f.TryGetProperty("error", out _)));
        Assert.Equal(2, frames.Count);

        Assert.Equal(topic, frames[0].GetProperty("topic").GetString());
        Assert.Equal(0L, frames[0].GetProperty("offset").GetInt64());
        Assert.Contains("TAIL-MARKER-A", frames[0].GetProperty("value").GetString(), StringComparison.Ordinal);

        Assert.Equal(1L, frames[1].GetProperty("offset").GetInt64());
        Assert.Contains("TAIL-MARKER-B", frames[1].GetProperty("value").GetString(), StringComparison.Ordinal);

        Assert.Equal(0, adapter.ActiveConnections);
    }

    [Fact]
    public async Task Consume_SocketAlreadyClosed_ReleasesConnectionWithoutSends()
    {
        var adapter = WebSocketAdapterTestHost.CreateAdapter(_logManager);
        var endpoint = WebSocketAdapterTestHost.GetEndpoint(WebSocketAdapterTestHost.MapEndpoints(adapter), "/consume/");
        var socket = new ScriptedWebSocket();
        socket.MarkClosed();
        var context = WebSocketAdapterTestHost.CreateWebSocketContext(socket, "ws-consume-closed");

        await endpoint(context).WaitAsync(TestTimeout);

        Assert.Empty(socket.SentFrames);
        Assert.Equal(0, adapter.ActiveConnections);
    }

    [Fact]
    public async Task Consume_StorageFailure_RepliesInternalErrorAndTerminates()
    {
        // A disposed LogManager makes the first read throw, which the consume loop must
        // translate into an internal_error frame before terminating.
        var failingLogManager = WebSocketAdapterTestHost.CreateInMemoryLogManager();
        failingLogManager.Dispose();

        var adapter = WebSocketAdapterTestHost.CreateAdapter(failingLogManager);
        var endpoint = WebSocketAdapterTestHost.GetEndpoint(WebSocketAdapterTestHost.MapEndpoints(adapter), "/consume/");
        var socket = new ScriptedWebSocket();
        var context = WebSocketAdapterTestHost.CreateWebSocketContext(socket, "ws-consume-broken");

        await endpoint(context).WaitAsync(TestTimeout);

        var frame = WebSocketAdapterTestHost.ParseFrame(Assert.Single(socket.SentFrames));
        Assert.Equal("internal_error", frame.GetProperty("error").GetString());
        Assert.Equal(0, adapter.ActiveConnections);
    }
}
