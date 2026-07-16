using System.Buffers.Binary;
using System.Text.Json;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Drives the produce endpoint of <see cref="WebSocketProtocolAdapter"/> end-to-end over a
/// scripted socket: a RecordBatch-shaped value is appended to the log and readable back,
/// value-less messages get an invalid_request reply, malformed JSON gets internal_error,
/// and binary frames are ignored.
/// </summary>
public sealed class WebSocketProduceLoopTests : IDisposable
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(15);

    private readonly LogManager _logManager = WebSocketAdapterTestHost.CreateInMemoryLogManager();

    public void Dispose() => _logManager.Dispose();

    private Task InvokeProduceAsync(ScriptedWebSocket socket, string topic, out WebSocketProtocolAdapter adapter)
    {
        adapter = WebSocketAdapterTestHost.CreateAdapter(_logManager);
        var endpoint = WebSocketAdapterTestHost.GetEndpoint(WebSocketAdapterTestHost.MapEndpoints(adapter), "/produce/");
        var context = WebSocketAdapterTestHost.CreateWebSocketContext(socket, topic);
        return endpoint(context);
    }

    [Fact]
    public async Task Produce_RecordBatchShapedValue_IsAppendedAndReadableFromLog()
    {
        const string topic = "ws-produce-roundtrip";
        var batch = RecordBatchBuilder.BuildAsciiRecordBatch("PRODUCE-ROUNDTRIP-MARKER");
        var produceJson = JsonSerializer.Serialize(new Dictionary<string, string>
        {
            ["value"] = RecordBatchBuilder.ToAsciiTransparentString(batch),
        });

        var socket = new ScriptedWebSocket();
        socket.EnqueueText(produceJson);

        await InvokeProduceAsync(socket, topic, out var adapter).WaitAsync(TestTimeout);

        Assert.Empty(socket.SentFrames); // no error replies
        Assert.Equal(0, adapter.ActiveConnections);

        var stored = await _logManager.ReadBatchesAsync(new TopicPartition { Topic = topic, Partition = 0 }, 0);
        var single = Assert.Single(stored);
        Assert.Equal(batch.Length, single.Length);

        // Storage assigns base offset 0 (bytes 0-7) and rewrites the CRC (bytes 17-20);
        // everything else must round-trip verbatim.
        Assert.Equal(0L, BinaryPrimitives.ReadInt64BigEndian(single.AsSpan(0, 8)));
        Assert.Equal(
            batch.AsSpan(KafkaConstants.RecordBatch.LengthOffset, 9).ToArray(),
            single.AsSpan(KafkaConstants.RecordBatch.LengthOffset, 9).ToArray());
        Assert.Equal(
            batch.AsSpan(KafkaConstants.RecordBatch.AttributesOffset).ToArray(),
            single.AsSpan(KafkaConstants.RecordBatch.AttributesOffset).ToArray());
    }

    [Fact]
    public async Task Produce_MessageWithoutValue_RepliesInvalidRequest()
    {
        const string topic = "ws-produce-no-value";
        var socket = new ScriptedWebSocket();
        socket.EnqueueText("{}");

        await InvokeProduceAsync(socket, topic, out var adapter).WaitAsync(TestTimeout);

        var frame = WebSocketAdapterTestHost.ParseFrame(Assert.Single(socket.SentFrames));
        Assert.Equal("invalid_request", frame.GetProperty("error").GetString());
        Assert.Equal("Message value is required", frame.GetProperty("message").GetString());
        Assert.Null(_logManager.GetLog(new TopicPartition { Topic = topic, Partition = 0 }));
        Assert.Equal(0, adapter.ActiveConnections);
    }

    [Fact]
    public async Task Produce_MalformedJson_RepliesInternalErrorAndKeepsListening()
    {
        const string topic = "ws-produce-malformed";
        var socket = new ScriptedWebSocket();
        socket.EnqueueText("{ not json at all");
        socket.EnqueueText("{}"); // loop continues after the error and processes the next frame

        await InvokeProduceAsync(socket, topic, out _).WaitAsync(TestTimeout);

        var frames = socket.SentFrames.Select(WebSocketAdapterTestHost.ParseFrame).ToList();
        Assert.Equal(2, frames.Count);
        Assert.Equal("internal_error", frames[0].GetProperty("error").GetString());
        Assert.Equal("invalid_request", frames[1].GetProperty("error").GetString());
    }

    [Fact]
    public async Task Produce_BinaryFrames_AreIgnored()
    {
        const string topic = "ws-produce-binary";
        var socket = new ScriptedWebSocket();
        socket.EnqueueBinary([1, 2, 3]);

        await InvokeProduceAsync(socket, topic, out _).WaitAsync(TestTimeout);

        Assert.Empty(socket.SentFrames);
        Assert.Null(_logManager.GetLog(new TopicPartition { Topic = topic, Partition = 0 }));
    }
}
