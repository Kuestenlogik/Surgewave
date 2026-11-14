using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Streaming;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Streaming;
using Kuestenlogik.Surgewave.Transport;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests.Components;

/// <summary>
/// A streaming consumer has what the broker pushes from the moment the broker has its subscription (#289).
/// </summary>
/// <remarks>
/// The consumer took the frames the broker pushes only once it had read the answer to its Subscribe. The broker
/// starts pushing when it has taken the subscription, which is before that, and a frame that came first was
/// dropped: its records were never read. The broker here pushes one frame while the Subscribe is still unanswered.
/// </remarks>
public sealed class StreamingConsumerSubscribeTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task ARecordPushedBeforeTheSubscribeIsAnsweredIsRead()
    {
        var ct = TestContext.Current.CancellationToken;
        var broker = new PushingBeforeItAnswers();
        await using var client = new SurgewaveNativeClient(broker);
        await client.ConnectAsync(ct);

        await using var consumer = await SurgewaveStreamingConsumer.SubscribeAsync(client, "orders", [0], cancellationToken: ct);

        await using var records = consumer.Records.GetAsyncEnumerator(ct);
        Assert.True(await records.MoveNextAsync().AsTask().WaitAsync(Bound, ct));
        Assert.Equal(7, records.Current.Offset);
        Assert.True(broker.PushedBeforeItAnswered);
    }

    [Fact]
    public async Task ASubscribeTheBrokerRefusesLeavesNoHandlerBehind()
    {
        var ct = TestContext.Current.CancellationToken;
        var broker = new PushingBeforeItAnswers { Refuses = true };
        await using var client = new SurgewaveNativeClient(broker);
        await client.ConnectAsync(ct);

        await Assert.ThrowsAnyAsync<Exception>(() => SurgewaveStreamingConsumer.SubscribeAsync(client, "orders", [0], cancellationToken: ct));

        Assert.False(broker.HasPushHandler);
    }

    /// <summary>
    /// A broker that pushes a record for a subscription as soon as it has read the Subscribe, and answers it after.
    /// </summary>
    private sealed class PushingBeforeItAnswers : ISurgewaveTransport
    {
        private readonly TaskCompletionSource<Exception> _ended = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private Func<SurgewaveResponseHeader, ReadOnlyMemory<byte>, Task>? _pushHandler;

        public bool Refuses { get; init; }
        public bool PushedBeforeItAnswered { get; private set; }
        public bool HasPushHandler => Volatile.Read(ref _pushHandler) is not null;

        public SurgewaveTransportType TransportType => SurgewaveTransportType.Tcp;
        public bool IsConnected { get; private set; }
        public bool ServerSupportsCompression => false;
        public Task<Exception> ConnectionEnded => _ended.Task;

        public ValueTask ConnectAsync(CancellationToken cancellationToken = default)
        {
            IsConnected = true;
            return ValueTask.CompletedTask;
        }

        public async ValueTask<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> SendRequestAsync(
            SurgewaveOpCode opCode, ReadOnlyMemory<byte> payload, bool compress = true, CancellationToken cancellationToken = default)
        {
            if (opCode == SurgewaveOpCode.Subscribe && Refuses)
                return (new SurgewaveResponseHeader { OpCode = SurgewaveOpCode.Error, ErrorCode = SurgewaveErrorCode.TopicNotFound }, ReadOnlyMemory<byte>.Empty);

            if (opCode == SurgewaveOpCode.Subscribe)
            {
                var subscription = ReadSubscriptionId(payload);
                if (Volatile.Read(ref _pushHandler) is { } handler)
                {
                    PushedBeforeItAnswered = true;
                    await handler(new SurgewaveResponseHeader { OpCode = SurgewaveOpCode.FetchResponse }, Record(subscription));
                }
            }

            return (new SurgewaveResponseHeader { OpCode = opCode, ErrorCode = SurgewaveErrorCode.None }, ReadOnlyMemory<byte>.Empty);
        }

        private static string ReadSubscriptionId(ReadOnlyMemory<byte> payload)
        {
            var reader = new SurgewavePayloadReader(payload.Span);
            return SubscribePayload.Read(ref reader).SubscriptionId;
        }

        private static byte[] Record(string subscription)
        {
            var pushed = new StreamRecordPayload
            {
                SubscriptionId = subscription,
                Partition = 0,
                HighWatermark = 8,
                Messages = [new StreamMessage(Offset: 7, Timestamp: 1, Key: null, Value: [1, 2, 3])],
            };
            var buffer = new byte[pushed.EstimateSize() + 32];
            var writer = new SurgewavePayloadWriter(buffer);
            pushed.Write(ref writer);
            return buffer[..writer.Position];
        }

        public void RegisterPushHandler(SurgewaveOpCode opCode, Func<SurgewaveResponseHeader, ReadOnlyMemory<byte>, Task> handler)
            => Volatile.Write(ref _pushHandler, handler);

        public void UnregisterPushHandler(SurgewaveOpCode opCode) => Volatile.Write(ref _pushHandler, null);

        public ValueTask DisposeAsync()
        {
            IsConnected = false;
            return ValueTask.CompletedTask;
        }
    }
}
