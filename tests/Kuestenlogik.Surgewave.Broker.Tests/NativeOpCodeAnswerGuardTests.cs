using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Streaming;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Transport;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// The guard behind #281: every op code of the native protocol, sent to a running broker with a payload its
/// handler cannot read, is answered, and the connection takes the next request.
/// </summary>
/// <remarks>
/// <para>
/// A payload that is too short or says nonsense is the one unexpected exception every handler can be brought to
/// from the outside: the handlers read their requests with spans, and a read past the end throws. Before #281
/// such a request was not answered at all for most op codes, and the client waited for as long as the connection
/// stood. An op code added later is in the enumeration and so in this test, whichever handler takes it and
/// however that handler is written.
/// </para>
/// <para>
/// The broker is the embedded runtime with the handlers it registers by itself; op codes it has no handler for
/// are answered as unknown, which is an answer too.
/// </para>
/// </remarks>
public sealed class NativeOpCodeAnswerGuardTests(NativeOpCodeAnswerGuardTests.RunningBroker broker)
    : IClassFixture<NativeOpCodeAnswerGuardTests.RunningBroker>
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    public static TheoryData<SurgewaveOpCode> EveryOpCode => [.. Enum.GetValues<SurgewaveOpCode>().Distinct()];

    /// <summary>Requests no handler can read as it expects: nothing, lengths that are negative, and too little.</summary>
    private static readonly byte[][] UnreadablePayloads =
    [
        [],
        [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        [0x00, 0x7F],
        [0x00, 0x01, (byte)'t'],
    ];

    [Theory]
    [MemberData(nameof(EveryOpCode))]
    public async Task ARequestItsHandlerCannotReadIsAnsweredAndTheConnectionGoesOn(SurgewaveOpCode opCode)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var client = new TcpTransport(
            new TransportOptions { Host = "127.0.0.1", Port = broker.Port, EnableCompression = false });
        await client.ConnectAsync(ct).AsTask().WaitAsync(Bound, ct);

        foreach (var payload in UnreadablePayloads)
        {
            var answer = client.SendRequestAsync(opCode, payload, compress: false, ct).AsTask();
            try
            {
                await answer.WaitAsync(Bound, ct);
            }
            catch (Exception ex) when (ex is TimeoutException or IOException)
            {
                Assert.Fail($"{opCode} with a payload of {payload.Length} bytes ({Convert.ToHexString(payload)}) was not answered: {ex.GetType().Name}: {ex.Message}");
            }
        }

        // The connection is where it was: a request the broker can read is answered as such.
        var (header, _) = await client.SendRequestAsync(SurgewaveOpCode.ListTopics, ReadOnlyMemory<byte>.Empty, compress: false, ct)
            .AsTask().WaitAsync(Bound, ct);
        Assert.Equal(SurgewaveErrorCode.None, header.ErrorCode);
    }

    /// <summary>
    /// What the guard above found: a stream acknowledgement was taken without an answer, readable or not, while
    /// the streaming consumer sends it as a request and waits for the answer before it sends the next one.
    /// </summary>
    [Fact]
    public async Task AStreamAcknowledgementIsAnswered()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var client = new TcpTransport(
            new TransportOptions { Host = "127.0.0.1", Port = broker.Port, EnableCompression = false });
        await client.ConnectAsync(ct).AsTask().WaitAsync(Bound, ct);

        var subscription = Guid.NewGuid().ToString("N");
        var subscribe = new SubscribePayload
        {
            SubscriptionId = subscription,
            Topic = $"stream-{subscription}",
            Partitions = [new PartitionOffset(0, -1)],
            MaxBytesPerPush = 64 * 1024,
        };
        var (subscribed, _) = await client.SendRequestAsync(SurgewaveOpCode.Subscribe, Bytes(subscribe.EstimateSize(), subscribe.Write), compress: false, ct)
            .AsTask().WaitAsync(Bound, ct);
        Assert.Equal(SurgewaveErrorCode.None, subscribed.ErrorCode);

        var ack = new StreamAckPayload { SubscriptionId = subscription, AcknowledgedBytes = 4096 };
        var (acknowledged, _) = await client.SendRequestAsync(SurgewaveOpCode.StreamAck, Bytes(ack.EstimateSize(), ack.Write), compress: false, ct)
            .AsTask().WaitAsync(Bound, ct);
        Assert.Equal(SurgewaveOpCode.StreamAck, acknowledged.OpCode);
        Assert.Equal(SurgewaveErrorCode.None, acknowledged.ErrorCode);

        var stray = new StreamAckPayload { SubscriptionId = "no-such-subscription", AcknowledgedBytes = 4096 };
        var (refused, _) = await client.SendRequestAsync(SurgewaveOpCode.StreamAck, Bytes(stray.EstimateSize(), stray.Write), compress: false, ct)
            .AsTask().WaitAsync(Bound, ct);
        Assert.Equal(SurgewaveErrorCode.SubscriptionNotFound, refused.ErrorCode);
    }

    private delegate void WritePayload(ref SurgewavePayloadWriter writer);

    private static ReadOnlyMemory<byte> Bytes(int estimatedSize, WritePayload write)
    {
        var buffer = new byte[estimatedSize + 32];
        var writer = new SurgewavePayloadWriter(buffer);
        write(ref writer);
        return buffer.AsMemory(0, writer.Position);
    }

    /// <summary>One broker for the class: each op code asks it over a connection of its own.</summary>
    public sealed class RunningBroker : IAsyncLifetime
    {
        private SurgewaveRuntime? _runtime;

        public int Port => _runtime!.Port;

        public async ValueTask InitializeAsync()
        {
            _runtime = await SurgewaveRuntime.CreateBuilder()
                .WithPort(0)
                .WithStorageEngine(StorageEngines.Memory)
                .Build()
                .StartAsync();
        }

        public async ValueTask DisposeAsync()
        {
            if (_runtime is not null)
                await _runtime.DisposeAsync();
        }
    }
}