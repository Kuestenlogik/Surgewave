using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Commands;
using Kuestenlogik.Surgewave.Client.Tests.Fakes;
using Kuestenlogik.Surgewave.Protocol.Native;
using Xunit;
using Answer = Kuestenlogik.Surgewave.Client.Tests.Fakes.ScriptedNativeServer.Answer;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// What a user of <see cref="SurgewaveNativeClient"/> gets when the broker closes the connection, and what it
/// gets of an error the broker answered with (#281). Over a loopback socket and the client's own transport.
/// </summary>
public sealed class NativeClientConnectionLossTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A call waiting for its answer fails when the broker closes the connection, and so does every call after
    /// it: with an <see cref="IOException"/>, which is what the client's retry policies take for a lost connection,
    /// and with the broker it was. The call after used to be written into the closed connection and wait.
    /// </summary>
    [Fact]
    public async Task ACallWaitingForItsAnswerAndTheCallsAfterItFailWhenTheBrokerClosesTheConnection()
    {
        var ct = TestContext.Current.CancellationToken;
        var asked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ScriptedNativeServer(_ =>
        {
            asked.TrySetResult();
            return null;
        });
        await using var client = new SurgewaveNativeClient("127.0.0.1", server.Port);
        await client.ConnectAsync(ct).WaitAsync(Bound, ct);

        var waiting = client.Topics.ListAsync(ct);
        await asked.Task.WaitAsync(Bound, ct);
        await server.CloseConnectionAsync();

        var failure = await Assert.ThrowsAnyAsync<IOException>(() => waiting.WaitAsync(Bound, ct));
        Assert.Contains($"127.0.0.1:{server.Port}", failure.Message, StringComparison.Ordinal);
        Assert.False(client.IsConnected);

        var after = await Assert.ThrowsAnyAsync<IOException>(() => client.Topics.ListAsync(ct).WaitAsync(Bound, ct));
        Assert.Contains($"127.0.0.1:{server.Port}", after.Message, StringComparison.Ordinal);
        Assert.Contains("closed the connection", after.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A streaming consumer waits for records the broker pushes; no request of its own is in flight. When the
    /// connection goes, reading its records fails and says why, instead of waiting for the next record.
    /// </summary>
    [Fact]
    public async Task AStreamingConsumersRecordsFailWhenTheBrokerClosesTheConnection()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var server = new ScriptedNativeServer(request => Answer.Ok(request.OpCode));
        await using var client = new SurgewaveNativeClient("127.0.0.1", server.Port);
        await client.ConnectAsync(ct).WaitAsync(Bound, ct);
        await using var consumer = await client.Messaging.SubscribeAsync("orders", [0], cancellationToken: ct).WaitAsync(Bound, ct);

        var reading = Task.Run(async () =>
        {
            await foreach (var _ in consumer.Records.WithCancellation(ct))
            {
            }
        }, ct);

        await server.CloseConnectionAsync();

        var failure = await Assert.ThrowsAnyAsync<IOException>(() => reading.WaitAsync(Bound, ct));
        Assert.Contains("closed the connection", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// What the broker says about a request it refuses reaches the caller: for an internal error that is the
    /// request to look up in the broker's log.
    /// </summary>
    [Fact]
    public async Task TheBrokersWordsAboutARefusedRequestAreInTheFailure()
    {
        var ct = TestContext.Current.CancellationToken;
        const string said = "The broker failed to carry out CreateTopic (request 1); the cause is in the broker's log";
        await using var server = new ScriptedNativeServer(_ => Answer.Error(SurgewaveErrorCode.InternalError, said));
        await using var client = new SurgewaveNativeClient("127.0.0.1", server.Port);
        await client.ConnectAsync(ct).WaitAsync(Bound, ct);

        var failure = await Assert.ThrowsAsync<SurgewaveProtocolException>(
            () => client.Topics.CreateAsync("orders", partitions: 1, cancellationToken: ct).WaitAsync(Bound, ct));

        Assert.Equal(SurgewaveErrorCode.InternalError, failure.ErrorCode);
        Assert.Equal(said, failure.BrokerMessage);
        Assert.Contains(said, failure.Message, StringComparison.Ordinal);
    }
}
