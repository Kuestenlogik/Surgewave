using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// A Kafka request is answered or its connection is closed (#281). A Kafka client matches answers to requests by
/// their order on the connection, so a request left without an answer is not only one the client waits for until
/// its request timeout: every answer after it is taken for the answer to the request before.
/// </summary>
/// <remarks>
/// <para>
/// Kafka answers a request whose handling failed with an error response of that API, and closes the connection
/// where it cannot build one; it closes the connection for a request it cannot read. This broker builds no such
/// error response, so it closes in both cases, which a client takes as a disconnect and retries at once.
/// </para>
/// <para>
/// Each test runs the Kafka connection handler over a loopback socket, as the broker does for an accepted
/// connection, and reads what a client would read. The bound on a wait is how long a test may take to fail.
/// </para>
/// </remarks>
public sealed class KafkaRequestAnswerTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task ARequestWhoseHandlerThrowsClosesTheConnection()
    {
        await using var connection = await KafkaConnection.OpenAsync(
            new Handler(ApiKey.ApiVersions, (_, _) => throw new IOException("the disk is gone")));

        await connection.SendAsync(Request(ApiKey.ApiVersions, correlationId: 1));

        Assert.Null(await connection.ReadFrameAsync());
    }

    /// <summary>
    /// The request after the failed one is not answered in its place: the client would read the answer with
    /// correlation id 2 as the answer to its request 1.
    /// </summary>
    [Fact]
    public async Task TheRequestAfterAFailedOneIsNotAnsweredInItsPlace()
    {
        await using var connection = await KafkaConnection.OpenAsync(
            new Handler(ApiKey.ApiVersions, (request, _) => request.CorrelationId == 1
                ? throw new InvalidOperationException("an entry is missing")
                : Task.FromResult<KafkaResponse>(new CorrelationOnly { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion })));

        await connection.SendAsync([.. Request(ApiKey.ApiVersions, correlationId: 1), .. Request(ApiKey.ApiVersions, correlationId: 2)]);

        Assert.Null(await connection.ReadFrameAsync());
    }

    /// <summary>A request the broker cannot read has no answer the client could read either.</summary>
    [Fact]
    public async Task ARequestThatCannotBeReadClosesTheConnection()
    {
        await using var connection = await KafkaConnection.OpenAsync(
            new Handler(ApiKey.ApiVersions, (request, _) => Task.FromResult<KafkaResponse>(
                new CorrelationOnly { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion })));

        await connection.SendAsync(Request((ApiKey)9999, correlationId: 1));

        Assert.Null(await connection.ReadFrameAsync());
    }

    /// <summary>A broker that stops cancels its handlers; the connection is closed, and nothing is written for the request.</summary>
    [Fact]
    public async Task ABrokerThatStopsClosesTheConnection()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var connection = await KafkaConnection.OpenAsync(
            new Handler(ApiKey.ApiVersions, async (_, ct) =>
            {
                entered.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct);
                throw new InvalidOperationException("not reached");
            }));

        await connection.SendAsync(Request(ApiKey.ApiVersions, correlationId: 1));
        await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        await connection.StopBrokerAsync();

        Assert.Null(await connection.ReadFrameAsync());
    }

    /// <summary>Requests that are answered are answered in order, on a connection that stays open.</summary>
    [Fact]
    public async Task RequestsThatAreAnsweredAreAnsweredInOrder()
    {
        await using var connection = await KafkaConnection.OpenAsync(
            new Handler(ApiKey.ApiVersions, (request, _) => Task.FromResult<KafkaResponse>(
                new CorrelationOnly { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion })));

        await connection.SendAsync([.. Request(ApiKey.ApiVersions, correlationId: 1), .. Request(ApiKey.ApiVersions, correlationId: 2)]);

        Assert.Equal(1, BinaryPrimitives.ReadInt32BigEndian(await connection.ReadFrameAsync()));
        Assert.Equal(2, BinaryPrimitives.ReadInt32BigEndian(await connection.ReadFrameAsync()));
    }

    /// <summary>A request of version 0 with a null client id and no body, behind its size.</summary>
    private static byte[] Request(ApiKey apiKey, int correlationId)
    {
        var frame = new byte[4 + 2 + 2 + 4 + 2];
        BinaryPrimitives.WriteInt32BigEndian(frame, frame.Length - 4);
        BinaryPrimitives.WriteInt16BigEndian(frame.AsSpan(4), (short)apiKey);
        BinaryPrimitives.WriteInt16BigEndian(frame.AsSpan(6), 0);
        BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(8), correlationId);
        BinaryPrimitives.WriteInt16BigEndian(frame.AsSpan(12), -1);
        return frame;
    }

    private sealed class Handler(ApiKey apiKey, Func<KafkaRequest, CancellationToken, Task<KafkaResponse>> handle) : IKafkaRequestHandler
    {
        public IEnumerable<ApiKey> SupportedApiKeys => [apiKey];

        public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
            => handle(request, cancellationToken);
    }

    private sealed class CorrelationOnly : KafkaResponse
    {
        public override void WriteTo(KafkaProtocolWriter writer) => writer.WriteInt32(CorrelationId);
    }

    /// <summary>
    /// A client's socket and the Kafka connection handler on the other end of it, treated as the broker treats an
    /// accepted connection: the first four bytes are read before the handler is chosen, and the socket is closed
    /// when the handler returns or throws.
    /// </summary>
    private sealed class KafkaConnection : IAsyncDisposable
    {
        private readonly TcpClient _client;
        private readonly CancellationTokenSource _brokerStopping = new();
        private readonly Task _served;

        private KafkaConnection(TcpClient client, TcpClient accepted, KafkaConnectionHandler handler)
        {
            _client = client;
            _served = ServeAsync(accepted, handler);
        }

        public static async Task<KafkaConnection> OpenAsync(params IKafkaRequestHandler[] handlers)
        {
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var client = new TcpClient { NoDelay = true };
            await client.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port, TestContext.Current.CancellationToken);
            var accepted = await listener.AcceptTcpClientAsync(TestContext.Current.CancellationToken);
            var handler = new KafkaConnectionHandler(handlers, pipelineDepth: 8, metrics: null, NullLogger<KafkaConnectionHandler>.Instance);
            return new KafkaConnection(client, accepted, handler);
        }

        private async Task ServeAsync(TcpClient accepted, KafkaConnectionHandler handler)
        {
            using var _ = accepted;
            try
            {
                var stream = accepted.GetStream();
                var first = new byte[4];
                await stream.ReadExactlyAsync(first, _brokerStopping.Token);
                await handler.HandleConnectionAsync(stream, first, new ConnectionContext("127.0.0.1", accepted.Client.RemoteEndPoint), _brokerStopping.Token);
            }
            catch (Exception)
            {
                // Whatever ends a connection, the broker logs it and closes the socket, which leaving this method does.
            }
        }

        public Task SendAsync(byte[] bytes) =>
            _client.GetStream().WriteAsync(bytes, TestContext.Current.CancellationToken).AsTask();

        public Task StopBrokerAsync() => _brokerStopping.CancelAsync();

        /// <summary>The next frame the broker wrote, without its size; null when the broker closed the connection.</summary>
        public async Task<byte[]?> ReadFrameAsync()
        {
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            bound.CancelAfter(Bound);
            var stream = _client.GetStream();
            try
            {
                var size = new byte[4];
                if (await stream.ReadAtLeastAsync(size, size.Length, throwOnEndOfStream: false, bound.Token) < size.Length)
                    return null;

                var frame = new byte[BinaryPrimitives.ReadInt32BigEndian(size)];
                await stream.ReadExactlyAsync(frame, bound.Token);
                return frame;
            }
            catch (OperationCanceledException) when (!TestContext.Current.CancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException($"The broker neither answered nor closed the connection within {Bound.TotalSeconds:F0} s.");
            }
        }

        public async ValueTask DisposeAsync()
        {
            _client.Dispose();
            await _brokerStopping.CancelAsync();
            await _served;
            _brokerStopping.Dispose();
        }
    }
}
