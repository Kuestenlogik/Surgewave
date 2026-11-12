using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A Raft request is answered by its own answer or by none (#275): the answer to a request the caller gave up on, or
/// one that came after the request's time was over, is never read as the answer to the next request.
/// </summary>
/// <remarks>
/// A TCP connection to a peer carries one request after the other and the answers in that order. A request that was
/// abandoned after it was written — the caller cancelled, or the last attempt ran out of time — left the connection
/// in place with its answer still to come, and the next request on it read that answer: a pre-vote granted for one
/// term counted as the vote of another, the two answers having the same layout. The peer here is a socket that
/// answers when the test says so, so each case is the order of the frames and nothing else.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftTransportAbandonedRequestTests : IAsyncDisposable
{
    private const int Peer = 2;
    private const short PreVoteApiKey = 104;
    private const short RequestVoteApiKey = 101;

    private readonly SlowPeer _peer = new();
    private readonly RaftTransport _transport;

    public RaftTransportAbandonedRequestTests()
    {
        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = Peer, Host = "127.0.0.1", Port = 9092, ReplicationPort = _peer.Port });
        _transport = new RaftTransport(
            NullLogger<RaftTransport>.Instance, state, new ClusteringConfig { BrokerId = 1 }, new TcpPeerTransport())
        {
            RequestTimeout = TimeSpan.FromMilliseconds(250),
        };
    }

    [Fact]
    public async Task TheAnswerToARequestTheCallerGaveUpOn_IsNotReadAsTheAnswerToTheNextOne()
    {
        var ct = TestContext.Current.CancellationToken;
        using var giveUp = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var abandoned = _transport.SendPreVoteAsync(Peer, new PreVoteRequest(6, 1, 0, 0), giveUp.Token);
        var preVote = await _peer.NextRequestAsync(ct);
        Assert.Equal(PreVoteApiKey, preVote.ApiKey);

        // The caller gives up while the peer still has the request.
        await giveUp.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned);

        var asked = _transport.SendRequestVoteAsync(Peer, new RequestVoteRequest(6, 1, 0, 0), ct);
        var vote = await _peer.NextRequestAsync(ct);
        Assert.Equal(RequestVoteApiKey, vote.ApiKey);

        // The peer answers in the order it was asked: the pre-vote granted, then the vote refused.
        await preVote.AnswerAsync(term: 5, granted: true);
        await vote.AnswerAsync(term: 6, granted: false);

        var response = await asked;
        Assert.False(response.VoteGranted, "the pre-vote granted to the abandoned request was read as the vote");
        Assert.Equal(6, response.Term);
    }

    [Fact]
    public async Task TheAnswerThatComesAfterARequestRanOutOfTime_IsNotReadAsTheAnswerToTheNextOne()
    {
        var ct = TestContext.Current.CancellationToken;

        // Three attempts, none answered in time; the last one is what the request ends with.
        var timedOut = _transport.SendPreVoteAsync(Peer, new PreVoteRequest(6, 1, 0, 0), ct);
        await _peer.NextRequestAsync(ct);
        await _peer.NextRequestAsync(ct);
        var last = await _peer.NextRequestAsync(ct);
        await Assert.ThrowsAnyAsync<Exception>(() => timedOut);

        var asked = _transport.SendRequestVoteAsync(Peer, new RequestVoteRequest(6, 1, 0, 0), ct);
        var vote = await _peer.NextRequestAsync(ct);
        Assert.Equal(RequestVoteApiKey, vote.ApiKey);

        await last.AnswerAsync(term: 5, granted: true);
        await vote.AnswerAsync(term: 6, granted: false);

        var response = await asked;
        Assert.False(response.VoteGranted, "the pre-vote granted after its request had timed out was read as the vote");
        Assert.Equal(6, response.Term);
    }

    [Fact]
    public async Task AnAnswerToAnotherRequest_IsNotTakenForTheAnswer()
    {
        var ct = TestContext.Current.CancellationToken;

        var asked = _transport.SendRequestVoteAsync(Peer, new RequestVoteRequest(6, 1, 0, 0), ct);
        var first = await _peer.NextRequestAsync(ct);

        // A frame that answers some other request: the correlation id is not this request's.
        await first.AnswerAsync(term: 5, granted: true, correlationId: first.CorrelationId + 1000);

        // The request is asked again, on a connection of its own, and answered there.
        var askedAgain = _peer.NextRequestAsync(ct);
        if (await Task.WhenAny(asked, askedAgain) == asked)
            Assert.Fail($"an answer with another request's correlation id was taken for the vote: granted {(await asked).VoteGranted}");
        var again = await askedAgain;
        Assert.NotSame(first.Connection, again.Connection);
        await again.AnswerAsync(term: 6, granted: false);

        var response = await asked;
        Assert.False(response.VoteGranted, "an answer with another request's correlation id was taken for the vote");
        Assert.Equal(6, response.Term);
    }

    public async ValueTask DisposeAsync()
    {
        await _transport.DisposeAsync();
        await _peer.DisposeAsync();
    }

    /// <summary>One request frame as the peer read it, and the connection to answer it on.</summary>
    private sealed record Request(short ApiKey, int CorrelationId, NetworkStream Connection)
    {
        /// <summary>
        /// Writes a vote or pre-vote answer, which share a layout. A connection the asking side has dropped since
        /// takes it or refuses it, as the socket decides; either way nobody reads it.
        /// </summary>
        public async Task AnswerAsync(int term, bool granted, int? correlationId = null)
        {
            var frame = new byte[13];
            BinaryPrimitives.WriteInt32BigEndian(frame, 9);
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(4), correlationId ?? CorrelationId);
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(8), term);
            frame[12] = (byte)(granted ? 1 : 0);
            try
            {
                await Connection.WriteAsync(frame);
                await Connection.FlushAsync();
            }
            catch (IOException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }

    /// <summary>
    /// A peer that reads every request and answers none by itself: the test takes the requests in the order they
    /// arrived and answers them when and how the case needs.
    /// </summary>
    private sealed class SlowPeer : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Channel<Request> _requests = Channel.CreateUnbounded<Request>();
        private readonly List<Task> _connections = [];
        private readonly Task _accepting;

        public SlowPeer()
        {
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _accepting = AcceptAsync();
        }

        public int Port { get; }

        public async Task<Request> NextRequestAsync(CancellationToken ct) =>
            await _requests.Reader.ReadAsync(ct).AsTask().WaitAsync(TimeSpan.FromSeconds(30), ct);

        private async Task AcceptAsync()
        {
            try
            {
                while (true)
                {
                    var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    lock (_connections)
                        _connections.Add(ReadAsync(client));
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task ReadAsync(TcpClient client)
        {
            using (client)
            {
                var stream = client.GetStream();
                var size = new byte[4];
                try
                {
                    while (await stream.ReadAtLeastAsync(size, 4, throwOnEndOfStream: false, _stop.Token) == 4)
                    {
                        var body = new byte[BinaryPrimitives.ReadInt32BigEndian(size)];
                        await stream.ReadExactlyAsync(body, _stop.Token);
                        await _requests.Writer.WriteAsync(new Request(
                            BinaryPrimitives.ReadInt16BigEndian(body),
                            BinaryPrimitives.ReadInt32BigEndian(body.AsSpan(4)),
                            stream));
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (IOException)
                {
                }

                // The connection stays open for a late answer until the peer stops.
                try { await Task.Delay(Timeout.Infinite, _stop.Token); }
                catch (OperationCanceledException) { }
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            _listener.Stop();
            await _accepting;
            Task[] connections;
            lock (_connections)
                connections = [.. _connections];
            await Task.WhenAll(connections);
            _listener.Dispose();
            _stop.Dispose();
        }
    }
}
