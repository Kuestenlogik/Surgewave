using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The membership seam (#166): who votes is asked of a voter set, not of the transport.
/// </summary>
/// <remarks>
/// The default set answers exactly what the transport used to, so this changes nothing on
/// its own — which is the point, and what these tests pin. They exist so that replacing it
/// with an explicit controller quorum is a substitution rather than a behaviour change
/// discovered afterwards.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftVoterSetTests
{
    private const int LocalNode = 1;

    [Fact]
    public void TheLocalNodeIsAVoter()
    {
        // Majority is counted over the whole cluster including self; leaving self out would
        // make a three-node cluster need two of the other two.
        var set = new TransportDerivedVoterSet(new StubTransport([2, 3]), LocalNode);

        Assert.Contains(LocalNode, set.VoterIds);
        Assert.Equal(3, set.VoterIds.Count);
    }

    [Fact]
    public void ASingleNodeIsItsOwnMajority()
    {
        var set = new TransportDerivedVoterSet(new StubTransport([]), LocalNode);

        Assert.Equal([LocalNode], set.VoterIds);
        Assert.Equal(1, set.Majority);
    }

    [Theory]
    [InlineData(new int[] { }, 1)]        // 1 voter
    [InlineData(new[] { 2 }, 2)]          // 2 voters
    [InlineData(new[] { 2, 3 }, 2)]       // 3 voters
    [InlineData(new[] { 2, 3, 4 }, 3)]    // 4 voters
    [InlineData(new[] { 2, 3, 4, 5 }, 3)] // 5 voters
    public void MajorityIsTheStrictMajorityOverVoters(int[] peers, int expected)
    {
        var set = new TransportDerivedVoterSet(new StubTransport(peers), LocalNode);

        Assert.Equal(expected, set.Majority);
    }

    [Fact]
    public void ABrokerThatRegistersLaterBecomesAVoter()
    {
        // Membership is dynamic today: RaftNode re-reads it, with a comment saying so, and
        // freezing it at construction would silently change that. The set is queried, not
        // snapshotted — an explicit quorum simply returns the same answer every time.
        var transport = new StubTransport([2]);
        var set = new TransportDerivedVoterSet(transport, LocalNode);
        Assert.Equal(2, set.VoterIds.Count);

        transport.Peers = [2, 3];

        Assert.Equal(3, set.VoterIds.Count);
        Assert.Contains(3, set.VoterIds);
    }

    [Fact]
    public void ADepartedBrokerStopsBeingAVoter()
    {
        var transport = new StubTransport([2, 3]);
        var set = new TransportDerivedVoterSet(transport, LocalNode);
        Assert.Equal(3, set.VoterIds.Count);

        transport.Peers = [2];

        Assert.Equal(2, set.VoterIds.Count);
        Assert.DoesNotContain(3, set.VoterIds);
    }

    [Fact]
    public void ATransportThatListsSelfDoesNotDoubleCountIt()
    {
        // The transport derives peers from cluster state, which contains this broker too;
        // counting it twice would inflate the majority by one and could make a healthy
        // cluster unable to elect.
        var set = new TransportDerivedVoterSet(new StubTransport([LocalNode, 2]), LocalNode);

        Assert.Equal(2, set.VoterIds.Count);
        Assert.Equal(2, set.Majority);
    }

    private sealed class StubTransport : IRaftTransport
    {
        public StubTransport(IReadOnlyList<int> peers) => Peers = peers;

        public IReadOnlyList<int> Peers { get; set; }

        public IReadOnlyList<int> GetPeerIds() => Peers;

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(true);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
            => throw new NotSupportedException();
    }
}
