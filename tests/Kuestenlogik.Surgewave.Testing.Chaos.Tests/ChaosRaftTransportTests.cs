using Kuestenlogik.Surgewave.Testing.Chaos;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using NSubstitute;
using Xunit;

namespace Kuestenlogik.Surgewave.Testing.Chaos.Tests;

/// <summary>
/// Unit tests for ChaosRaftTransport fault injection around IRaftTransport operations.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ChaosRaftTransportTests
{
    private readonly ChaosEngine _engine;
    private readonly IRaftTransport _innerTransport;
    private readonly ChaosRaftTransport _chaosTransport;

    public ChaosRaftTransportTests()
    {
        _engine = new ChaosEngine();
        _innerTransport = Substitute.For<IRaftTransport>();
        _chaosTransport = new ChaosRaftTransport(_innerTransport, _engine, brokerId: 1);
    }

    [Fact]
    public async Task SendAppendEntries_WithPartition_ThrowsException()
    {
        // Arrange
        _engine.ActivateFault(FaultType.NetworkPartition, new FaultScope
        {
            BrokerId = 1,
            TargetPeerId = 2
        });

        var request = new AppendEntriesRequest(
            Term: 1, LeaderId: 1, PrevLogIndex: 0, PrevLogTerm: 0,
            Entries: [], LeaderCommit: 0);

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(
            () => _chaosTransport.SendAppendEntriesAsync(2, request, CancellationToken.None));
    }

    [Fact]
    public async Task SendAppendEntries_NoFault_DelegatesToInner()
    {
        // Arrange
        var request = new AppendEntriesRequest(
            Term: 1, LeaderId: 1, PrevLogIndex: 0, PrevLogTerm: 0,
            Entries: [], LeaderCommit: 0);

        var expectedResponse = new AppendEntriesResponse(Term: 1, Success: true, MatchIndex: 0);
        _innerTransport.SendAppendEntriesAsync(2, request, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(expectedResponse));

        // Act
        var response = await _chaosTransport.SendAppendEntriesAsync(2, request, CancellationToken.None);

        // Assert
        Assert.Equal(expectedResponse, response);
        await _innerTransport.Received(1).SendAppendEntriesAsync(2, request, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SendRequestVote_WithElectionDisruption_ReturnsRejection()
    {
        // Arrange - election disruption returns a rejection, not an exception
        _engine.ActivateFault(FaultType.LeaderElectionDisruption, new FaultScope
        {
            BrokerId = 1
        });

        var request = new RequestVoteRequest(
            Term: 2, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0);

        // Act
        var response = await _chaosTransport.SendRequestVoteAsync(2, request, CancellationToken.None);

        // Assert - election disruption drops the request and returns a rejection
        Assert.False(response.VoteGranted);
    }

    [Fact]
    public async Task IsPeerReachable_WithPartition_ReturnsFalse()
    {
        // Arrange
        _engine.ActivateFault(FaultType.NetworkPartition, new FaultScope
        {
            BrokerId = 1,
            TargetPeerId = 2
        });

        // Act
        var reachable = await _chaosTransport.IsPeerReachableAsync(2, CancellationToken.None);

        // Assert
        Assert.False(reachable);
    }

    [Fact]
    public async Task IsPeerReachable_NoFault_DelegatesToInner()
    {
        // Arrange
        _innerTransport.IsPeerReachableAsync(2, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(true));

        // Act
        var reachable = await _chaosTransport.IsPeerReachableAsync(2, CancellationToken.None);

        // Assert
        Assert.True(reachable);
        await _innerTransport.Received(1).IsPeerReachableAsync(2, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SlowNetwork_InjectsLatency()
    {
        // Arrange
        var latency = TimeSpan.FromMilliseconds(100);
        _engine.ActivateFault(FaultType.SlowNetwork, new FaultScope { BrokerId = 1 }, latency);

        var request = new AppendEntriesRequest(
            Term: 1, LeaderId: 1, PrevLogIndex: 0, PrevLogTerm: 0,
            Entries: [], LeaderCommit: 0);

        var expectedResponse = new AppendEntriesResponse(Term: 1, Success: true, MatchIndex: 0);
        _innerTransport.SendAppendEntriesAsync(2, request, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(expectedResponse));

        // Act
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var response = await _chaosTransport.SendAppendEntriesAsync(2, request, CancellationToken.None);
        sw.Stop();

        // Assert
        Assert.Equal(expectedResponse, response);
        Assert.True(sw.ElapsedMilliseconds >= 50,
            $"Expected at least 50ms latency, but took {sw.ElapsedMilliseconds}ms");
    }
}
