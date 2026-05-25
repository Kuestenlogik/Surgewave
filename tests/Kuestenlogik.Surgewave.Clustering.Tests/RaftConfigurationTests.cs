using System.Collections.Immutable;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Building-block tests for the immutable <see cref="RaftConfiguration"/>
/// snapshot. Surgewave's static-voter-set today doesn't mutate this type at
/// runtime, but the future KIP-853 implementation will — pinning the
/// add/remove/update semantics now means the eventual implementation
/// inherits a tested foundation rather than re-deriving the rules.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftConfigurationTests
{
    [Fact]
    public void Empty_HasNoVotersAndZeroSequence()
    {
        var empty = RaftConfiguration.Empty;
        Assert.Empty(empty.Voters);
        Assert.Equal(0, empty.ConfigurationSequence);
        Assert.Equal(1, empty.Majority); // 0/2 + 1 — degenerate but defined
    }

    [Theory]
    [InlineData(1, 1)] // 1 voter  → majority 1
    [InlineData(2, 2)] // 2 voters → majority 2 (no tolerance for failure)
    [InlineData(3, 2)] // 3 voters → majority 2 (tolerates 1 failure)
    [InlineData(5, 3)] // 5 voters → majority 3 (tolerates 2)
    [InlineData(7, 4)] // 7 voters → majority 4 (tolerates 3)
    public void Majority_IsStrictMajorityOfVoterCount(int voterCount, int expectedMajority)
    {
        var config = BuildConfig(voterCount);
        Assert.Equal(expectedMajority, config.Majority);
    }

    [Fact]
    public void AddVoter_AppendsAndBumpsSequence()
    {
        var initial = BuildConfig(2);
        var voter3 = MakeVoter(3);

        var added = initial.AddVoter(voter3);

        Assert.Equal(3, added.Voters.Length);
        Assert.Equal(initial.ConfigurationSequence + 1, added.ConfigurationSequence);
        Assert.True(added.ContainsVoter(3));
        // Original is unchanged — record-immutability invariant.
        Assert.Equal(2, initial.Voters.Length);
    }

    [Fact]
    public void AddVoter_DuplicateNodeId_Throws()
    {
        var initial = BuildConfig(2);

        var ex = Assert.Throws<InvalidOperationException>(() => initial.AddVoter(MakeVoter(1)));
        Assert.Contains("already in the configuration", ex.Message);
    }

    [Fact]
    public void RemoveVoter_ShrinksAndBumpsSequence()
    {
        var initial = BuildConfig(3);

        var removed = initial.RemoveVoter(2);

        Assert.Equal(2, removed.Voters.Length);
        Assert.False(removed.ContainsVoter(2));
        Assert.Equal(initial.ConfigurationSequence + 1, removed.ConfigurationSequence);
    }

    [Fact]
    public void RemoveVoter_UnknownNodeId_Throws()
    {
        var initial = BuildConfig(2);

        Assert.Throws<InvalidOperationException>(() => initial.RemoveVoter(99));
    }

    [Fact]
    public void UpdateVoter_ReplacesEntryAndBumpsSequence()
    {
        var initial = BuildConfig(2);
        var updated = MakeVoter(1, port: 9095);

        var newConfig = initial.UpdateVoter(updated);

        var entry = Assert.Single(newConfig.Voters, v => v.NodeId == 1);
        Assert.Equal((ushort)9095, entry.Listeners[0].Port);
        Assert.Equal(initial.ConfigurationSequence + 1, newConfig.ConfigurationSequence);
    }

    [Fact]
    public void UpdateVoter_UnknownNodeId_Throws()
    {
        var initial = BuildConfig(2);
        Assert.Throws<InvalidOperationException>(() => initial.UpdateVoter(MakeVoter(42)));
    }

    [Fact]
    public void ChainedMutations_PreserveSequenceMonotonicity()
    {
        // Sequence number is the single-server-change "only one diff in
        // flight" invariant's ground truth — verify it never goes backwards
        // across a representative chain of mutations.
        var c0 = RaftConfiguration.Empty;
        var c1 = c0.AddVoter(MakeVoter(1));
        var c2 = c1.AddVoter(MakeVoter(2));
        var c3 = c2.AddVoter(MakeVoter(3));
        var c4 = c3.UpdateVoter(MakeVoter(2, port: 9100));
        var c5 = c4.RemoveVoter(3);

        var sequences = new[] { c0, c1, c2, c3, c4, c5 }.Select(c => c.ConfigurationSequence).ToList();
        for (var i = 1; i < sequences.Count; i++)
        {
            Assert.True(sequences[i] > sequences[i - 1],
                $"Sequence regressed between step {i - 1} ({sequences[i - 1]}) and step {i} ({sequences[i]})");
        }
    }

    private static RaftConfiguration BuildConfig(int voterCount)
    {
        var voters = ImmutableArray.CreateRange(
            Enumerable.Range(1, voterCount).Select(id => MakeVoter(id)));
        return new RaftConfiguration
        {
            Voters = voters,
            ConfigurationSequence = 0,
        };
    }

    private static RaftVoter MakeVoter(int nodeId, ushort port = 9092) => new()
    {
        NodeId = nodeId,
        DirectoryId = Guid.NewGuid(),
        Listeners = ImmutableArray.Create(new RaftListener
        {
            Name = "INTERNAL",
            Host = $"broker-{nodeId}",
            Port = port,
        }),
    };
}
