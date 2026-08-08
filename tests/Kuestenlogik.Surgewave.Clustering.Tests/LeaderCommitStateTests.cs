using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// What a leader knows about what has actually been replicated (#122, prerequisite).
///
/// <para>The leader tracked its own log end offset in <c>PartitionReplica.LogEndOffset</c>, which is
/// written when the broker becomes leader or follower and — on a leader — never again: producer
/// appends go to the log directly. Two things followed from reading that field as "the leader's
/// LEO". Follower lag came out as zero or negative forever, so no follower could ever be found
/// lagging; and the high watermark, being the minimum LEO across the ISR, was pinned to the offset
/// the partition had when leadership was won.</para>
///
/// <para>The second gap is silence: the lag check runs only when a follower fetch ARRIVES, so a
/// follower that dies never triggers it, stays in the ISR, and — contributing a LEO of zero —
/// holds the watermark down permanently.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class LeaderCommitStateTests
{
    private const int Leader = 0;
    private const int Follower = 1;

    [Fact]
    public async Task HighWatermark_FollowerCaughtUpAfterProduce_TracksTheAppendedOffsets()
    {
        var (rm, lm, state) = NewLeader(out var tp);

        // Produce after leadership was won — exactly what the frozen field never saw.
        await lm.AppendBatchAsync(tp, CreateValidBatch(0, 5), BatchCrcMode.Validate, CancellationToken.None);
        Assert.Equal(5, lm.GetLog(tp)!.NextOffset);

        rm.UpdateFollowerFetchPosition(tp, Follower, fetchOffset: 5);

        Assert.Equal(5, state.GetPartitionState(tp)!.HighWatermark);
    }

    [Fact]
    public async Task HighWatermark_FollowerBehind_StopsAtWhatTheFollowerHas()
    {
        var (rm, lm, state) = NewLeader(out var tp);

        await lm.AppendBatchAsync(tp, CreateValidBatch(0, 5), BatchCrcMode.Validate, CancellationToken.None);
        await lm.AppendBatchAsync(tp, CreateValidBatch(5, 5), BatchCrcMode.Validate, CancellationToken.None);

        rm.UpdateFollowerFetchPosition(tp, Follower, fetchOffset: 5);

        // Not 10: the follower only has the first batch, and the watermark is the ISR minimum.
        Assert.Equal(5, state.GetPartitionState(tp)!.HighWatermark);
    }

    [Fact]
    public void HighWatermark_NeverGoesBackwards()
    {
        var partitionState = new PartitionState { TopicPartition = new TopicPartition { Topic = "t", Partition = 0 } };

        Assert.True(partitionState.TryAdvanceHighWatermark(10));
        Assert.False(partitionState.TryAdvanceHighWatermark(4));
        Assert.Equal(10, partitionState.HighWatermark);
    }

    [Fact]
    public async Task SilentFollower_IsExpiredFromTheIsr_AndTheWatermarkMovesOn()
    {
        // The failure that made acks=all unimplementable: a follower that stops fetching keeps its
        // ISR seat, and since it never reports a LEO the watermark stays where it was.
        var (rm, lm, state) = NewLeader(out var tp);
        rm.ReplicaLagTimeMax = TimeSpan.FromMilliseconds(200);

        await lm.AppendBatchAsync(tp, CreateValidBatch(0, 5), BatchCrcMode.Validate, CancellationToken.None);
        rm.UpdateFollowerFetchPosition(tp, Follower, fetchOffset: 5);
        Assert.Contains(Follower, state.GetIsrSnapshot(tp));
        Assert.Equal(5, state.GetPartitionState(tp)!.HighWatermark);

        // The follower dies here: more records are appended, and no fetch ever arrives for them.
        await lm.AppendBatchAsync(tp, CreateValidBatch(5, 5), BatchCrcMode.Validate, CancellationToken.None);
        rm.CheckIsrForPartition(tp);
        Assert.Equal(5, state.GetPartitionState(tp)!.HighWatermark);

        await Task.Delay(300);
        rm.CheckIsrForPartition(tp);

        Assert.DoesNotContain(Follower, state.GetIsrSnapshot(tp));
        Assert.Equal(10, state.GetPartitionState(tp)!.HighWatermark);
    }

    [Fact]
    public void UnobservedFollower_GetsAGracePeriodBeforeBeingExpired()
    {
        // A replica that has not fetched YET is not the same as one that fell silent — a freshly
        // assigned ISR contains followers that have never reported, and evicting them on the first
        // sweep would shrink every new partition down to its leader.
        var (rm, _, state) = NewLeader(out var tp);
        rm.ReplicaLagTimeMax = TimeSpan.FromSeconds(30);

        rm.CheckIsrForPartition(tp);

        Assert.Contains(Follower, state.GetIsrSnapshot(tp));
    }

    [Fact]
    public async Task Gate_LeaderIsTheOnlyInSyncReplica_DoesNotBlock()
    {
        // Nobody to wait for: the leader's own append IS the commit. Waiting here would hang until
        // the producer's timeout on a watermark that nothing else can advance.
        var (rm, lm, state) = NewLeader(out var tp);
        state.UpdateIsr(tp, [Leader]);
        var gate = new ReplicaCommitGate(state, rm);

        await lm.AppendBatchAsync(tp, CreateValidBatch(0, 5), BatchCrcMode.Validate, CancellationToken.None);

        var committed = await gate.WaitForDurableCommitAsync(
            tp, committedThroughOffset: 5, TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.True(committed);
    }

    [Fact]
    public async Task Gate_FollowerCatchesUp_ReleasesTheProducer()
    {
        var (rm, lm, state) = NewLeader(out var tp);
        var gate = new ReplicaCommitGate(state, rm);

        await lm.AppendBatchAsync(tp, CreateValidBatch(0, 5), BatchCrcMode.Validate, CancellationToken.None);

        var waiting = gate.WaitForDurableCommitAsync(
            tp, committedThroughOffset: 5, TimeSpan.FromSeconds(5), CancellationToken.None).AsTask();

        // The whole point: the batch is on the LEADER, and the producer is still waiting.
        Assert.False(waiting.IsCompleted);

        rm.UpdateFollowerFetchPosition(tp, Follower, fetchOffset: 5);

        Assert.True(await waiting);
    }

    [Fact]
    public async Task Gate_FollowerNeverAcks_TimesOutInsteadOfClaimingDurability()
    {
        var (rm, lm, state) = NewLeader(out var tp);
        var gate = new ReplicaCommitGate(state, rm);

        await lm.AppendBatchAsync(tp, CreateValidBatch(0, 5), BatchCrcMode.Validate, CancellationToken.None);

        // The follower keeps its ISR seat but never fetches — admission would still say yes, which
        // is exactly why admission alone is not durability.
        var committed = await gate.WaitForDurableCommitAsync(
            tp, committedThroughOffset: 5, TimeSpan.FromMilliseconds(200), CancellationToken.None);

        Assert.False(committed);
    }

    private static (ReplicaManager Rm, LogManager Lm, ClusterState State) NewLeader(out TopicPartition tp)
    {
        var config = new ClusteringConfig { BrokerId = Leader, Host = "localhost", Port = 9092 };
        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = Leader, Host = "localhost", Port = 9092 });
        state.AddBroker(new BrokerNode { BrokerId = Follower, Host = "localhost", Port = 9093 });

        var lm = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-hw-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var rm = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, lm, config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());

        tp = new TopicPartition { Topic = "commit", Partition = 0 };
        state.AssignReplicas(tp, [Leader, Follower]);
        state.ElectLeader(tp, Leader);
        state.UpdateIsr(tp, [Leader, Follower]);
        rm.BecomeLeaderAsync(tp, leaderEpoch: 1, CancellationToken.None).GetAwaiter().GetResult();

        return (rm, lm, state);
    }

    private const int BatchSize = 100;

    private static byte[] CreateValidBatch(long baseOffset, int recordCount)
    {
        var batch = new byte[BatchSize];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), baseOffset);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);
        batch[16] = 2;
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), recordCount - 1);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), 1_700_000_000_000);
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), recordCount);
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);
        return batch;
    }
}
