using System.Text;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Testing.Chaos.Linearizability;
using Xunit;

namespace Kuestenlogik.Surgewave.Testing.Chaos.Tests.Linearizability;

/// <summary>
/// Synthetic histories with and without injected violations prove that
/// <see cref="LinearizabilityChecker"/> catches the exact per-partition invariants we
/// expect of a Kafka-like broker: append-only ordering, read-after-write consistency,
/// consistent reads, contiguous offsets, and absence of lost writes within the
/// observed range.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class LinearizabilityCheckerTests
{
    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static DateTimeOffset T(int seq) => DateTimeOffset.UnixEpoch.AddMilliseconds(seq);

    private static History Clean3PerPartition(string topic = "orders", int partition = 0)
    {
        var h = new History();
        for (var i = 0; i < 3; i++)
        {
            h.Record(new ProduceInvoke { ClientId = 1, Timestamp = T(2 * i), Topic = topic, Partition = partition, Value = Bytes($"m{i}") });
            h.Record(new ProduceOk { ClientId = 1, Timestamp = T(2 * i + 1), Topic = topic, Partition = partition, Offset = i, Value = Bytes($"m{i}") });
        }
        for (var i = 0; i < 3; i++)
        {
            h.Record(new ConsumeInvoke { ClientId = 2, Timestamp = T(100 + 2 * i), Topic = topic, Partition = partition, FromOffset = i });
            h.Record(new ConsumeOk { ClientId = 2, Timestamp = T(100 + 2 * i + 1), Topic = topic, Partition = partition, Offset = i, Value = Bytes($"m{i}") });
        }
        return h;
    }

    [Fact]
    public void CleanHistory_Passes()
    {
        var checker = new LinearizabilityChecker();
        var result = checker.Check(Clean3PerPartition());
        Assert.True(result.IsValid, string.Join("\n", result.Violations.Select(v => v.Description)));
    }

    [Fact]
    public void EmptyHistory_Passes()
    {
        var result = new LinearizabilityChecker().Check(new History());
        Assert.True(result.IsValid);
        Assert.Empty(result.Violations);
    }

    [Fact]
    public void OffsetCollision_IsCaught()
    {
        var h = new History();
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(0), Topic = "t", Partition = 0, Offset = 5, Value = Bytes("a") });
        h.Record(new ProduceOk { ClientId = 2, Timestamp = T(1), Topic = "t", Partition = 0, Offset = 5, Value = Bytes("b") });

        var result = new LinearizabilityChecker().Check(h);
        Assert.False(result.IsValid);
        Assert.Contains(result.Violations, v => v is OffsetCollisionViolation oc && oc.Offset == 5);
    }

    [Fact]
    public void IdempotentProduceAtSameOffset_DoesNotCollide()
    {
        // Idempotent producer retries acknowledging the same (offset, value) — Kafka
        // semantics allow this under EOS / idempotence. Only different values should
        // collide.
        var h = new History();
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(0), Topic = "t", Partition = 0, Offset = 5, Value = Bytes("a") });
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(1), Topic = "t", Partition = 0, Offset = 5, Value = Bytes("a") });

        var result = new LinearizabilityChecker().Check(h);
        Assert.DoesNotContain(result.Violations, v => v is OffsetCollisionViolation);
    }

    [Fact]
    public void DivergentRead_IsCaught()
    {
        var h = new History();
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(0), Topic = "t", Partition = 0, Offset = 0, Value = Bytes("a") });
        h.Record(new ConsumeOk { ClientId = 2, Timestamp = T(1), Topic = "t", Partition = 0, Offset = 0, Value = Bytes("x") });

        var result = new LinearizabilityChecker().Check(h);
        Assert.False(result.IsValid);
        Assert.Contains(result.Violations, v => v is DivergentReadViolation);
    }

    [Fact]
    public void InconsistentReads_AreCaught()
    {
        var h = new History();
        // Two consumers disagree at the same offset; no acknowledged produce recorded.
        h.Record(new ConsumeOk { ClientId = 2, Timestamp = T(0), Topic = "t", Partition = 0, Offset = 0, Value = Bytes("x") });
        h.Record(new ConsumeOk { ClientId = 3, Timestamp = T(1), Topic = "t", Partition = 0, Offset = 0, Value = Bytes("y") });

        var result = new LinearizabilityChecker().Check(h);
        Assert.False(result.IsValid);
        Assert.Contains(result.Violations, v => v is InconsistentReadsViolation);
    }

    [Fact]
    public void OffsetGap_IsCaught()
    {
        var h = new History();
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(0), Topic = "t", Partition = 0, Offset = 0, Value = Bytes("a") });
        // Offset 1 is never acknowledged.
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(1), Topic = "t", Partition = 0, Offset = 2, Value = Bytes("c") });

        var result = new LinearizabilityChecker().Check(h);
        Assert.False(result.IsValid);
        Assert.Contains(result.Violations,
            v => v is OffsetGapViolation g && g.GapStart == 0 && g.GapEnd == 2);
    }

    [Fact]
    public void LostWrite_IsCaught_WhenConsumerReadsPastIt()
    {
        // Produce at offsets 0, 1, 2 acknowledged, but the consumer sees only 0 and 2.
        // The checker flags offset 1 as potentially lost because reads went past it.
        var h = new History();
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(0), Topic = "t", Partition = 0, Offset = 0, Value = Bytes("a") });
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(1), Topic = "t", Partition = 0, Offset = 1, Value = Bytes("b") });
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(2), Topic = "t", Partition = 0, Offset = 2, Value = Bytes("c") });
        h.Record(new ConsumeOk { ClientId = 2, Timestamp = T(10), Topic = "t", Partition = 0, Offset = 0, Value = Bytes("a") });
        h.Record(new ConsumeOk { ClientId = 2, Timestamp = T(11), Topic = "t", Partition = 0, Offset = 2, Value = Bytes("c") });

        var result = new LinearizabilityChecker().Check(h);
        Assert.False(result.IsValid);
        Assert.Contains(result.Violations,
            v => v is PotentiallyLostWriteViolation lost && lost.Offset == 1);
    }

    [Fact]
    public void LostWrite_NotFlagged_WhenConsumerStopsBefore()
    {
        // Same as above but consumer stops at offset 0 — offset 1 may still be in
        // flight, which is indistinguishable from a lost write until we read further.
        var h = new History();
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(0), Topic = "t", Partition = 0, Offset = 0, Value = Bytes("a") });
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(1), Topic = "t", Partition = 0, Offset = 1, Value = Bytes("b") });
        h.Record(new ConsumeOk { ClientId = 2, Timestamp = T(10), Topic = "t", Partition = 0, Offset = 0, Value = Bytes("a") });

        var result = new LinearizabilityChecker().Check(h);
        // The gap check won't fire (offsets are contiguous 0..1). The lost-write
        // check won't fire either (consumer never read past 0).
        Assert.DoesNotContain(result.Violations, v => v is PotentiallyLostWriteViolation);
        Assert.DoesNotContain(result.Violations, v => v is OffsetGapViolation);
    }

    [Fact]
    public void MultiplePartitions_CheckedIndependently()
    {
        var h = new History();
        // Partition 0: clean
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(0), Topic = "t", Partition = 0, Offset = 0, Value = Bytes("a") });
        h.Record(new ConsumeOk { ClientId = 2, Timestamp = T(1), Topic = "t", Partition = 0, Offset = 0, Value = Bytes("a") });
        // Partition 1: collision
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(2), Topic = "t", Partition = 1, Offset = 0, Value = Bytes("a") });
        h.Record(new ProduceOk { ClientId = 3, Timestamp = T(3), Topic = "t", Partition = 1, Offset = 0, Value = Bytes("z") });

        var result = new LinearizabilityChecker().Check(h);
        Assert.False(result.IsValid);
        var violation = Assert.IsType<OffsetCollisionViolation>(result.Violations.Single());
        Assert.Equal(1, violation.Partition);
    }

    [Fact]
    public void ProduceFailAlone_IsIgnored()
    {
        // A produce that failed with no subsequent ack or consume is treated as
        // unknown-outcome — the checker cannot decide whether the record hit the log.
        var h = new History();
        h.Record(new ProduceInvoke { ClientId = 1, Timestamp = T(0), Topic = "t", Partition = 0, Value = Bytes("a") });
        h.Record(new ProduceFail { ClientId = 1, Timestamp = T(1), Topic = "t", Partition = 0, Value = Bytes("a"), Reason = "timeout" });

        var result = new LinearizabilityChecker().Check(h);
        Assert.True(result.IsValid);
    }

    [Fact]
    public void ConcurrentProducers_SameOffsetDifferentValues_IsCollision()
    {
        var h = new History();
        h.Record(new ProduceInvoke { ClientId = 1, Timestamp = T(0), Topic = "t", Partition = 0, Value = Bytes("a") });
        h.Record(new ProduceInvoke { ClientId = 2, Timestamp = T(1), Topic = "t", Partition = 0, Value = Bytes("b") });
        h.Record(new ProduceOk { ClientId = 1, Timestamp = T(2), Topic = "t", Partition = 0, Offset = 10, Value = Bytes("a") });
        h.Record(new ProduceOk { ClientId = 2, Timestamp = T(3), Topic = "t", Partition = 0, Offset = 10, Value = Bytes("b") });

        var result = new LinearizabilityChecker().Check(h);
        Assert.False(result.IsValid);
        Assert.Contains(result.Violations, v => v is OffsetCollisionViolation);
    }
}
