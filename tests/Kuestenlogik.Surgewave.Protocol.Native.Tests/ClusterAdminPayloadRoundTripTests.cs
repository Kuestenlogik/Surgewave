using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — Cluster admin payloads (reassignment + log
/// integrity verification). Same Read/Write/EstimateSize pattern as the
/// earlier <see cref="ClusterPayloadRoundTripTests"/>; pinning these
/// catches framing regressions on the admin-RPC surface used by the
/// AlterPartitionReassignments / ListPartitionReassignments / VerifyLogIntegrity
/// flows.
/// </summary>
public sealed class ClusterAdminPayloadRoundTripTests
{
    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // Reassignment payloads
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void PartitionReassignmentRequestPayload_RoundTrip_PreservesReplicas()
    {
        var original = new PartitionReassignmentRequestPayload
        {
            Topic = "orders",
            Partition = 7,
            Replicas = new[] { 1, 2, 3 },
        };

        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return PartitionReassignmentRequestPayload.Read(ref r); });

        Assert.Equal("orders", parsed.Topic);
        Assert.Equal(7, parsed.Partition);
        Assert.Equal(new[] { 1, 2, 3 }, parsed.Replicas);
    }

    [Fact]
    public void PartitionReassignmentRequestPayload_EmptyReplicas_RoundTrips()
    {
        // Empty replicas list = cancel reassignment (upstream semantics).
        var original = new PartitionReassignmentRequestPayload
        {
            Topic = "events",
            Partition = 0,
            Replicas = Array.Empty<int>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return PartitionReassignmentRequestPayload.Read(ref r); });
        Assert.Empty(parsed.Replicas);
    }

    [Fact]
    public void AlterReassignmentsRequestPayload_RoundTrip_AggregatesPartitions()
    {
        var original = new AlterReassignmentsRequestPayload
        {
            Reassignments = new[]
            {
                new PartitionReassignmentRequestPayload { Topic = "t1", Partition = 0, Replicas = new[] { 1, 2 } },
                new PartitionReassignmentRequestPayload { Topic = "t1", Partition = 1, Replicas = new[] { 2, 3 } },
                new PartitionReassignmentRequestPayload { Topic = "t2", Partition = 0, Replicas = new[] { 3 } },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return AlterReassignmentsRequestPayload.Read(ref r); });

        Assert.Equal(3, parsed.Reassignments.Count);
        Assert.Equal("t2", parsed.Reassignments[2].Topic);
        Assert.Equal(new[] { 2, 3 }, parsed.Reassignments[1].Replicas);
    }

    [Fact]
    public void AlterReassignmentsResponsePayload_RoundTrip_PreservesBothFields()
    {
        var original = new AlterReassignmentsResponsePayload { Success = true, PartitionCount = 12 };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return AlterReassignmentsResponsePayload.Read(ref r); });
        Assert.True(parsed.Success);
        Assert.Equal(12, parsed.PartitionCount);
    }

    [Fact]
    public void PartitionReassignmentStatusPayload_RoundTrip_PreservesEnumAndProgress()
    {
        var original = new PartitionReassignmentStatusPayload
        {
            Topic = "events",
            Partition = 4,
            Status = ReassignmentStatus.Syncing,
            ProgressPercent = 73,
            OriginalReplicas = new[] { 1, 2 },
            TargetReplicas = new[] { 2, 3 },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return PartitionReassignmentStatusPayload.Read(ref r); });

        Assert.Equal(ReassignmentStatus.Syncing, parsed.Status);
        Assert.Equal(73, parsed.ProgressPercent);
        Assert.Equal(new[] { 1, 2 }, parsed.OriginalReplicas);
        Assert.Equal(new[] { 2, 3 }, parsed.TargetReplicas);
    }

    [Theory]
    [InlineData(ReassignmentStatus.Pending)]
    [InlineData(ReassignmentStatus.Adding)]
    [InlineData(ReassignmentStatus.Completed)]
    [InlineData(ReassignmentStatus.Failed)]
    [InlineData(ReassignmentStatus.Cancelled)]
    public void PartitionReassignmentStatusPayload_RoundTrips_EveryEnumValue(ReassignmentStatus status)
    {
        // Pin every enum value through the byte-cast. A future re-numbering
        // would silently swap statuses without this guard.
        var original = new PartitionReassignmentStatusPayload
        {
            Topic = "t",
            Partition = 0,
            Status = status,
            ProgressPercent = 0,
            OriginalReplicas = Array.Empty<int>(),
            TargetReplicas = Array.Empty<int>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return PartitionReassignmentStatusPayload.Read(ref r); });
        Assert.Equal(status, parsed.Status);
    }

    [Fact]
    public void ListReassignmentsPayload_RoundTrip_AggregatesStatuses()
    {
        var original = new ListReassignmentsPayload
        {
            Reassignments = new[]
            {
                new PartitionReassignmentStatusPayload
                {
                    Topic = "t1", Partition = 0, Status = ReassignmentStatus.Completing, ProgressPercent = 95,
                    OriginalReplicas = new[] { 1 }, TargetReplicas = new[] { 2 },
                },
                new PartitionReassignmentStatusPayload
                {
                    Topic = "t1", Partition = 1, Status = ReassignmentStatus.Failed, ProgressPercent = 0,
                    OriginalReplicas = new[] { 1, 2 }, TargetReplicas = new[] { 2, 3 },
                },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ListReassignmentsPayload.Read(ref r); });

        Assert.Equal(2, parsed.Reassignments.Count);
        Assert.Equal(ReassignmentStatus.Failed, parsed.Reassignments[1].Status);
    }

    // ───────────────────────────────────────────────────────────────
    // Log integrity verification payloads
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void VerifyLogIntegrityRequestPayload_RoundTrip_PreservesAllFields()
    {
        var original = new VerifyLogIntegrityRequestPayload
        {
            Topic = "audit-events",
            Partition = 3,
            MaxCorruptedBatches = 100,
            IncludeDetails = true,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return VerifyLogIntegrityRequestPayload.Read(ref r); });

        Assert.Equal("audit-events", parsed.Topic);
        Assert.Equal(3, parsed.Partition);
        Assert.Equal(100, parsed.MaxCorruptedBatches);
        Assert.True(parsed.IncludeDetails);
    }

    [Fact]
    public void VerifyLogIntegrityRequestPayload_AllTopicsAllPartitions_RoundTrips()
    {
        // Per the doc-comments on the struct, Topic="" / Partition=-1 /
        // MaxCorruptedBatches=0 mean "all topics" / "all partitions" /
        // "no limit". Pin that wire shape.
        var original = new VerifyLogIntegrityRequestPayload
        {
            Topic = "",
            Partition = -1,
            MaxCorruptedBatches = 0,
            IncludeDetails = false,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return VerifyLogIntegrityRequestPayload.Read(ref r); });

        Assert.Equal("", parsed.Topic);
        Assert.Equal(-1, parsed.Partition);
        Assert.Equal(0, parsed.MaxCorruptedBatches);
        Assert.False(parsed.IncludeDetails);
    }

    [Fact]
    public void CorruptedBatchDetailPayload_RoundTrip_PreservesCrcsAndOffsets()
    {
        var original = new CorruptedBatchDetailPayload
        {
            Topic = "events",
            Partition = 2,
            BaseOffset = 1_000_000_007L,
            ExpectedCrc = 0xDEADBEEF,
            ActualCrc = 0xCAFEBABE,
            BatchLength = 4096,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CorruptedBatchDetailPayload.Read(ref r); });

        Assert.Equal(1_000_000_007L, parsed.BaseOffset);
        Assert.Equal(0xDEADBEEF, parsed.ExpectedCrc);
        Assert.Equal(0xCAFEBABE, parsed.ActualCrc);
        Assert.Equal(4096, parsed.BatchLength);
    }

    [Fact]
    public void VerifyLogIntegrityResponsePayload_RoundTrip_PreservesAggregatesAndDetails()
    {
        var original = new VerifyLogIntegrityResponsePayload
        {
            BatchesChecked = 100_000,
            CorruptedBatches = 2,
            BytesChecked = 1_000_000_000L,
            CorruptedBytes = 8192L,
            PartitionsChecked = 64,
            DurationMs = 12_500,
            TopicsVerified = new[] { "events", "audit" },
            CorruptedBatchDetails = new[]
            {
                new CorruptedBatchDetailPayload
                {
                    Topic = "events", Partition = 5, BaseOffset = 999, ExpectedCrc = 1, ActualCrc = 2, BatchLength = 4096,
                },
                new CorruptedBatchDetailPayload
                {
                    Topic = "audit", Partition = 0, BaseOffset = 500, ExpectedCrc = 3, ActualCrc = 4, BatchLength = 4096,
                },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return VerifyLogIntegrityResponsePayload.Read(ref r); });

        Assert.Equal(100_000, parsed.BatchesChecked);
        Assert.Equal(2, parsed.CorruptedBatches);
        Assert.Equal(1_000_000_000L, parsed.BytesChecked);
        Assert.Equal(12_500L, parsed.DurationMs);
        Assert.Equal(new[] { "events", "audit" }, parsed.TopicsVerified);
        Assert.Equal(2, parsed.CorruptedBatchDetails.Count);
        Assert.Equal("audit", parsed.CorruptedBatchDetails[1].Topic);
        Assert.Equal(4u, parsed.CorruptedBatchDetails[1].ActualCrc);
    }

    [Fact]
    public void VerifyLogIntegrityResponsePayload_NoCorruption_RoundTrips()
    {
        // Healthy-cluster path — empty detail/topic lists.
        var original = new VerifyLogIntegrityResponsePayload
        {
            BatchesChecked = 50_000,
            CorruptedBatches = 0,
            BytesChecked = 500_000_000L,
            CorruptedBytes = 0,
            PartitionsChecked = 32,
            DurationMs = 4_000,
            TopicsVerified = Array.Empty<string>(),
            CorruptedBatchDetails = Array.Empty<CorruptedBatchDetailPayload>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return VerifyLogIntegrityResponsePayload.Read(ref r); });

        Assert.Equal(0, parsed.CorruptedBatches);
        Assert.Empty(parsed.TopicsVerified);
        Assert.Empty(parsed.CorruptedBatchDetails);
    }
}
