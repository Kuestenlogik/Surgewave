using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Error codes go on the wire as bare numbers, so their values ARE the contract — a name that reads
/// correctly in C# while carrying the wrong number is invisible in every test that compares enum
/// members to enum members.
///
/// <para>That is exactly how <c>OutOfOrderSequenceNumber</c> sat on 44 — Kafka's
/// <c>POLICY_VIOLATION</c> — with 45 unused. Nothing hung, because both codes are fatal to a client,
/// but an idempotent producer that broke its sequence was told it had violated a policy, and any
/// client branching on 45 never saw it. These are the values from Apache Kafka's
/// <c>org.apache.kafka.common.protocol.Errors</c>.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ErrorCodeWireValueTests
{
    [Theory]
    // The neighbourhood the mis-numbering was in — every one of these is a code a producer can see.
    [InlineData(ErrorCode.InvalidRequest, 42)]
    [InlineData(ErrorCode.UnsupportedForMessageFormat, 43)]
    [InlineData(ErrorCode.PolicyViolation, 44)]
    [InlineData(ErrorCode.OutOfOrderSequenceNumber, 45)]
    [InlineData(ErrorCode.DuplicateSequenceNumber, 46)]
    [InlineData(ErrorCode.InvalidProducerEpoch, 47)]
    [InlineData(ErrorCode.UnknownProducerId, 59)]
    // The codes the durability work put on the wire (#122).
    [InlineData(ErrorCode.NotEnoughReplicas, 19)]
    [InlineData(ErrorCode.NotEnoughReplicasAfterAppend, 20)]
    [InlineData(ErrorCode.InvalidRequiredAcks, 21)]
    // The second instance of exactly the defect this file was written for: SnapshotNotFound sat on
    // 87, which is Kafka's INVALID_RECORD, while 98 — the real SNAPSHOT_NOT_FOUND — was unused. The
    // Raft snapshot-fetch path put 87 on the wire, so every Kafka-compatible client decoded "this
    // record failed validation" from a response about a missing snapshot. Uncaught because neither
    // code was listed here.
    [InlineData(ErrorCode.InvalidRecord, 87)]
    [InlineData(ErrorCode.SnapshotNotFound, 98)]
    // And the everyday ones, so a careless renumbering anywhere in the block is caught.
    [InlineData(ErrorCode.None, 0)]
    [InlineData(ErrorCode.OffsetOutOfRange, 1)]
    [InlineData(ErrorCode.CorruptMessage, 2)]
    [InlineData(ErrorCode.UnknownTopicOrPartition, 3)]
    [InlineData(ErrorCode.NotLeaderForPartition, 6)]
    [InlineData(ErrorCode.RequestTimedOut, 7)]
    public void ErrorCode_HasItsKafkaWireValue(ErrorCode code, short expected)
    {
        Assert.Equal(expected, (short)code);
    }

    [Fact]
    public void NoTwoErrorCodes_ShareAValue()
    {
        // The mis-numbering was survivable only because POLICY_VIOLATION did not exist here yet. A
        // collision is otherwise silent: the compiler accepts duplicate enum values, and whichever
        // name the switch happens to match wins.
        var values = Enum.GetValues<ErrorCode>();
        var names = Enum.GetNames<ErrorCode>();

        var duplicates = values
            .Select((value, index) => (value, name: names[index]))
            .GroupBy(entry => entry.value)
            .Where(group => group.Count() > 1)
            .Select(group => $"{(short)group.Key}: {string.Join(", ", group.Select(entry => entry.name))}")
            .ToList();

        Assert.True(duplicates.Count == 0,
            "error codes sharing a wire value: " + string.Join(" | ", duplicates));
    }
}
