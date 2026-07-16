namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

/// <summary>
/// Pins the numeric values of the ErrorCode enum against the Kafka protocol
/// (broker codes) and librdkafka (negative local codes) — the values are wire
/// contract for drop-in Confluent.Kafka compatibility and must never drift.
/// </summary>
public class ErrorCodeTests
{
    [Theory]
    [InlineData(ErrorCode.NoError, 0)]
    [InlineData(ErrorCode.Unknown, -1)]
    [InlineData(ErrorCode.Local_NoOffset, -168)]
    [InlineData(ErrorCode.Local_TimedOut, -185)]
    [InlineData(ErrorCode.Local_QueueFull, -184)]
    [InlineData(ErrorCode.Local_InvalidArg, -186)]
    [InlineData(ErrorCode.Local_State, -172)]
    [InlineData(ErrorCode.Local_UnknownTopic, -188)]
    [InlineData(ErrorCode.Local_UnknownPartition, -190)]
    [InlineData(ErrorCode.Local_AllBrokersDown, -187)]
    [InlineData(ErrorCode.Local_Transport, -195)]
    [InlineData(ErrorCode.Local_Fatal, -150)]
    [InlineData(ErrorCode.OffsetOutOfRange, 1)]
    [InlineData(ErrorCode.InvalidMessage, 2)]
    [InlineData(ErrorCode.UnknownTopicOrPartition, 3)]
    [InlineData(ErrorCode.InvalidMessageSize, 4)]
    [InlineData(ErrorCode.LeaderNotAvailable, 5)]
    [InlineData(ErrorCode.NotLeaderForPartition, 6)]
    [InlineData(ErrorCode.RequestTimedOut, 7)]
    [InlineData(ErrorCode.BrokerNotAvailable, 8)]
    [InlineData(ErrorCode.ReplicaNotAvailable, 9)]
    [InlineData(ErrorCode.MessageSizeTooLarge, 10)]
    [InlineData(ErrorCode.GroupCoordinatorNotAvailable, 15)]
    [InlineData(ErrorCode.NotCoordinator, 16)]
    [InlineData(ErrorCode.IllegalGeneration, 22)]
    [InlineData(ErrorCode.InconsistentGroupProtocol, 23)]
    [InlineData(ErrorCode.UnknownMemberId, 25)]
    [InlineData(ErrorCode.InvalidSessionTimeout, 26)]
    [InlineData(ErrorCode.RebalanceInProgress, 27)]
    [InlineData(ErrorCode.TopicAlreadyExists, 36)]
    [InlineData(ErrorCode.InvalidPartitions, 37)]
    [InlineData(ErrorCode.InvalidReplicationFactor, 38)]
    [InlineData(ErrorCode.MemberIdRequired, 79)]
    public void ErrorCode_MatchesProtocolNumericValue(ErrorCode code, int expected)
    {
        Assert.Equal(expected, (int)code);
    }

    [Fact]
    public void BrokerCodes_ArePositive_LocalCodes_AreNegative()
    {
        Assert.True(new Error(ErrorCode.OffsetOutOfRange).IsBrokerError);
        Assert.False(new Error(ErrorCode.OffsetOutOfRange).IsLocalError);
        Assert.True(new Error(ErrorCode.Local_Transport).IsLocalError);
        Assert.False(new Error(ErrorCode.Local_Transport).IsBrokerError);
    }
}
