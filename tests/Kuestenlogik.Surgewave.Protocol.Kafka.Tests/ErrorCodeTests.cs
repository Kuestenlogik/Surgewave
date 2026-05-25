using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for ErrorCode enum values and their Kafka compatibility.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ErrorCodeTests
{
    [Fact]
    public void ErrorCode_None_IsZero()
    {
        Assert.Equal(0, (short)ErrorCode.None);
    }

    [Fact]
    public void ErrorCode_Unknown_IsNegativeOne()
    {
        Assert.Equal(-1, (short)ErrorCode.Unknown);
    }

    [Theory]
    [InlineData(ErrorCode.OffsetOutOfRange, 1)]
    [InlineData(ErrorCode.CorruptMessage, 2)]
    [InlineData(ErrorCode.UnknownTopicOrPartition, 3)]
    [InlineData(ErrorCode.InvalidFetchSize, 4)]
    [InlineData(ErrorCode.LeaderNotAvailable, 5)]
    [InlineData(ErrorCode.NotLeaderForPartition, 6)]
    [InlineData(ErrorCode.RequestTimedOut, 7)]
    [InlineData(ErrorCode.BrokerNotAvailable, 8)]
    [InlineData(ErrorCode.MessageTooLarge, 10)]
    [InlineData(ErrorCode.UnsupportedVersion, 35)]
    [InlineData(ErrorCode.TopicAlreadyExists, 36)]
    public void ErrorCode_NumericValues_MatchKafkaSpec(ErrorCode errorCode, short expectedValue)
    {
        Assert.Equal(expectedValue, (short)errorCode);
    }

    [Fact]
    public void ErrorCode_AllDefined_HaveUniqueValues()
    {
        // Get all defined error codes
        var values = Enum.GetValues<ErrorCode>();
        var distinctValues = values.Select(v => (short)v).Distinct().ToList();
        Assert.Equal(values.Length, distinctValues.Count);
    }

    [Fact]
    public void ErrorCode_CanBeRoundTripped_ThroughShort()
    {
        foreach (var errorCode in Enum.GetValues<ErrorCode>())
        {
            var asShort = (short)errorCode;
            var roundTripped = (ErrorCode)asShort;
            Assert.Equal(errorCode, roundTripped);
        }
    }

    [Theory]
    [InlineData(ErrorCode.TopicAuthorizationFailed)]
    [InlineData(ErrorCode.GroupAuthorizationFailed)]
    [InlineData(ErrorCode.ClusterAuthorizationFailed)]
    public void ErrorCode_AuthorizationErrors_AreDefined(ErrorCode errorCode)
    {
        Assert.True(Enum.IsDefined(errorCode));
    }

    [Theory]
    [InlineData(ErrorCode.InvalidProducerEpoch)]
    [InlineData(ErrorCode.DuplicateSequenceNumber)]
    [InlineData(ErrorCode.InvalidTxnState)]
    [InlineData(ErrorCode.TransactionalIdAuthorizationFailed)]
    public void ErrorCode_TransactionErrors_AreDefined(ErrorCode errorCode)
    {
        Assert.True(Enum.IsDefined(errorCode));
    }
}
