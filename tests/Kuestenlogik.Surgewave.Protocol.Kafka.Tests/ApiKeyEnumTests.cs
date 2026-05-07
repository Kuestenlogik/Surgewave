using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for ApiKey enum - ensuring Kafka wire protocol compatibility.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ApiKeyEnumTests
{
    [Theory]
    [InlineData(ApiKey.Produce, 0)]
    [InlineData(ApiKey.Fetch, 1)]
    [InlineData(ApiKey.ListOffsets, 2)]
    [InlineData(ApiKey.Metadata, 3)]
    [InlineData(ApiKey.OffsetCommit, 8)]
    [InlineData(ApiKey.OffsetFetch, 9)]
    [InlineData(ApiKey.FindCoordinator, 10)]
    [InlineData(ApiKey.JoinGroup, 11)]
    [InlineData(ApiKey.Heartbeat, 12)]
    [InlineData(ApiKey.LeaveGroup, 13)]
    [InlineData(ApiKey.SyncGroup, 14)]
    [InlineData(ApiKey.DescribeGroups, 15)]
    [InlineData(ApiKey.ListGroups, 16)]
    [InlineData(ApiKey.SaslHandshake, 17)]
    [InlineData(ApiKey.ApiVersions, 18)]
    [InlineData(ApiKey.CreateTopics, 19)]
    [InlineData(ApiKey.DeleteTopics, 20)]
    [InlineData(ApiKey.InitProducerId, 22)]
    [InlineData(ApiKey.AddPartitionsToTxn, 24)]
    [InlineData(ApiKey.EndTxn, 26)]
    [InlineData(ApiKey.DescribeAcls, 29)]
    [InlineData(ApiKey.CreateAcls, 30)]
    [InlineData(ApiKey.DeleteAcls, 31)]
    [InlineData(ApiKey.DescribeConfigs, 32)]
    [InlineData(ApiKey.AlterConfigs, 33)]
    [InlineData(ApiKey.SaslAuthenticate, 36)]
    [InlineData(ApiKey.CreatePartitions, 37)]
    [InlineData(ApiKey.DeleteGroups, 42)]
    [InlineData(ApiKey.IncrementalAlterConfigs, 44)]
    [InlineData(ApiKey.BrokerRegistration, 62)]
    [InlineData(ApiKey.BrokerHeartbeat, 63)]
    [InlineData(ApiKey.ConsumerGroupHeartbeat, 68)]
    [InlineData(ApiKey.DescribeTopicPartitions, 75)]
    public void ApiKey_NumericValues_MatchKafkaSpec(ApiKey apiKey, short expected)
    {
        Assert.Equal(expected, (short)apiKey);
    }

    [Fact]
    public void ApiKey_AllDefined_HaveUniqueValues()
    {
        var values = Enum.GetValues<ApiKey>();
        var distinctValues = values.Select(v => (short)v).Distinct().ToList();
        Assert.Equal(values.Length, distinctValues.Count);
    }

    [Fact]
    public void ApiKey_CanRoundTripThroughShort()
    {
        foreach (var apiKey in Enum.GetValues<ApiKey>())
        {
            var asShort = (short)apiKey;
            var roundTripped = (ApiKey)asShort;
            Assert.Equal(apiKey, roundTripped);
        }
    }
}
