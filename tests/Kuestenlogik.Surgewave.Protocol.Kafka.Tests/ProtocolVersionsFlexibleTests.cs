using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// The first flexible version of every API the broker advertises, as Kafka's message schemas declare it
/// (<c>flexibleVersions</c> in clients/src/main/resources/common/message/*Request.json, Kafka 46dc4b51f7).
/// <see cref="ProtocolVersions.IsFlexible"/> decides whether the request header carries tagged fields; for
/// twenty advertised APIs it answered "never", so the header's tagged-fields byte was read as the first body
/// field. A Java 4.1 client could not describe a transaction or a topic's partitions, and DescribeCluster was
/// misread silently. -1: the API has no flexible version.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ProtocolVersionsFlexibleTests
{
    [Theory]
    [InlineData(ApiKey.Produce, 9)]
    [InlineData(ApiKey.Fetch, 12)]
    [InlineData(ApiKey.ListOffsets, 6)]
    [InlineData(ApiKey.Metadata, 9)]
    [InlineData(ApiKey.OffsetCommit, 8)]
    [InlineData(ApiKey.OffsetFetch, 6)]
    [InlineData(ApiKey.FindCoordinator, 3)]
    [InlineData(ApiKey.JoinGroup, 6)]
    [InlineData(ApiKey.Heartbeat, 4)]
    [InlineData(ApiKey.LeaveGroup, 4)]
    [InlineData(ApiKey.SyncGroup, 4)]
    [InlineData(ApiKey.DescribeGroups, 5)]
    [InlineData(ApiKey.ListGroups, 3)]
    [InlineData(ApiKey.DeleteGroups, 2)]
    [InlineData(ApiKey.SaslHandshake, -1)]
    [InlineData(ApiKey.ApiVersions, 3)]
    [InlineData(ApiKey.SaslAuthenticate, 2)]
    [InlineData(ApiKey.CreateTopics, 5)]
    [InlineData(ApiKey.DeleteTopics, 4)]
    [InlineData(ApiKey.DeleteRecords, 2)]
    [InlineData(ApiKey.CreatePartitions, 2)]
    [InlineData(ApiKey.DescribeTopicPartitions, 0)]
    [InlineData(ApiKey.InitProducerId, 2)]
    [InlineData(ApiKey.AddPartitionsToTxn, 3)]
    [InlineData(ApiKey.AddOffsetsToTxn, 3)]
    [InlineData(ApiKey.EndTxn, 3)]
    [InlineData(ApiKey.TxnOffsetCommit, 3)]
    [InlineData(ApiKey.DescribeTransactions, 0)]
    [InlineData(ApiKey.ListTransactions, 0)]
    [InlineData(ApiKey.DescribeConfigs, 4)]
    [InlineData(ApiKey.AlterConfigs, 2)]
    [InlineData(ApiKey.IncrementalAlterConfigs, 1)]
    [InlineData(ApiKey.ListConfigResources, 0)]
    [InlineData(ApiKey.DescribeAcls, 2)]
    [InlineData(ApiKey.CreateAcls, 2)]
    [InlineData(ApiKey.DeleteAcls, 2)]
    [InlineData(ApiKey.CreateDelegationToken, 2)]
    [InlineData(ApiKey.RenewDelegationToken, 2)]
    [InlineData(ApiKey.ExpireDelegationToken, 2)]
    [InlineData(ApiKey.DescribeDelegationToken, 2)]
    [InlineData(ApiKey.DescribeClientQuotas, 1)]
    [InlineData(ApiKey.AlterClientQuotas, 1)]
    [InlineData(ApiKey.DescribeUserScramCredentials, 0)]
    [InlineData(ApiKey.AlterUserScramCredentials, 0)]
    [InlineData(ApiKey.DescribeCluster, 0)]
    [InlineData(ApiKey.DescribeProducers, 0)]
    [InlineData(ApiKey.DescribeLogDirs, 2)]
    [InlineData(ApiKey.AlterReplicaLogDirs, 2)]
    [InlineData(ApiKey.ElectLeaders, 2)]
    [InlineData(ApiKey.AlterPartitionReassignments, 0)]
    [InlineData(ApiKey.ListPartitionReassignments, 0)]
    [InlineData(ApiKey.OffsetDelete, 1)]
    [InlineData(ApiKey.ConsumerGroupHeartbeat, 0)]
    [InlineData(ApiKey.ConsumerGroupDescribe, 0)]
    [InlineData(ApiKey.GetTelemetrySubscriptions, 0)]
    [InlineData(ApiKey.PushTelemetry, 0)]
    [InlineData(ApiKey.ShareGroupHeartbeat, 0)]
    [InlineData(ApiKey.ShareGroupDescribe, 0)]
    [InlineData(ApiKey.ShareFetch, 0)]
    [InlineData(ApiKey.ShareAcknowledge, 0)]
    [InlineData(ApiKey.DescribeShareGroupOffsets, 0)]
    [InlineData(ApiKey.AlterShareGroupOffsets, 0)]
    [InlineData(ApiKey.StreamsGroupHeartbeat, 0)]
    [InlineData(ApiKey.StreamsGroupDescribe, 0)]
    public void IsFlexible_FromTheVersionKafkaDeclares(ApiKey apiKey, int firstFlexibleVersion)
    {
        if (firstFlexibleVersion < 0)
        {
            Assert.False(ProtocolVersions.IsFlexible(apiKey, 0));
            return;
        }

        Assert.True(ProtocolVersions.IsFlexible(apiKey, (short)firstFlexibleVersion));
        if (firstFlexibleVersion > 0)
            Assert.False(ProtocolVersions.IsFlexible(apiKey, (short)(firstFlexibleVersion - 1)));
    }
}
