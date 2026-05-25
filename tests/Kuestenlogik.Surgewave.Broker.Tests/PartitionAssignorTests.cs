using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Broker.Native.Assignors;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for partition assignment strategies.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PartitionAssignorTests
{
    #region RangeAssignor Tests

    [Fact]
    public void RangeAssignor_Name_IsRange()
    {
        var assignor = new RangeAssignor();
        Assert.Equal("range", assignor.Name);
    }

    [Fact]
    public void RangeAssignor_SingleMember_GetsAllPartitions()
    {
        var assignor = new RangeAssignor();
        var topics = new List<string> { "topic1" };
        var partitionCounts = new Dictionary<string, int> { ["topic1"] = 3 };
        var members = new List<MemberSubscription>
        {
            new("member1", ["topic1"], [])
        };

        var result = assignor.Assign(topics, partitionCounts, members);

        Assert.Equal(3, result["member1"].Count);
    }

    [Fact]
    public void RangeAssignor_TwoMembers_EvenSplit()
    {
        var assignor = new RangeAssignor();
        var topics = new List<string> { "topic1" };
        var partitionCounts = new Dictionary<string, int> { ["topic1"] = 4 };
        var members = new List<MemberSubscription>
        {
            new("member1", ["topic1"], []),
            new("member2", ["topic1"], [])
        };

        var result = assignor.Assign(topics, partitionCounts, members);

        Assert.Equal(2, result["member1"].Count);
        Assert.Equal(2, result["member2"].Count);
    }

    [Fact]
    public void RangeAssignor_UnevenPartitions_FirstMembersGetExtra()
    {
        var assignor = new RangeAssignor();
        var topics = new List<string> { "topic1" };
        var partitionCounts = new Dictionary<string, int> { ["topic1"] = 5 };
        var members = new List<MemberSubscription>
        {
            new("member1", ["topic1"], []),
            new("member2", ["topic1"], [])
        };

        var result = assignor.Assign(topics, partitionCounts, members);

        Assert.Equal(3, result["member1"].Count);
        Assert.Equal(2, result["member2"].Count);
    }

    #endregion

    #region RoundRobinAssignor Tests

    [Fact]
    public void RoundRobinAssignor_Name_IsRoundRobin()
    {
        var assignor = new RoundRobinAssignor();
        Assert.Equal("roundrobin", assignor.Name);
    }

    [Fact]
    public void RoundRobinAssignor_TwoMembers_AlternatingAssignment()
    {
        var assignor = new RoundRobinAssignor();
        var topics = new List<string> { "topic1" };
        var partitionCounts = new Dictionary<string, int> { ["topic1"] = 4 };
        var members = new List<MemberSubscription>
        {
            new("member1", ["topic1"], []),
            new("member2", ["topic1"], [])
        };

        var result = assignor.Assign(topics, partitionCounts, members);

        Assert.Equal(2, result["member1"].Count);
        Assert.Equal(2, result["member2"].Count);
    }

    #endregion

    #region StickyAssignor Tests

    [Fact]
    public void StickyAssignor_Name_IsSticky()
    {
        var assignor = new StickyAssignor();
        Assert.Equal("sticky", assignor.Name);
    }

    [Fact]
    public void StickyAssignor_EvenDistribution()
    {
        var assignor = new StickyAssignor();
        var topics = new List<string> { "topic1" };
        var partitionCounts = new Dictionary<string, int> { ["topic1"] = 6 };
        var members = new List<MemberSubscription>
        {
            new("member1", ["topic1"], []),
            new("member2", ["topic1"], []),
            new("member3", ["topic1"], [])
        };

        var result = assignor.Assign(topics, partitionCounts, members);

        Assert.Equal(2, result["member1"].Count);
        Assert.Equal(2, result["member2"].Count);
        Assert.Equal(2, result["member3"].Count);
    }

    #endregion

    #region CooperativeStickyAssignor Tests

    [Fact]
    public void CooperativeStickyAssignor_Name_IsCooperativeSticky()
    {
        var assignor = new CooperativeStickyAssignor();
        Assert.Equal("cooperative-sticky", assignor.Name);
    }

    #endregion

    #region PartitionAssignorFactory Tests

    [Theory]
    [InlineData("range", typeof(RangeAssignor))]
    [InlineData("roundrobin", typeof(RoundRobinAssignor))]
    [InlineData("sticky", typeof(StickyAssignor))]
    [InlineData("cooperative-sticky", typeof(CooperativeStickyAssignor))]
    public void PartitionAssignorFactory_GetAssignor_ReturnsCorrectType(string name, Type expectedType)
    {
        var assignor = PartitionAssignorFactory.GetAssignor(name);
        Assert.IsType(expectedType, assignor);
    }

    [Fact]
    public void PartitionAssignorFactory_GetAssignor_CaseInsensitive()
    {
        var assignor1 = PartitionAssignorFactory.GetAssignor("RANGE");
        var assignor2 = PartitionAssignorFactory.GetAssignor("Range");

        Assert.IsType<RangeAssignor>(assignor1);
        Assert.IsType<RangeAssignor>(assignor2);
    }

    [Fact]
    public void PartitionAssignorFactory_AvailableStrategies_ContainsAllStrategies()
    {
        var strategies = PartitionAssignorFactory.AvailableStrategies;

        Assert.Contains("range", strategies);
        Assert.Contains("roundrobin", strategies);
        Assert.Contains("sticky", strategies);
        Assert.Contains("cooperative-sticky", strategies);
    }

    #endregion

    #region AssignedPartition Tests

    [Fact]
    public void AssignedPartition_Equality_SameValues_AreEqual()
    {
        var p1 = new AssignedPartition("topic", 1);
        var p2 = new AssignedPartition("topic", 1);

        Assert.Equal(p1, p2);
    }

    [Fact]
    public void AssignedPartition_CanBeUsedInHashSet()
    {
        var set = new HashSet<AssignedPartition>
        {
            new("topic1", 0),
            new("topic1", 1),
            new("topic2", 0)
        };

        var added = set.Add(new AssignedPartition("topic1", 0));

        Assert.False(added);
        Assert.Equal(3, set.Count);
    }

    #endregion

    #region SubscriptionMetadata Tests

    [Fact]
    public void SubscriptionMetadata_Serialize_Deserialize_RoundTrip()
    {
        var topics = new List<string> { "topic1", "topic2" };
        var userData = new byte[] { 1, 2, 3, 4, 5 };

        var serialized = SubscriptionMetadata.Serialize(topics, userData);
        var (deserializedTopics, deserializedUserData) = SubscriptionMetadata.Deserialize(serialized);

        Assert.Equal(topics, deserializedTopics);
        Assert.Equal(userData, deserializedUserData);
    }

    #endregion

    #region AssignmentData Tests

    [Fact]
    public void AssignmentData_Serialize_Deserialize_RoundTrip()
    {
        var partitions = new List<AssignedPartition>
        {
            new("topic1", 0),
            new("topic1", 1),
            new("topic2", 0)
        };
        var userData = new byte[] { 10, 20 };

        var serialized = AssignmentData.Serialize(partitions, userData);
        var (deserializedPartitions, deserializedUserData) = AssignmentData.Deserialize(serialized);

        Assert.Equal(3, deserializedPartitions.Count);
        Assert.Contains(new AssignedPartition("topic1", 0), deserializedPartitions);
        Assert.Equal(userData, deserializedUserData);
    }

    #endregion
}
