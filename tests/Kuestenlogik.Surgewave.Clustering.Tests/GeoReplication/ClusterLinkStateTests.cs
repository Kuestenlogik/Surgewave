using Kuestenlogik.Surgewave.Clustering.GeoReplication;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.GeoReplication;

[Trait("Category", TestCategories.Unit)]
public sealed class ClusterLinkStateTests
{
    [Fact]
    public void EnumValues()
    {
        // Assert
        Assert.Equal(0, (int)ClusterLinkState.Initializing);
        Assert.Equal(1, (int)ClusterLinkState.Active);
        Assert.Equal(2, (int)ClusterLinkState.Paused);
        Assert.Equal(3, (int)ClusterLinkState.Error);
    }

    [Fact]
    public void ClusterLinkStatus_Properties()
    {
        // Act
        var status = new ClusterLinkStatus
        {
            LinkId = "link-1",
            State = ClusterLinkState.Active,
            RemoteClusterId = "remote-cluster",
            MirroredTopicCount = 5,
            TotalLagMessages = 1000,
            LastFetchTimestamp = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            ErrorMessage = null
        };

        // Assert
        Assert.Equal("link-1", status.LinkId);
        Assert.Equal(ClusterLinkState.Active, status.State);
        Assert.Equal("remote-cluster", status.RemoteClusterId);
        Assert.Equal(5, status.MirroredTopicCount);
        Assert.Equal(1000, status.TotalLagMessages);
        Assert.NotNull(status.LastFetchTimestamp);
        Assert.Null(status.ErrorMessage);
    }

    [Fact]
    public void MirrorTopicState_Properties()
    {
        // Act
        var state = new MirrorTopicState
        {
            SourceTopic = "orders",
            LinkId = "link-1",
            PartitionCount = 6
        };

        // Assert
        Assert.Equal("orders", state.SourceTopic);
        Assert.Equal("link-1", state.LinkId);
        Assert.Equal(6, state.PartitionCount);
        Assert.True(state.IsReadOnly); // default
        Assert.NotNull(state.ReplicationLag);
        Assert.Empty(state.ReplicationLag);
        Assert.NotNull(state.LastSyncedOffset);
        Assert.Empty(state.LastSyncedOffset);
    }
}
