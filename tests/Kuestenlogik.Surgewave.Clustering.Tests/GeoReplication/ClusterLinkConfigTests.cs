using Kuestenlogik.Surgewave.Clustering.GeoReplication;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.GeoReplication;

[Trait("Category", TestCategories.Unit)]
public sealed class ClusterLinkConfigTests
{
    [Fact]
    public void DefaultValues()
    {
        // Act
        var config = new ClusterLinkConfig
        {
            LinkId = "link-1",
            RemoteBootstrapServers = "remote:9092"
        };

        // Assert
        Assert.Equal(500, config.FetchIntervalMs);
        Assert.Equal(1024 * 1024, config.FetchMaxBytes);
        Assert.Equal(4, config.FetcherThreads);
        Assert.Equal(30_000, config.MetadataSyncIntervalMs);
        Assert.Equal(10_000, config.ConsumerOffsetSyncIntervalMs);
        Assert.Null(config.RemoteClusterId);
    }

    [Fact]
    public void TopicFilter_DefaultMatchAll()
    {
        // Act
        var config = new ClusterLinkConfig
        {
            LinkId = "link-1",
            RemoteBootstrapServers = "remote:9092"
        };

        // Assert
        Assert.Equal(".*", config.TopicFilter);
    }

    [Fact]
    public void TopicExcludes_DefaultEmpty()
    {
        // Act
        var config = new ClusterLinkConfig
        {
            LinkId = "link-1",
            RemoteBootstrapServers = "remote:9092"
        };

        // Assert
        Assert.Empty(config.TopicExcludes);
    }

    [Fact]
    public void SyncDefaults()
    {
        // Act
        var config = new ClusterLinkConfig
        {
            LinkId = "link-1",
            RemoteBootstrapServers = "remote:9092"
        };

        // Assert
        Assert.True(config.SyncConsumerOffsets);
        Assert.True(config.SyncTopicConfigs);
        Assert.False(config.SyncAcls);
    }
}
