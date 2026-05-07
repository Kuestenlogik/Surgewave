using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Gateway.Tests;

public class ClusterRegistryTests : IAsyncDisposable
{
    private ClusterRegistry? _registry;

    public async ValueTask DisposeAsync()
    {
        if (_registry != null)
        {
            await _registry.DisposeAsync();
        }
    }

    [Fact]
    public void MultiClusterConfig_CreatesAllClients()
    {
        // Arrange
        var config = new GatewayConfig
        {
            DefaultCluster = "cluster-a",
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 },
                ["cluster-b"] = new ClusterConfig { BrokerHost = "host-b", BrokerPort = 9093 }
            }
        };

        // Act
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Assert
        Assert.Equal(2, _registry.ClusterIds.Count());
        Assert.Contains("cluster-a", _registry.ClusterIds);
        Assert.Contains("cluster-b", _registry.ClusterIds);
    }

    [Fact]
    public void MultiClusterConfig_SetsDefaultCluster()
    {
        // Arrange
        var config = new GatewayConfig
        {
            DefaultCluster = "cluster-b",
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 },
                ["cluster-b"] = new ClusterConfig { BrokerHost = "host-b", BrokerPort = 9093 }
            }
        };

        // Act
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Assert
        Assert.Equal("cluster-b", _registry.DefaultClusterId);
    }

    [Fact]
    public void MultiClusterConfig_WithoutExplicitDefaultCluster_UsesConfigDefault()
    {
        // Arrange - GatewayConfig.DefaultCluster defaults to "surgewave-cluster"
        var config = new GatewayConfig
        {
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["surgewave-cluster"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 }
            }
        };

        // Act
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Assert - Uses the default value from GatewayConfig
        Assert.Equal("surgewave-cluster", _registry.DefaultClusterId);
    }

    [Fact]
    public void GetClient_WithClusterId_ReturnsCorrectClient()
    {
        // Arrange
        var config = new GatewayConfig
        {
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 },
                ["cluster-b"] = new ClusterConfig { BrokerHost = "host-b", BrokerPort = 9093 }
            }
        };
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Act
        var clientA = _registry.GetClient("cluster-a");
        var clientB = _registry.GetClient("cluster-b");

        // Assert
        Assert.NotNull(clientA);
        Assert.NotNull(clientB);
        Assert.NotSame(clientA, clientB);
    }

    [Fact]
    public void GetClient_WithNull_ReturnsDefaultClient()
    {
        // Arrange
        var config = new GatewayConfig
        {
            DefaultCluster = "cluster-a",
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 },
                ["cluster-b"] = new ClusterConfig { BrokerHost = "host-b", BrokerPort = 9093 }
            }
        };
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Act
        var defaultClient = _registry.GetClient(null);
        var explicitClient = _registry.GetClient("cluster-a");

        // Assert
        Assert.Same(defaultClient, explicitClient);
    }

    [Fact]
    public void GetClient_WithEmptyString_ReturnsDefaultClient()
    {
        // Arrange
        var config = new GatewayConfig
        {
            DefaultCluster = "cluster-a",
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 }
            }
        };
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Act
        var defaultClient = _registry.GetClient("");
        var explicitClient = _registry.GetClient("cluster-a");

        // Assert
        Assert.Same(defaultClient, explicitClient);
    }

    [Fact]
    public void GetClient_WithUnknownCluster_ThrowsKeyNotFoundException()
    {
        // Arrange
        var config = new GatewayConfig
        {
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 }
            }
        };
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Act & Assert
        var ex = Assert.Throws<KeyNotFoundException>(() => _registry.GetClient("unknown-cluster"));
        Assert.Contains("unknown-cluster", ex.Message);
        Assert.Contains("cluster-a", ex.Message); // Should list available clusters
    }

    [Fact]
    public void GetClient_IsCaseInsensitive()
    {
        // Arrange
        var config = new GatewayConfig
        {
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["Cluster-A"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 }
            }
        };
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Act
        var client1 = _registry.GetClient("cluster-a");
        var client2 = _registry.GetClient("CLUSTER-A");
        var client3 = _registry.GetClient("Cluster-A");

        // Assert
        Assert.Same(client1, client2);
        Assert.Same(client2, client3);
    }

    [Fact]
    public void TryGetClient_WithValidCluster_ReturnsTrue()
    {
        // Arrange
        var config = new GatewayConfig
        {
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 }
            }
        };
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Act
        var result = _registry.TryGetClient("cluster-a", out var client);

        // Assert
        Assert.True(result);
        Assert.NotNull(client);
    }

    [Fact]
    public void TryGetClient_WithInvalidCluster_ReturnsFalse()
    {
        // Arrange
        var config = new GatewayConfig
        {
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 }
            }
        };
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Act
        var result = _registry.TryGetClient("unknown", out var client);

        // Assert
        Assert.False(result);
        Assert.Null(client);
    }

    [Fact]
    public void TryGetClient_WithNull_ReturnsDefaultClient()
    {
        // Arrange
        var config = new GatewayConfig
        {
            DefaultCluster = "cluster-a",
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 }
            }
        };
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Act
        var result = _registry.TryGetClient(null, out var client);

        // Assert
        Assert.True(result);
        Assert.NotNull(client);
    }

    [Fact]
    public void GetConfig_WithValidCluster_ReturnsConfig()
    {
        // Arrange
        var config = new GatewayConfig
        {
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 }
            }
        };
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Act
        var clusterConfig = _registry.GetConfig("cluster-a");

        // Assert
        Assert.NotNull(clusterConfig);
        Assert.Equal("host-a", clusterConfig.BrokerHost);
        Assert.Equal(9092, clusterConfig.BrokerPort);
    }

    [Fact]
    public void GetConfig_WithNull_ReturnsDefaultConfig()
    {
        // Arrange
        var config = new GatewayConfig
        {
            DefaultCluster = "cluster-a",
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 }
            }
        };
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Act
        var clusterConfig = _registry.GetConfig(null);

        // Assert
        Assert.NotNull(clusterConfig);
        Assert.Equal("host-a", clusterConfig.BrokerHost);
    }

    [Fact]
    public void GetConfig_WithInvalidCluster_ReturnsNull()
    {
        // Arrange
        var config = new GatewayConfig
        {
            Clusters = new Dictionary<string, ClusterConfig>
            {
                ["cluster-a"] = new ClusterConfig { BrokerHost = "host-a", BrokerPort = 9092 }
            }
        };
        _registry = new ClusterRegistry(config, NullLogger<ClusterRegistry>.Instance);

        // Act
        var clusterConfig = _registry.GetConfig("unknown");

        // Assert
        Assert.Null(clusterConfig);
    }
}
