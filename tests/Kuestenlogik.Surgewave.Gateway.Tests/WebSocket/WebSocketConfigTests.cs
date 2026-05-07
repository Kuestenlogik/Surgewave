using Kuestenlogik.Surgewave.Gateway.WebSocket;
using Xunit;

namespace Kuestenlogik.Surgewave.Gateway.Tests.WebSocket;

public class WebSocketConfigTests
{
    [Fact]
    public void DefaultValues_AreCorrect()
    {
        // Arrange & Act
        var config = new WebSocketConfig();

        // Assert
        Assert.True(config.Enabled);
        Assert.Equal(30000, config.HeartbeatIntervalMs);
        Assert.Equal(120000, config.SessionTimeoutMs);
        Assert.Equal(1048576, config.MaxMessageSizeBytes);
        Assert.Equal(100, config.MaxSubscriptionsPerSession);
        Assert.True(config.BatchDeliveryEnabled);
        Assert.Equal(100, config.BatchDeliveryMaxSize);
        Assert.Equal(100, config.BatchDeliveryMaxWaitMs);
        Assert.Equal(1000, config.SendBufferCapacity);
    }

    [Fact]
    public void Enabled_CanBeSet()
    {
        // Arrange
        var config = new WebSocketConfig();

        // Act
        config.Enabled = false;

        // Assert
        Assert.False(config.Enabled);
    }

    [Fact]
    public void HeartbeatIntervalMs_CanBeSet()
    {
        // Arrange
        var config = new WebSocketConfig();

        // Act
        config.HeartbeatIntervalMs = 15000;

        // Assert
        Assert.Equal(15000, config.HeartbeatIntervalMs);
    }

    [Fact]
    public void SessionTimeoutMs_CanBeSet()
    {
        // Arrange
        var config = new WebSocketConfig();

        // Act
        config.SessionTimeoutMs = 60000;

        // Assert
        Assert.Equal(60000, config.SessionTimeoutMs);
    }

    [Fact]
    public void MaxMessageSizeBytes_CanBeSet()
    {
        // Arrange
        var config = new WebSocketConfig();

        // Act
        config.MaxMessageSizeBytes = 2097152; // 2 MB

        // Assert
        Assert.Equal(2097152, config.MaxMessageSizeBytes);
    }

    [Fact]
    public void MaxSubscriptionsPerSession_CanBeSet()
    {
        // Arrange
        var config = new WebSocketConfig();

        // Act
        config.MaxSubscriptionsPerSession = 50;

        // Assert
        Assert.Equal(50, config.MaxSubscriptionsPerSession);
    }

    [Fact]
    public void BatchDeliveryEnabled_CanBeSet()
    {
        // Arrange
        var config = new WebSocketConfig();

        // Act
        config.BatchDeliveryEnabled = false;

        // Assert
        Assert.False(config.BatchDeliveryEnabled);
    }

    [Fact]
    public void BatchDeliveryMaxSize_CanBeSet()
    {
        // Arrange
        var config = new WebSocketConfig();

        // Act
        config.BatchDeliveryMaxSize = 500;

        // Assert
        Assert.Equal(500, config.BatchDeliveryMaxSize);
    }

    [Fact]
    public void BatchDeliveryMaxWaitMs_CanBeSet()
    {
        // Arrange
        var config = new WebSocketConfig();

        // Act
        config.BatchDeliveryMaxWaitMs = 200;

        // Assert
        Assert.Equal(200, config.BatchDeliveryMaxWaitMs);
    }

    [Fact]
    public void SendBufferCapacity_CanBeSet()
    {
        // Arrange
        var config = new WebSocketConfig();

        // Act
        config.SendBufferCapacity = 5000;

        // Assert
        Assert.Equal(5000, config.SendBufferCapacity);
    }
}
