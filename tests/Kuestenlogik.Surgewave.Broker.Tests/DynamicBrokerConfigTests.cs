using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for the DynamicBrokerConfig class.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DynamicBrokerConfigTests : IDisposable
{
    private readonly string _tempDir;
    private readonly ILoggerFactory _loggerFactory;

    public DynamicBrokerConfigTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-dynamic-config-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        _loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
    }

    public void Dispose()
    {
        _loggerFactory.Dispose();
        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    private DynamicBrokerConfig CreateConfig()
    {
        var staticConfig = new BrokerConfig { DataDirectory = _tempDir };
        return new DynamicBrokerConfig(staticConfig, _loggerFactory.CreateLogger<DynamicBrokerConfig>());
    }

    [Fact]
    public void GetConfig_ReturnsStaticValue_WhenNoDynamicOverride()
    {
        // Arrange
        var config = CreateConfig();

        // Act
        var value = config.GetConfig("num.partitions");

        // Assert - should return default from BrokerConfig (which is 1)
        Assert.NotNull(value);
        Assert.True(int.TryParse(value, out var numPartitions));
    }

    [Fact]
    public void SetConfig_ReturnsDynamicValue_WhenOverridden()
    {
        // Arrange
        var config = CreateConfig();

        // Act
        var error = config.SetConfig("num.partitions", "5");
        var value = config.GetConfig("num.partitions");

        // Assert
        Assert.Null(error);
        Assert.Equal("5", value);
    }

    [Fact]
    public void SetConfig_ReturnsError_ForReadOnlyConfig()
    {
        // Arrange
        var config = CreateConfig();

        // Act
        var error = config.SetConfig("broker.id", "999");

        // Assert
        Assert.NotNull(error);
        Assert.Contains("read-only", error);
    }

    [Fact]
    public void SetConfig_ReturnsError_ForUnknownConfig()
    {
        // Arrange
        var config = CreateConfig();

        // Act
        var error = config.SetConfig("unknown.config.key", "value");

        // Assert
        Assert.NotNull(error);
        Assert.Contains("not a recognized", error);
    }

    [Fact]
    public void SetConfig_ValidatesNumericValues()
    {
        // Arrange
        var config = CreateConfig();

        // Act - try invalid numeric value
        var error = config.SetConfig("num.partitions", "not-a-number");

        // Assert
        Assert.NotNull(error);
        Assert.Contains("numeric", error);
    }

    [Fact]
    public void SetConfig_ValidatesMinimumValues()
    {
        // Arrange
        var config = CreateConfig();

        // Act - try invalid value (partitions must be >= 1)
        var error = config.SetConfig("num.partitions", "0");

        // Assert
        Assert.NotNull(error);
        Assert.Contains("at least 1", error);
    }

    [Fact]
    public void SetConfig_ValidatesBooleanValues()
    {
        // Arrange
        var config = CreateConfig();

        // Act - try invalid boolean
        var error = config.SetConfig("auto.create.topics.enable", "maybe");

        // Assert
        Assert.NotNull(error);
        Assert.Contains("true", error);
    }

    [Fact]
    public void SetConfig_ValidatesCompressionType()
    {
        // Arrange
        var config = CreateConfig();

        // Act - try invalid compression type
        var error = config.SetConfig("compression.type", "bzip2");

        // Assert
        Assert.NotNull(error);
        Assert.Contains("gzip", error);
    }

    [Fact]
    public void SetConfig_AcceptsValidCompressionType()
    {
        // Arrange
        var config = CreateConfig();

        // Act - try valid compression type
        var error = config.SetConfig("compression.type", "lz4");

        // Assert
        Assert.Null(error);
        Assert.Equal("lz4", config.GetConfig("compression.type"));
    }

    [Fact]
    public void SetConfig_RevertsToDefault_WhenSetToNull()
    {
        // Arrange
        var config = CreateConfig();
        config.SetConfig("num.partitions", "10");

        // Act
        var error = config.SetConfig("num.partitions", null);
        var value = config.GetConfig("num.partitions");

        // Assert
        Assert.Null(error);
        Assert.False(config.IsDynamicallySet("num.partitions"));
    }

    [Fact]
    public void IsDynamicallySet_ReturnsTrueForOverrides()
    {
        // Arrange
        var config = CreateConfig();

        // Act
        config.SetConfig("num.partitions", "5");

        // Assert
        Assert.True(config.IsDynamicallySet("num.partitions"));
        Assert.False(config.IsDynamicallySet("log.segment.bytes"));
    }

    [Fact]
    public void GetDynamicConfigs_ReturnsAllOverrides()
    {
        // Arrange
        var config = CreateConfig();
        config.SetConfig("num.partitions", "5");
        config.SetConfig("log.retention.hours", "24");

        // Act
        var dynamicConfigs = config.GetDynamicConfigs();

        // Assert
        Assert.Equal(2, dynamicConfigs.Count);
        Assert.Equal("5", dynamicConfigs["num.partitions"]);
        Assert.Equal("24", dynamicConfigs["log.retention.hours"]);
    }

    [Fact]
    public void ConfigChanged_EventFires_WhenConfigChanged()
    {
        // Arrange
        var config = CreateConfig();
        ConfigChangedEventArgs? eventArgs = null;
        config.ConfigChanged += (sender, args) => eventArgs = args;

        // Act
        config.SetConfig("num.partitions", "10");

        // Assert
        Assert.NotNull(eventArgs);
        Assert.Equal("num.partitions", eventArgs.Name);
        Assert.Equal("10", eventArgs.NewValue);
    }

    [Fact]
    public void SetConfig_PersistsToFile()
    {
        // Arrange
        var config = CreateConfig();
        var configFilePath = Path.Combine(_tempDir, "dynamic-config.json");

        // Act
        config.SetConfig("num.partitions", "7");

        // Assert
        Assert.True(File.Exists(configFilePath));
        var content = File.ReadAllText(configFilePath);
        Assert.Contains("num.partitions", content);
        Assert.Contains("7", content);
    }

    [Fact]
    public void DynamicConfigKeys_ContainsExpectedKeys()
    {
        // Assert - verify key dynamic configs are present
        Assert.Contains("socket.send.buffer.bytes", DynamicBrokerConfig.DynamicConfigKeys);
        Assert.Contains("log.retention.hours", DynamicBrokerConfig.DynamicConfigKeys);
        Assert.Contains("num.partitions", DynamicBrokerConfig.DynamicConfigKeys);
        Assert.Contains("compression.type", DynamicBrokerConfig.DynamicConfigKeys);
    }

    [Fact]
    public void ReadOnlyConfigKeys_ContainsExpectedKeys()
    {
        // Assert - verify key read-only configs are present
        Assert.Contains("broker.id", DynamicBrokerConfig.ReadOnlyConfigKeys);
        Assert.Contains("node.id", DynamicBrokerConfig.ReadOnlyConfigKeys);
        Assert.Contains("listeners", DynamicBrokerConfig.ReadOnlyConfigKeys);
        Assert.Contains("log.dirs", DynamicBrokerConfig.ReadOnlyConfigKeys);
    }

    [Fact]
    public void SetConfig_AppliesChangesToStaticConfig()
    {
        // Arrange
        var staticConfig = new BrokerConfig { DataDirectory = _tempDir, DefaultNumPartitions = 1 };
        var config = new DynamicBrokerConfig(staticConfig, _loggerFactory.CreateLogger<DynamicBrokerConfig>());

        // Act
        config.SetConfig("num.partitions", "8");

        // Assert - the underlying static config should be updated
        Assert.Equal(8, staticConfig.DefaultNumPartitions);
    }

    [Fact]
    public void GetConfig_ReturnsCorrectStaticValues()
    {
        // Arrange
        var staticConfig = new BrokerConfig
        {
            DataDirectory = _tempDir,
            BrokerId = 42,
            DefaultNumPartitions = 3,
            AutoCreateTopics = true,
            LogRetentionHours = 168
        };
        var config = new DynamicBrokerConfig(staticConfig, _loggerFactory.CreateLogger<DynamicBrokerConfig>());

        // Assert
        Assert.Equal("42", config.GetConfig("broker.id"));
        Assert.Equal("3", config.GetConfig("num.partitions"));
        Assert.Equal("true", config.GetConfig("auto.create.topics.enable"));
        Assert.Equal("168", config.GetConfig("log.retention.hours"));
    }

    [Fact]
    public void SetConfig_ValidatesNegativeBytes()
    {
        // Arrange
        var config = CreateConfig();

        // Act - negative bytes (not -1) should fail
        var error = config.SetConfig("log.retention.bytes", "-50");

        // Assert
        Assert.NotNull(error);
        Assert.Contains("non-negative", error);
    }

    [Fact]
    public void SetConfig_AllowsUnlimitedBytes()
    {
        // Arrange
        var config = CreateConfig();

        // Act - -1 means unlimited
        var error = config.SetConfig("log.retention.bytes", "-1");

        // Assert
        Assert.Null(error);
        Assert.Equal("-1", config.GetConfig("log.retention.bytes"));
    }
}
