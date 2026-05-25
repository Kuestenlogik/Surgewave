using Kuestenlogik.Surgewave.Storage.Tiering;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Tiering.Tests;

/// <summary>
/// Tests for TieredStorageConfig and RemoteStorageProviderFactory.
/// </summary>
public class TieredStorageConfigTests
{
    [Fact]
    public void DefaultConfig_HasExpectedValues()
    {
        var config = new TieredStorageConfig();

        Assert.False(config.Enabled);
        Assert.Equal("local", config.Provider);
        Assert.Equal("./tiered-storage", config.LocalPath);
        Assert.Equal("surgewave-tiered", config.AzureContainerName);
        Assert.Equal(string.Empty, config.Prefix);
        Assert.Equal(24, config.LocalRetentionHours);
        Assert.Equal(-1, config.RemoteRetentionHours);
        Assert.Equal(1, config.TieringLagHours);
        Assert.Equal(1024 * 1024, config.MinSegmentSizeBytes);
        Assert.Equal(1024L * 1024 * 1024, config.LocalCacheSizeBytes);
        Assert.Equal("./tiered-cache", config.LocalCachePath);
        Assert.True(config.DeleteAfterUpload);
        Assert.Equal(300, config.TieringIntervalSeconds);
    }

    [Fact]
    public void Config_IsRecordType_SupportsEqualityComparison()
    {
        var a = new TieredStorageConfig { Provider = "s3" };
        var b = new TieredStorageConfig { Provider = "s3" };
        Assert.Equal(a, b);
    }

    [Fact]
    public void Config_WithDifferentProvider_IsNotEqual()
    {
        var a = new TieredStorageConfig { Provider = "s3" };
        var b = new TieredStorageConfig { Provider = "azure" };
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Config_CanSetAllProperties()
    {
        var config = new TieredStorageConfig
        {
            Enabled = true,
            Provider = "s3",
            LocalPath = "/local/path",
            AzureConnectionString = "DefaultEndpointsProtocol=https;...",
            AzureContainerName = "my-container",
            S3BucketName = "my-bucket",
            S3Region = "us-east-1",
            GcpBucketName = "my-gcp-bucket",
            Prefix = "surgewave/",
            LocalRetentionHours = 48,
            RemoteRetentionHours = 720,
            TieringLagHours = 2,
            MinSegmentSizeBytes = 5 * 1024 * 1024,
            LocalCacheSizeBytes = 10L * 1024 * 1024 * 1024,
            LocalCachePath = "/cache",
            DeleteAfterUpload = false,
            TieringIntervalSeconds = 60
        };

        Assert.True(config.Enabled);
        Assert.Equal("s3", config.Provider);
        Assert.Equal("/local/path", config.LocalPath);
        Assert.Equal("my-bucket", config.S3BucketName);
        Assert.Equal("us-east-1", config.S3Region);
        Assert.Equal("surgewave/", config.Prefix);
        Assert.Equal(720, config.RemoteRetentionHours);
        Assert.Equal(5 * 1024 * 1024, config.MinSegmentSizeBytes);
        Assert.False(config.DeleteAfterUpload);
        Assert.Equal(60, config.TieringIntervalSeconds);
    }

    [Fact]
    public async Task RemoteStorageProviderFactory_CreateLocal_ReturnsLocalProvider()
    {
        var testDir = Path.Combine(Path.GetTempPath(), $"surgewave-factory-{Guid.NewGuid():N}");
        try
        {
            Directory.CreateDirectory(testDir);
            var config = new TieredStorageConfig { Provider = "local", LocalPath = testDir };

            var provider = RemoteStorageProviderFactory.Create(config);

            Assert.NotNull(provider);
            Assert.IsType<LocalFileSystemStorageProvider>(provider);

            await provider.DisposeAsync();
        }
        finally
        {
            try { Directory.Delete(testDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public void RemoteStorageProviderFactory_UnknownProvider_ThrowsArgumentException()
    {
        var config = new TieredStorageConfig { Provider = "unknown-provider-xyz" };

        Assert.ThrowsAny<ArgumentException>(
            () => RemoteStorageProviderFactory.Create(config));
    }

    [Fact]
    public async Task RemoteStorageProviderFactory_RegisterCustomProvider_CanCreate()
    {
        var testDir = Path.Combine(Path.GetTempPath(), $"surgewave-custom-{Guid.NewGuid():N}");
        try
        {
            Directory.CreateDirectory(testDir);
            var providerName = $"test-custom-{Guid.NewGuid():N}";

            RemoteStorageProviderFactory.RegisterProvider(
                providerName,
                cfg => new LocalFileSystemStorageProvider(testDir));

            var config = new TieredStorageConfig { Provider = providerName };
            var provider = RemoteStorageProviderFactory.Create(config);

            Assert.NotNull(provider);
            await provider.DisposeAsync();
        }
        finally
        {
            try { Directory.Delete(testDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task RemoteStorageProviderFactory_ProviderNameIsCaseInsensitive()
    {
        var testDir = Path.Combine(Path.GetTempPath(), $"surgewave-case-{Guid.NewGuid():N}");
        try
        {
            Directory.CreateDirectory(testDir);

            var configLower = new TieredStorageConfig { Provider = "local", LocalPath = testDir };
            var configUpper = new TieredStorageConfig { Provider = "LOCAL", LocalPath = testDir };
            var configMixed = new TieredStorageConfig { Provider = "Local", LocalPath = testDir };

            var p1 = RemoteStorageProviderFactory.Create(configLower);
            var p2 = RemoteStorageProviderFactory.Create(configUpper);
            var p3 = RemoteStorageProviderFactory.Create(configMixed);

            Assert.NotNull(p1);
            Assert.NotNull(p2);
            Assert.NotNull(p3);

            await p1.DisposeAsync();
            await p2.DisposeAsync();
            await p3.DisposeAsync();
        }
        finally
        {
            try { Directory.Delete(testDir, recursive: true); } catch { }
        }
    }
}
