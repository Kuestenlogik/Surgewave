using Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Tests for cloud ObjectStore providers (S3, Azure Blob, GCP Cloud Storage),
/// the ObjectStoreKeyFormatter, ObjectStoreProviderFactory, and ObjectStoreProviderConfig.
/// These tests work without actual cloud credentials by testing key/path formatting logic
/// and factory behavior independently.
/// </summary>
[Trait("Category", "CloudObjectStore")]
public class CloudObjectStoreProviderTests
{
    // ==================== ObjectStoreKeyFormatter Tests ====================

    [Fact]
    public void KeyFormat_WithPrefix_FormatsCorrectly()
    {
        // Arrange & Act
        var key = ObjectStoreKeyFormatter.FormatKey("surgewave/data", "my-topic", 3, 12345);

        // Assert
        Assert.Equal("surgewave/data/topics/my-topic/partitions/3/00000000000000012345.segment", key);
    }

    [Fact]
    public void KeyFormat_WithoutPrefix_OmitsPrefix()
    {
        // Arrange & Act
        var key = ObjectStoreKeyFormatter.FormatKey(null, "my-topic", 0, 0);

        // Assert
        Assert.Equal("topics/my-topic/partitions/0/00000000000000000000.segment", key);
    }

    [Fact]
    public void KeyFormat_EmptyPrefix_OmitsPrefix()
    {
        // Arrange & Act
        var key = ObjectStoreKeyFormatter.FormatKey("", "orders", 1, 500);

        // Assert
        Assert.Equal("topics/orders/partitions/1/00000000000000000500.segment", key);
    }

    [Fact]
    public void KeyFormat_PadsOffset_20Digits()
    {
        // Arrange & Act
        var key = ObjectStoreKeyFormatter.FormatKey(null, "t", 0, 1);

        // Assert
        Assert.Contains("00000000000000000001.segment", key, StringComparison.Ordinal);
    }

    [Fact]
    public void KeyFormat_MaxOffset_Formats20Digits()
    {
        // Arrange - use a very large offset
        var largeOffset = 99999999999999999L;

        // Act
        var key = ObjectStoreKeyFormatter.FormatKey(null, "t", 0, largeOffset);

        // Assert
        Assert.Contains("00099999999999999999.segment", key, StringComparison.Ordinal);
    }

    [Fact]
    public void KeyFormat_HandlesSpecialTopicChars()
    {
        // Arrange - topic names with dots and hyphens (common in Kafka)
        var topic = "my.company-events.v2";

        // Act
        var key = ObjectStoreKeyFormatter.FormatKey("prefix", topic, 7, 42);

        // Assert
        Assert.Equal("prefix/topics/my.company-events.v2/partitions/7/00000000000000000042.segment", key);
    }

    [Fact]
    public void KeyFormat_TopicWithUnderscores()
    {
        // Arrange
        var topic = "user_activity_log";

        // Act
        var key = ObjectStoreKeyFormatter.FormatKey(null, topic, 0, 100);

        // Assert
        Assert.Equal("topics/user_activity_log/partitions/0/00000000000000000100.segment", key);
    }

    [Fact]
    public void ParseOffsetFromKey_ValidKey_ReturnsOffset()
    {
        // Arrange
        var key = "surgewave/data/topics/my-topic/partitions/3/00000000000000012345.segment";

        // Act
        var offset = ObjectStoreKeyFormatter.ParseOffsetFromKey(key);

        // Assert
        Assert.Equal(12345L, offset);
    }

    [Fact]
    public void ParseOffsetFromKey_ZeroOffset_Returns0()
    {
        // Arrange
        var key = "topics/t/partitions/0/00000000000000000000.segment";

        // Act
        var offset = ObjectStoreKeyFormatter.ParseOffsetFromKey(key);

        // Assert
        Assert.Equal(0L, offset);
    }

    [Fact]
    public void ParseOffsetFromKey_InvalidExtension_ReturnsNull()
    {
        // Arrange
        var key = "topics/t/partitions/0/00000000000000000001.log";

        // Act
        var offset = ObjectStoreKeyFormatter.ParseOffsetFromKey(key);

        // Assert
        Assert.Null(offset);
    }

    [Fact]
    public void ParseOffsetFromKey_NonNumericFilename_ReturnsNull()
    {
        // Arrange
        var key = "topics/t/partitions/0/not-a-number.segment";

        // Act
        var offset = ObjectStoreKeyFormatter.ParseOffsetFromKey(key);

        // Assert
        Assert.Null(offset);
    }

    [Fact]
    public void FormatListPrefix_WithPrefix_FormatsCorrectly()
    {
        // Arrange & Act
        var prefix = ObjectStoreKeyFormatter.FormatListPrefix("surgewave", "events", 2);

        // Assert
        Assert.Equal("surgewave/topics/events/partitions/2/", prefix);
    }

    [Fact]
    public void FormatListPrefix_WithoutPrefix_OmitsPrefix()
    {
        // Arrange & Act
        var prefix = ObjectStoreKeyFormatter.FormatListPrefix(null, "events", 0);

        // Assert
        Assert.Equal("topics/events/partitions/0/", prefix);
    }

    // ==================== S3 Provider Key Format Tests ====================

    [Fact]
    public void S3Provider_KeyFormat_Correct()
    {
        // Verify S3 provider would use the correct key format.
        // We test the shared formatter which all providers use.
        var key = ObjectStoreKeyFormatter.FormatKey("s3-prefix", "orders", 5, 1000);

        Assert.Equal("s3-prefix/topics/orders/partitions/5/00000000000000001000.segment", key);
    }

    [Fact]
    public async Task S3Provider_UploadAndDownload_Roundtrip_ViaLocalProvider()
    {
        // Test the key format logic and roundtrip via LocalFileObjectStoreProvider
        // which uses the same key structure conceptually.
        var tempDir = Path.Combine(Path.GetTempPath(), "surgewave-s3-test", Guid.NewGuid().ToString("N"));
        try
        {
            var provider = new LocalFileObjectStoreProvider(tempDir);
            var data = new byte[] { 1, 2, 3, 4, 5 };

            await provider.UploadAsync("test-topic", 0, 100, data);
            var result = await provider.DownloadAsync("test-topic", 0, 100);

            Assert.NotNull(result);
            Assert.Equal(data, result);
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public void S3Provider_ListSegments_ParsesOffsets()
    {
        // Test offset parsing from S3-style keys
        var keys = new[]
        {
            "prefix/topics/topic1/partitions/0/00000000000000000000.segment",
            "prefix/topics/topic1/partitions/0/00000000000000001000.segment",
            "prefix/topics/topic1/partitions/0/00000000000000005000.segment",
            "prefix/topics/topic1/partitions/0/00000000000000005000.meta.json" // should be ignored
        };

        var offsets = keys
            .Select(ObjectStoreKeyFormatter.ParseOffsetFromKey)
            .Where(o => o.HasValue)
            .Select(o => o!.Value)
            .Order()
            .ToList();

        Assert.Equal(3, offsets.Count);
        Assert.Equal(0L, offsets[0]);
        Assert.Equal(1000L, offsets[1]);
        Assert.Equal(5000L, offsets[2]);
    }

    // ==================== Azure Provider Path Format Tests ====================

    [Fact]
    public void AzureProvider_BlobPath_Correct()
    {
        // Verify Azure Blob provider would use the correct blob path.
        var path = ObjectStoreKeyFormatter.FormatKey("azure-prefix", "events", 2, 9999);

        Assert.Equal("azure-prefix/topics/events/partitions/2/00000000000000009999.segment", path);
    }

    [Fact]
    public async Task AzureProvider_UploadAndDownload_Roundtrip_ViaLocalProvider()
    {
        // Test the path format logic via LocalFileObjectStoreProvider
        var tempDir = Path.Combine(Path.GetTempPath(), "surgewave-azure-test", Guid.NewGuid().ToString("N"));
        try
        {
            var provider = new LocalFileObjectStoreProvider(tempDir);
            var data = new byte[] { 10, 20, 30, 40, 50 };

            await provider.UploadAsync("azure-topic", 3, 500, data);
            var result = await provider.DownloadAsync("azure-topic", 3, 500);

            Assert.NotNull(result);
            Assert.Equal(data, result);

            // Verify list returns the offset
            var offsets = await provider.ListSegmentOffsetsAsync("azure-topic", 3);
            Assert.Single(offsets);
            Assert.Equal(500L, offsets[0]);
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, recursive: true);
        }
    }

    // ==================== GCP Provider Path Format Tests ====================

    [Fact]
    public void GcpProvider_ObjectPath_Correct()
    {
        // Verify GCP Cloud Storage provider would use the correct object path.
        var path = ObjectStoreKeyFormatter.FormatKey("gcp-prefix", "metrics", 1, 77777);

        Assert.Equal("gcp-prefix/topics/metrics/partitions/1/00000000000000077777.segment", path);
    }

    [Fact]
    public async Task GcpProvider_UploadAndDownload_Roundtrip_ViaLocalProvider()
    {
        // Test the path format logic via LocalFileObjectStoreProvider
        var tempDir = Path.Combine(Path.GetTempPath(), "surgewave-gcp-test", Guid.NewGuid().ToString("N"));
        try
        {
            var provider = new LocalFileObjectStoreProvider(tempDir);
            var data = new byte[] { 100, 200, 255 };

            await provider.UploadAsync("gcp-topic", 10, 0, data);
            var result = await provider.DownloadAsync("gcp-topic", 10, 0);

            Assert.NotNull(result);
            Assert.Equal(data, result);
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, recursive: true);
        }
    }

    // ==================== ObjectStoreProviderFactory Tests ====================

    [Fact]
    public void ProviderFactory_CreatesLocal()
    {
        // Arrange
        var config = new ObjectStoreProviderConfig
        {
            Type = ObjectStoreProviderType.Local,
            LocalPath = "./test-object-store"
        };

        // Act
        var provider = ObjectStoreProviderFactory.Create(config);

        // Assert
        Assert.IsType<LocalFileObjectStoreProvider>(provider);
    }

    [Fact]
    public void ProviderFactory_CreatesLocal_DefaultPath()
    {
        // Arrange
        var config = new ObjectStoreProviderConfig
        {
            Type = ObjectStoreProviderType.Local
        };

        // Act
        var provider = ObjectStoreProviderFactory.Create(config);

        // Assert
        Assert.IsType<LocalFileObjectStoreProvider>(provider);
    }

    [Fact]
    public void ProviderFactory_CreatesS3()
    {
        // Arrange
        var config = new ObjectStoreProviderConfig
        {
            Type = ObjectStoreProviderType.S3,
            BucketName = "test-bucket",
            Prefix = "surgewave"
        };

        // Act - This may throw if AWS SDK can't initialize without credentials/region,
        // but the factory logic itself should work.
        try
        {
            using var provider = ObjectStoreProviderFactory.Create(config) as IDisposable;
            Assert.IsType<S3ObjectStoreProvider>(provider);
        }
        catch (Exception ex) when (ex is Amazon.Runtime.AmazonServiceException or Amazon.Runtime.AmazonClientException)
        {
            // Expected when no AWS credentials or region are configured
        }
    }

    [Fact]
    public void ProviderFactory_CreatesAzure()
    {
        // Arrange - use a fake connection string format that Azure SDK will accept for client creation
        var config = new ObjectStoreProviderConfig
        {
            Type = ObjectStoreProviderType.AzureBlob,
            ConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            ContainerName = "test-container",
            Prefix = "surgewave"
        };

        // Act
        using var provider = ObjectStoreProviderFactory.Create(config) as IDisposable;

        // Assert
        Assert.IsType<AzureBlobObjectStoreProvider>(provider);
    }

    [Fact]
    public void ProviderFactory_CreatesGcp()
    {
        // Arrange
        var config = new ObjectStoreProviderConfig
        {
            Type = ObjectStoreProviderType.Gcp,
            BucketName = "test-bucket",
            Prefix = "surgewave"
        };

        // Act - This may throw if GCP credentials are not configured
        try
        {
            using var provider = ObjectStoreProviderFactory.Create(config) as IDisposable;
            Assert.IsType<GcpCloudStorageObjectStoreProvider>(provider);
        }
        catch (InvalidOperationException)
        {
            // Expected when no GCP credentials are configured
        }
        catch (Google.Apis.Auth.OAuth2.Responses.TokenResponseException)
        {
            // Expected when no GCP credentials are configured
        }
    }

    [Fact]
    public void ProviderFactory_UnknownType_Throws()
    {
        // Arrange
        var config = new ObjectStoreProviderConfig
        {
            Type = (ObjectStoreProviderType)999
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => ObjectStoreProviderFactory.Create(config));
    }

    [Fact]
    public void ProviderFactory_S3_MissingBucket_Throws()
    {
        // Arrange
        var config = new ObjectStoreProviderConfig
        {
            Type = ObjectStoreProviderType.S3
        };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => ObjectStoreProviderFactory.Create(config));
    }

    [Fact]
    public void ProviderFactory_Azure_MissingConnectionString_Throws()
    {
        // Arrange
        var config = new ObjectStoreProviderConfig
        {
            Type = ObjectStoreProviderType.AzureBlob,
            ContainerName = "test"
        };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => ObjectStoreProviderFactory.Create(config));
    }

    [Fact]
    public void ProviderFactory_Azure_MissingContainer_Throws()
    {
        // Arrange
        var config = new ObjectStoreProviderConfig
        {
            Type = ObjectStoreProviderType.AzureBlob,
            ConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net"
        };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => ObjectStoreProviderFactory.Create(config));
    }

    [Fact]
    public void ProviderFactory_Gcp_MissingBucket_Throws()
    {
        // Arrange
        var config = new ObjectStoreProviderConfig
        {
            Type = ObjectStoreProviderType.Gcp
        };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => ObjectStoreProviderFactory.Create(config));
    }

    // ==================== ObjectStoreProviderConfig Tests ====================

    [Fact]
    public void ProviderConfig_DefaultValues()
    {
        // Arrange & Act
        var config = new ObjectStoreProviderConfig();

        // Assert
        Assert.Equal(ObjectStoreProviderType.Local, config.Type);
        Assert.Null(config.LocalPath);
        Assert.Null(config.BucketName);
        Assert.Null(config.Region);
        Assert.Null(config.AccessKey);
        Assert.Null(config.SecretKey);
        Assert.Null(config.ConnectionString);
        Assert.Null(config.ContainerName);
        Assert.Null(config.Prefix);
    }

    [Fact]
    public void ProviderConfig_AllProperties_SetCorrectly()
    {
        // Arrange & Act
        var config = new ObjectStoreProviderConfig
        {
            Type = ObjectStoreProviderType.S3,
            LocalPath = "/data/store",
            BucketName = "my-bucket",
            Region = "us-west-2",
            AccessKey = "AKIA...",
            SecretKey = "secret",
            ConnectionString = "conn-str",
            ContainerName = "container",
            Prefix = "surgewave/prod"
        };

        // Assert
        Assert.Equal(ObjectStoreProviderType.S3, config.Type);
        Assert.Equal("/data/store", config.LocalPath);
        Assert.Equal("my-bucket", config.BucketName);
        Assert.Equal("us-west-2", config.Region);
        Assert.Equal("AKIA...", config.AccessKey);
        Assert.Equal("secret", config.SecretKey);
        Assert.Equal("conn-str", config.ConnectionString);
        Assert.Equal("container", config.ContainerName);
        Assert.Equal("surgewave/prod", config.Prefix);
    }

    // ==================== ObjectStoreProviderType Tests ====================

    [Fact]
    public void ProviderType_AllValues_Exist()
    {
        // Verify all enum values exist
        Assert.Equal(0, (int)ObjectStoreProviderType.Local);
        Assert.Equal(1, (int)ObjectStoreProviderType.S3);
        Assert.Equal(2, (int)ObjectStoreProviderType.AzureBlob);
        Assert.Equal(3, (int)ObjectStoreProviderType.Gcp);
    }
}
