using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for ApiVersions response serialization and the CreateDefault factory.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ApiVersionsResponseTests
{
    #region CreateDefault Tests

    [Fact]
    public void CreateDefault_ReturnsNonNullResponse()
    {
        // Act
        var response = ApiVersionsResponse.CreateDefault(correlationId: 1, apiVersion: 3);

        // Assert
        Assert.NotNull(response);
        Assert.NotNull(response.ApiVersions);
        Assert.NotEmpty(response.ApiVersions);
    }

    [Fact]
    public void CreateDefault_ErrorCode_IsNone()
    {
        // Act
        var response = ApiVersionsResponse.CreateDefault(1, 3);

        // Assert
        Assert.Equal(ErrorCode.None, response.ErrorCode);
    }

    [Fact]
    public void CreateDefault_IncludesProduceApiKey()
    {
        // Act
        var response = ApiVersionsResponse.CreateDefault(1, 3);

        // Assert
        var produce = response.ApiVersions!.FirstOrDefault(v => v.ApiKey == (short)ApiKey.Produce);
        Assert.NotNull(produce);
        Assert.True(produce.MaxVersion >= 9, "Produce should support at least v9 for flexible format");
    }

    [Fact]
    public void CreateDefault_IncludesFetchApiKey()
    {
        // Act
        var response = ApiVersionsResponse.CreateDefault(1, 3);

        // Assert
        var fetch = response.ApiVersions!.FirstOrDefault(v => v.ApiKey == (short)ApiKey.Fetch);
        Assert.NotNull(fetch);
    }

    [Fact]
    public void CreateDefault_IncludesApiVersionsApiKey()
    {
        // Act
        var response = ApiVersionsResponse.CreateDefault(1, 3);

        // Assert
        var apiVersions = response.ApiVersions!.FirstOrDefault(v => v.ApiKey == (short)ApiKey.ApiVersions);
        Assert.NotNull(apiVersions);
    }

    [Fact]
    public void CreateDefault_IncludesMetadataApiKey()
    {
        // Act
        var response = ApiVersionsResponse.CreateDefault(1, 3);

        // Assert
        var metadata = response.ApiVersions!.FirstOrDefault(v => v.ApiKey == (short)ApiKey.Metadata);
        Assert.NotNull(metadata);
    }

    [Fact]
    public void CreateDefault_AllVersionRanges_AreValid()
    {
        // Act
        var response = ApiVersionsResponse.CreateDefault(1, 3);

        // Assert - MinVersion <= MaxVersion for all entries
        foreach (var version in response.ApiVersions!)
        {
            Assert.True(version.MinVersion <= version.MaxVersion,
                $"ApiKey {version.ApiKey}: MinVersion ({version.MinVersion}) > MaxVersion ({version.MaxVersion})");
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(3)]
    public void CreateDefault_WithDifferentApiVersions_Succeeds(short apiVersion)
    {
        // Act
        var response = ApiVersionsResponse.CreateDefault(1, apiVersion);

        // Assert
        Assert.NotNull(response);
        Assert.Equal(apiVersion, response.ApiVersion);
    }

    [Fact]
    public void CreateDefault_CorrelationId_IsPreserved()
    {
        // Act
        var response = ApiVersionsResponse.CreateDefault(correlationId: 42, apiVersion: 3);

        // Assert
        Assert.Equal(42, response.CorrelationId);
    }

    #endregion

    #region WriteTo Tests

    [Fact]
    public void ApiVersionsResponse_V0_WriteTo_StartsWithCorrelationId()
    {
        // Arrange
        var response = ApiVersionsResponse.CreateDefault(99, 0);

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert - first 4 bytes are correlation ID
        var correlationId = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(bytes);
        Assert.Equal(99, correlationId);
    }

    [Fact]
    public void ApiVersionsResponse_V0_NoHeaderTaggedFields()
    {
        // Arrange - v0 uses non-flexible header even for response
        var response = new ApiVersionsResponse
        {
            CorrelationId = 1,
            ApiVersion = 0,
            ErrorCode = ErrorCode.None,
            ApiVersions = new[]
            {
                new ApiVersionsResponse.SupportedApiVersion { ApiKey = 18, MinVersion = 0, MaxVersion = 4 }
            }
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert - v0: CorrelationId(4) + ErrorCode(2) + ArraySize(4) + ApiEntry(6) + ThrottleTime absent
        // At minimum should be 4 + 2 + 4 + 6 = 16 bytes
        Assert.True(bytes.Length >= 16, $"Response should be at least 16 bytes, got {bytes.Length}");
    }

    [Fact]
    public void ApiVersionsResponse_V3_FlexibleFormat()
    {
        // Arrange - v3 uses flexible format for body
        var response = new ApiVersionsResponse
        {
            CorrelationId = 1,
            ApiVersion = 3,
            ErrorCode = ErrorCode.None,
            ThrottleTimeMs = 0,
            ApiVersions = new[]
            {
                new ApiVersionsResponse.SupportedApiVersion { ApiKey = 18, MinVersion = 0, MaxVersion = 4 }
            }
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void ApiVersionsResponse_WithSupportedFeatures_SerializesTaggedFields()
    {
        // Arrange
        var response = new ApiVersionsResponse
        {
            CorrelationId = 1,
            ApiVersion = 3,
            ErrorCode = ErrorCode.None,
            ApiVersions = Array.Empty<ApiVersionsResponse.SupportedApiVersion>(),
            SupportedFeatures = new List<ApiVersionsResponse.SupportedFeature>
            {
                new() { Name = "kraft.version", MinVersion = 0, MaxVersion = 1 }
            }
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void ApiVersionsResponse_WithFinalizedFeaturesEpoch_SerializesTaggedField()
    {
        // Arrange
        var response = new ApiVersionsResponse
        {
            CorrelationId = 1,
            ApiVersion = 3,
            ErrorCode = ErrorCode.None,
            ApiVersions = Array.Empty<ApiVersionsResponse.SupportedApiVersion>(),
            FinalizedFeaturesEpoch = 5
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert - should be larger than with epoch == -1
        var responseWithoutEpoch = new ApiVersionsResponse
        {
            CorrelationId = 1,
            ApiVersion = 3,
            ErrorCode = ErrorCode.None,
            ApiVersions = Array.Empty<ApiVersionsResponse.SupportedApiVersion>(),
            FinalizedFeaturesEpoch = -1 // default, not serialized
        };

        using var writer2 = new KafkaProtocolWriter();
        responseWithoutEpoch.WriteTo(writer2);
        var bytesWithoutEpoch = writer2.ToArray();

        Assert.True(bytes.Length > bytesWithoutEpoch.Length,
            "Response with epoch should be larger than without");
    }

    [Fact]
    public void ApiVersionsResponse_ZkMigrationReady_SerializesTaggedField()
    {
        // Arrange
        var response = new ApiVersionsResponse
        {
            CorrelationId = 1,
            ApiVersion = 3,
            ErrorCode = ErrorCode.None,
            ApiVersions = Array.Empty<ApiVersionsResponse.SupportedApiVersion>(),
            ZkMigrationReady = true
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void ApiVersionsResponse_V1_IncludesThrottleTimeMs()
    {
        // Arrange
        var response = new ApiVersionsResponse
        {
            CorrelationId = 1,
            ApiVersion = 1,
            ErrorCode = ErrorCode.None,
            ThrottleTimeMs = 100,
            ApiVersions = Array.Empty<ApiVersionsResponse.SupportedApiVersion>()
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert - v1 includes ThrottleTimeMs (v0 does not)
        var responseV0 = new ApiVersionsResponse
        {
            CorrelationId = 1,
            ApiVersion = 0,
            ErrorCode = ErrorCode.None,
            ThrottleTimeMs = 100,
            ApiVersions = Array.Empty<ApiVersionsResponse.SupportedApiVersion>()
        };
        using var writer0 = new KafkaProtocolWriter();
        responseV0.WriteTo(writer0);
        var bytesV0 = writer0.ToArray();

        // V1 should have 4 more bytes (ThrottleTimeMs)
        Assert.Equal(bytesV0.Length + 4, bytes.Length);
    }

    #endregion
}
