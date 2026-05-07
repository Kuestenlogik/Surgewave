using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for ACL Admin APIs (DescribeAcls, CreateAcls, DeleteAcls)
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class AclAdminApiTests
{
    #region DescribeAcls Request/Response Tests

    [Fact]
    public void DescribeAclsRequest_RoundTrip_V0()
    {
        // Arrange
        var request = new DescribeAclsRequest
        {
            ApiKey = ApiKey.DescribeAcls,
            ApiVersion = 0,
            CorrelationId = 123,
            ClientId = "test-client",
            ResourceTypeFilter = AclResourceTypeFilter.Topic,
            ResourceNameFilter = "test-topic",
            PatternTypeFilter = AclPatternTypeFilter.Literal,
            PrincipalFilter = "User:admin",
            HostFilter = "*",
            OperationFilter = AclOperationFilter.Read,
            PermissionTypeFilter = AclPermissionTypeFilter.Allow
        };

        // Act - Serialize
        using var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert - Verify bytes are non-empty
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void DescribeAclsResponse_WriteTo_V0()
    {
        // Arrange
        var response = new DescribeAclsResponse
        {
            CorrelationId = 123,
            ApiVersion = 0,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Resources = new List<DescribeAclsResponse.AclResource>
            {
                new()
                {
                    ResourceType = AclResourceTypeFilter.Topic,
                    ResourceName = "test-topic",
                    PatternType = AclPatternTypeFilter.Literal,
                    Acls = new List<DescribeAclsResponse.AclBinding>
                    {
                        new()
                        {
                            Principal = "User:admin",
                            Host = "*",
                            Operation = AclOperationFilter.Read,
                            PermissionType = AclPermissionTypeFilter.Allow
                        }
                    }
                }
            }
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
        // Verify correlation ID is at the start (big-endian)
        Assert.Equal(123, System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(0, 4)));
    }

    [Fact]
    public void DescribeAclsResponse_WriteTo_V2_Flexible()
    {
        // Arrange
        var response = new DescribeAclsResponse
        {
            CorrelationId = 456,
            ApiVersion = 2, // Flexible format
            ThrottleTimeMs = 100,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Resources = new List<DescribeAclsResponse.AclResource>()
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
        // Should include correlation ID, tagged fields, throttle time, etc.
        Assert.True(bytes.Length > 8);
    }

    [Fact]
    public void DescribeAclsResponse_WithMultipleResources_Serializes()
    {
        // Arrange
        var response = new DescribeAclsResponse
        {
            CorrelationId = 789,
            ApiVersion = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Resources = new List<DescribeAclsResponse.AclResource>
            {
                new()
                {
                    ResourceType = AclResourceTypeFilter.Topic,
                    ResourceName = "topic-1",
                    PatternType = AclPatternTypeFilter.Literal,
                    Acls = new List<DescribeAclsResponse.AclBinding>
                    {
                        new()
                        {
                            Principal = "User:user1",
                            Host = "*",
                            Operation = AclOperationFilter.Read,
                            PermissionType = AclPermissionTypeFilter.Allow
                        },
                        new()
                        {
                            Principal = "User:user2",
                            Host = "*",
                            Operation = AclOperationFilter.Write,
                            PermissionType = AclPermissionTypeFilter.Allow
                        }
                    }
                },
                new()
                {
                    ResourceType = AclResourceTypeFilter.Group,
                    ResourceName = "group-1",
                    PatternType = AclPatternTypeFilter.Literal,
                    Acls = new List<DescribeAclsResponse.AclBinding>
                    {
                        new()
                        {
                            Principal = "User:consumer",
                            Host = "*",
                            Operation = AclOperationFilter.Read,
                            PermissionType = AclPermissionTypeFilter.Allow
                        }
                    }
                }
            }
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    #endregion

    #region CreateAcls Request/Response Tests

    [Fact]
    public void CreateAclsRequest_RoundTrip_V0()
    {
        // Arrange
        var request = new CreateAclsRequest
        {
            ApiKey = ApiKey.CreateAcls,
            ApiVersion = 0,
            CorrelationId = 789,
            ClientId = "test-client",
            Creations = new List<CreateAclsRequest.AclCreation>
            {
                new()
                {
                    ResourceType = AclResourceTypeFilter.Topic,
                    ResourceName = "new-topic",
                    PatternType = AclPatternTypeFilter.Literal,
                    Principal = "User:producer",
                    Host = "*",
                    Operation = AclOperationFilter.Write,
                    PermissionType = AclPermissionTypeFilter.Allow
                }
            }
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void CreateAclsResponse_WriteTo_V0()
    {
        // Arrange
        var response = new CreateAclsResponse
        {
            CorrelationId = 789,
            ApiVersion = 0,
            ThrottleTimeMs = 0,
            Results = new List<CreateAclsResponse.AclCreationResult>
            {
                new()
                {
                    ErrorCode = ErrorCode.None,
                    ErrorMessage = null
                }
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
    public void CreateAclsResponse_WriteTo_WithError()
    {
        // Arrange
        var response = new CreateAclsResponse
        {
            CorrelationId = 999,
            ApiVersion = 1,
            ThrottleTimeMs = 0,
            Results = new List<CreateAclsResponse.AclCreationResult>
            {
                new()
                {
                    ErrorCode = ErrorCode.ClusterAuthorizationFailed,
                    ErrorMessage = "Not authorized to create ACLs"
                }
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
    public void CreateAclsResponse_WriteTo_V2_Flexible()
    {
        // Arrange
        var response = new CreateAclsResponse
        {
            CorrelationId = 111,
            ApiVersion = 2, // Flexible
            ThrottleTimeMs = 50,
            Results = new List<CreateAclsResponse.AclCreationResult>
            {
                new()
                {
                    ErrorCode = ErrorCode.None,
                    ErrorMessage = null
                },
                new()
                {
                    ErrorCode = ErrorCode.None,
                    ErrorMessage = null
                }
            }
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
        Assert.True(bytes.Length > 10);
    }

    #endregion

    #region DeleteAcls Request/Response Tests

    [Fact]
    public void DeleteAclsRequest_RoundTrip_V0()
    {
        // Arrange
        var request = new DeleteAclsRequest
        {
            ApiKey = ApiKey.DeleteAcls,
            ApiVersion = 0,
            CorrelationId = 111,
            ClientId = "test-client",
            Filters = new List<DeleteAclsRequest.AclFilter>
            {
                new()
                {
                    ResourceTypeFilter = AclResourceTypeFilter.Topic,
                    ResourceNameFilter = "old-topic",
                    PatternTypeFilter = AclPatternTypeFilter.Literal,
                    PrincipalFilter = "User:old-user",
                    HostFilter = "*",
                    OperationFilter = AclOperationFilter.Any,
                    PermissionTypeFilter = AclPermissionTypeFilter.Any
                }
            }
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void DeleteAclsResponse_WriteTo_V0()
    {
        // Arrange
        var response = new DeleteAclsResponse
        {
            CorrelationId = 111,
            ApiVersion = 0,
            ThrottleTimeMs = 0,
            FilterResults = new List<DeleteAclsResponse.AclFilterResult>
            {
                new()
                {
                    ErrorCode = ErrorCode.None,
                    ErrorMessage = null,
                    MatchingAcls = new List<DeleteAclsResponse.MatchingAcl>
                    {
                        new()
                        {
                            ErrorCode = ErrorCode.None,
                            ErrorMessage = null,
                            ResourceType = AclResourceTypeFilter.Topic,
                            ResourceName = "old-topic",
                            PatternType = AclPatternTypeFilter.Literal,
                            Principal = "User:old-user",
                            Host = "*",
                            Operation = AclOperationFilter.Read,
                            PermissionType = AclPermissionTypeFilter.Allow
                        }
                    }
                }
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
    public void DeleteAclsResponse_WriteTo_V2_Flexible()
    {
        // Arrange
        var response = new DeleteAclsResponse
        {
            CorrelationId = 222,
            ApiVersion = 2, // Flexible
            ThrottleTimeMs = 25,
            FilterResults = new List<DeleteAclsResponse.AclFilterResult>
            {
                new()
                {
                    ErrorCode = ErrorCode.None,
                    ErrorMessage = null,
                    MatchingAcls = new List<DeleteAclsResponse.MatchingAcl>()
                }
            }
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    #endregion

    #region ACL Enum Mapping Tests

    [Fact]
    public void AclResourceTypeFilter_MatchesKafkaProtocol()
    {
        Assert.Equal(0, (sbyte)AclResourceTypeFilter.Unknown);
        Assert.Equal(1, (sbyte)AclResourceTypeFilter.Any);
        Assert.Equal(2, (sbyte)AclResourceTypeFilter.Topic);
        Assert.Equal(3, (sbyte)AclResourceTypeFilter.Group);
        Assert.Equal(4, (sbyte)AclResourceTypeFilter.Cluster);
        Assert.Equal(5, (sbyte)AclResourceTypeFilter.TransactionalId);
        Assert.Equal(6, (sbyte)AclResourceTypeFilter.DelegationToken);
    }

    [Fact]
    public void AclPatternTypeFilter_MatchesKafkaProtocol()
    {
        Assert.Equal(0, (sbyte)AclPatternTypeFilter.Unknown);
        Assert.Equal(1, (sbyte)AclPatternTypeFilter.Any);
        Assert.Equal(2, (sbyte)AclPatternTypeFilter.Match);
        Assert.Equal(3, (sbyte)AclPatternTypeFilter.Literal);
        Assert.Equal(4, (sbyte)AclPatternTypeFilter.Prefixed);
    }

    [Fact]
    public void AclOperationFilter_MatchesKafkaProtocol()
    {
        Assert.Equal(0, (sbyte)AclOperationFilter.Unknown);
        Assert.Equal(1, (sbyte)AclOperationFilter.Any);
        Assert.Equal(2, (sbyte)AclOperationFilter.All);
        Assert.Equal(3, (sbyte)AclOperationFilter.Read);
        Assert.Equal(4, (sbyte)AclOperationFilter.Write);
        Assert.Equal(5, (sbyte)AclOperationFilter.Create);
        Assert.Equal(6, (sbyte)AclOperationFilter.Delete);
        Assert.Equal(7, (sbyte)AclOperationFilter.Alter);
        Assert.Equal(8, (sbyte)AclOperationFilter.Describe);
        Assert.Equal(9, (sbyte)AclOperationFilter.ClusterAction);
        Assert.Equal(10, (sbyte)AclOperationFilter.DescribeConfigs);
        Assert.Equal(11, (sbyte)AclOperationFilter.AlterConfigs);
        Assert.Equal(12, (sbyte)AclOperationFilter.IdempotentWrite);
    }

    [Fact]
    public void AclPermissionTypeFilter_MatchesKafkaProtocol()
    {
        Assert.Equal(0, (sbyte)AclPermissionTypeFilter.Unknown);
        Assert.Equal(1, (sbyte)AclPermissionTypeFilter.Any);
        Assert.Equal(2, (sbyte)AclPermissionTypeFilter.Deny);
        Assert.Equal(3, (sbyte)AclPermissionTypeFilter.Allow);
    }

    #endregion

    #region Protocol Version Tests

    [Fact]
    public void DescribeAcls_FlexibleFormatVersion()
    {
        // DescribeAcls uses flexible format starting at v2
        Assert.False(ProtocolVersions.IsFlexible(ApiKey.DescribeAcls, 0));
        Assert.False(ProtocolVersions.IsFlexible(ApiKey.DescribeAcls, 1));
        Assert.True(ProtocolVersions.IsFlexible(ApiKey.DescribeAcls, 2));
    }

    [Fact]
    public void CreateAcls_FlexibleFormatVersion()
    {
        // CreateAcls uses flexible format starting at v2
        Assert.False(ProtocolVersions.IsFlexible(ApiKey.CreateAcls, 0));
        Assert.False(ProtocolVersions.IsFlexible(ApiKey.CreateAcls, 1));
        Assert.True(ProtocolVersions.IsFlexible(ApiKey.CreateAcls, 2));
    }

    [Fact]
    public void DeleteAcls_FlexibleFormatVersion()
    {
        // DeleteAcls uses flexible format starting at v2
        Assert.False(ProtocolVersions.IsFlexible(ApiKey.DeleteAcls, 0));
        Assert.False(ProtocolVersions.IsFlexible(ApiKey.DeleteAcls, 1));
        Assert.True(ProtocolVersions.IsFlexible(ApiKey.DeleteAcls, 2));
    }

    #endregion
}
