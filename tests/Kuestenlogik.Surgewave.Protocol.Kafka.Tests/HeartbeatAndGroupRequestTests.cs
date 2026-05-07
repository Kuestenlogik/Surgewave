using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for Heartbeat, JoinGroup, SyncGroup, LeaveGroup, ListGroups, DescribeGroups requests and responses.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class HeartbeatAndGroupRequestTests
{
    private readonly KafkaProtocolHandler _handler = new();

    #region HeartbeatRequest Tests

    [Fact]
    public void HeartbeatRequest_Parse_V0()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(12);   // ApiKey = Heartbeat
        writer.WriteInt16(0);    // ApiVersion = 0
        writer.WriteInt32(1);    // CorrelationId
        writer.WriteString("client");
        writer.WriteString("my-group");    // GroupId
        writer.WriteInt32(5);              // GenerationId
        writer.WriteString("member-abc"); // MemberId

        // Act
        var request = (HeartbeatRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.Equal("my-group", request.GroupId);
        Assert.Equal(5, request.GenerationId);
        Assert.Equal("member-abc", request.MemberId);
        Assert.Null(request.GroupInstanceId);
    }

    [Fact]
    public void HeartbeatRequest_Parse_V3_WithGroupInstanceId()
    {
        // Arrange - v3 adds GroupInstanceId
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(12);      // ApiKey = Heartbeat
        writer.WriteInt16(3);       // ApiVersion = 3
        writer.WriteInt32(2);       // CorrelationId
        writer.WriteString("client");
        writer.WriteString("consumer-group");
        writer.WriteInt32(10);
        writer.WriteString("member-id-1");
        writer.WriteString("instance-1"); // GroupInstanceId

        // Act
        var request = (HeartbeatRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.Equal("instance-1", request.GroupInstanceId);
    }

    [Fact]
    public void HeartbeatResponse_WriteTo_V0()
    {
        // Arrange
        var response = new HeartbeatResponse
        {
            CorrelationId = 1,
            ApiVersion = 0,
            ErrorCode = ErrorCode.None
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert - v0: CorrelationId(4) + ErrorCode(2)
        Assert.Equal(6, bytes.Length);
        var correlationId = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(bytes);
        Assert.Equal(1, correlationId);
        var errorCode = (ErrorCode)System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(bytes.AsSpan(4));
        Assert.Equal(ErrorCode.None, errorCode);
    }

    [Fact]
    public void HeartbeatResponse_WriteTo_V1_IncludesThrottleTime()
    {
        // Arrange - v1 adds ThrottleTimeMs
        var response = new HeartbeatResponse
        {
            CorrelationId = 1,
            ApiVersion = 1,
            ErrorCode = ErrorCode.RebalanceInProgress
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert - v1: CorrelationId(4) + ThrottleTimeMs(4) + ErrorCode(2)
        Assert.Equal(10, bytes.Length);
    }

    [Fact]
    public void HeartbeatResponse_V4_FlexibleFormat()
    {
        // Arrange - v4 is flexible
        var response = new HeartbeatResponse
        {
            CorrelationId = 5,
            ApiVersion = 4,
            ErrorCode = ErrorCode.None
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert - should be larger than v0 due to tagged fields
        Assert.True(bytes.Length > 6);
    }

    #endregion

    #region FindCoordinatorRequest Tests

    [Fact]
    public void FindCoordinatorRequest_Parse_V0_GroupCoordinator()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(10);    // ApiKey = FindCoordinator
        writer.WriteInt16(0);     // ApiVersion = 0
        writer.WriteInt32(3);     // CorrelationId
        writer.WriteString("client");
        writer.WriteString("consumer-group"); // Key

        // Act
        var request = (FindCoordinatorRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.Equal("consumer-group", request.Key);
        Assert.Equal(3, request.CorrelationId);
    }

    [Fact]
    public void FindCoordinatorRequest_Parse_V1_WithKeyType()
    {
        // Arrange - v1 adds KeyType
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(10);    // ApiKey = FindCoordinator
        writer.WriteInt16(1);     // ApiVersion = 1
        writer.WriteInt32(4);     // CorrelationId
        writer.WriteString("client");
        writer.WriteString("txn-123"); // Key
        writer.WriteInt8(1);           // KeyType = Transaction

        // Act
        var request = (FindCoordinatorRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.Equal("txn-123", request.Key);
        Assert.Equal(1, request.KeyType); // Transaction type
    }

    [Fact]
    public void FindCoordinatorRequest_WriteTo_V0()
    {
        // Arrange
        var request = new FindCoordinatorRequest
        {
            ApiKey = ApiKey.FindCoordinator,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "client",
            Key = "my-group",
            KeyType = 0
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void FindCoordinatorResponse_WriteTo_V0()
    {
        // Arrange
        var response = new FindCoordinatorResponse
        {
            CorrelationId = 1,
            ApiVersion = 0,
            ErrorCode = ErrorCode.None,
            NodeId = 0,
            Host = "localhost",
            Port = 9092
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
        var correlationId = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(bytes);
        Assert.Equal(1, correlationId);
    }

    [Fact]
    public void FindCoordinatorResponse_V4_BatchFormat()
    {
        // Arrange - v4 uses coordinator batch format
        var response = new FindCoordinatorResponse
        {
            CorrelationId = 1,
            ApiVersion = 4,
            Coordinators = new List<FindCoordinatorResponse.Coordinator>
            {
                new()
                {
                    Key = "my-group",
                    NodeId = 0,
                    Host = "localhost",
                    Port = 9092,
                    ErrorCode = ErrorCode.None
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

    #region ListGroupsRequest Tests

    [Fact]
    public void ListGroupsRequest_Parse_V0()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(16);   // ApiKey = ListGroups
        writer.WriteInt16(0);    // ApiVersion = 0
        writer.WriteInt32(1);    // CorrelationId
        writer.WriteString("client");
        // v0 has no body fields

        // Act
        var request = _handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.NotNull(request);
        Assert.Equal(1, request.CorrelationId);
    }

    #endregion
}
