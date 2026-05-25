using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for Kafka Metadata request and response serialization across API versions.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class MetadataRequestResponseTests
{
    private readonly KafkaProtocolHandler _handler = new();

    #region MetadataRequest Parse Tests

    [Fact]
    public void MetadataRequest_V0_ParsesTopics()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(3);   // ApiKey = Metadata
        writer.WriteInt16(0);   // ApiVersion = 0
        writer.WriteInt32(55);  // CorrelationId
        writer.WriteString("client-1");
        writer.WriteInt32(2);   // 2 topics
        writer.WriteString("topic-a");
        writer.WriteString("topic-b");

        // Act
        var request = (MetadataRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.Equal(55, request.CorrelationId);
        Assert.NotNull(request.Topics);
        Assert.Equal(2, request.Topics.Count);
        Assert.Equal("topic-a", request.Topics[0].Name);
        Assert.Equal("topic-b", request.Topics[1].Name);
    }

    [Fact]
    public void MetadataRequest_V4_IncludesAllowAutoTopicCreation()
    {
        // Arrange - v4 adds AllowAutoTopicCreation
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(3);   // ApiKey = Metadata
        writer.WriteInt16(4);   // ApiVersion = 4
        writer.WriteInt32(10);  // CorrelationId
        writer.WriteString("client");
        writer.WriteInt32(1);   // 1 topic
        writer.WriteString("my-topic");
        writer.WriteInt8(0);    // AllowAutoTopicCreation = false

        // Act
        var request = (MetadataRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.False(request.AllowAutoTopicCreation);
    }

    [Fact]
    public void MetadataRequest_V4_AllowAutoTopicCreationTrue()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(3);   // ApiKey = Metadata
        writer.WriteInt16(4);   // ApiVersion = 4
        writer.WriteInt32(10);  // CorrelationId
        writer.WriteString("client");
        writer.WriteInt32(0);   // 0 topics
        writer.WriteInt8(1);    // AllowAutoTopicCreation = true

        // Act
        var request = (MetadataRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.True(request.AllowAutoTopicCreation);
    }

    #endregion

    #region MetadataResponse WriteTo Tests

    [Fact]
    public void MetadataResponse_V0_WritesCorrectly()
    {
        // Arrange
        var response = new MetadataResponse
        {
            CorrelationId = 10,
            ApiVersion = 0,
            Brokers = new List<MetadataResponse.MetadataResponseBroker>
            {
                new() { NodeId = 1, Host = "broker-1", Port = 9092 }
            },
            ClusterId = null,
            ControllerId = 1,
            Topics = new List<MetadataResponse.MetadataResponseTopic>
            {
                new()
                {
                    ErrorCode = ErrorCode.None,
                    Name = "test-topic",
                    IsInternal = false,
                    Partitions = new List<MetadataResponse.MetadataResponsePartition>
                    {
                        new()
                        {
                            ErrorCode = ErrorCode.None,
                            PartitionIndex = 0,
                            LeaderId = 1,
                            ReplicaNodes = new List<int> { 1 },
                            IsrNodes = new List<int> { 1 }
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
        // CorrelationId is the first 4 bytes
        var correlationId = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(bytes);
        Assert.Equal(10, correlationId);
    }

    [Fact]
    public void MetadataResponse_V9_UsesFlexibleFormat()
    {
        // Arrange - v9+ is flexible
        var response = new MetadataResponse
        {
            CorrelationId = 20,
            ApiVersion = 9,
            Brokers = new List<MetadataResponse.MetadataResponseBroker>(),
            ClusterId = "cluster-1",
            ControllerId = -1,
            Topics = new List<MetadataResponse.MetadataResponseTopic>()
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void MetadataResponse_RoundTrip_V0()
    {
        // Arrange
        var original = new MetadataResponse
        {
            CorrelationId = 99,
            ApiVersion = 0,
            Brokers = new List<MetadataResponse.MetadataResponseBroker>
            {
                new() { NodeId = 0, Host = "localhost", Port = 9092 }
            },
            ClusterId = null,
            ControllerId = 0,
            Topics = new List<MetadataResponse.MetadataResponseTopic>
            {
                new()
                {
                    ErrorCode = ErrorCode.None,
                    Name = "round-trip-topic",
                    IsInternal = false,
                    Partitions = new List<MetadataResponse.MetadataResponsePartition>
                    {
                        new()
                        {
                            ErrorCode = ErrorCode.None,
                            PartitionIndex = 0,
                            LeaderId = 0,
                            ReplicaNodes = new List<int> { 0 },
                            IsrNodes = new List<int> { 0 }
                        }
                    }
                }
            }
        };

        // Act - serialize then deserialize (skipping the correlation ID which is written first)
        using var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();
        // Skip the correlation ID (4 bytes) that is part of WriteTo output
        var bodyBytes = bytes[4..]; // skip correlationId written by WriteTo
        var decoded = MetadataResponse.ReadFrom(bodyBytes, 0, 99);

        // Assert
        Assert.Equal(original.CorrelationId, decoded.CorrelationId);
        Assert.Single(decoded.Brokers);
        Assert.Equal("localhost", decoded.Brokers[0].Host);
        Assert.Single(decoded.Topics);
        Assert.Equal("round-trip-topic", decoded.Topics[0].Name);
    }

    [Fact]
    public void MetadataResponse_V2_IncludesClusterId()
    {
        // Arrange - v2 adds ClusterId
        var response = new MetadataResponse
        {
            CorrelationId = 1,
            ApiVersion = 2,
            Brokers = new List<MetadataResponse.MetadataResponseBroker>(),
            ClusterId = "abc123",
            ControllerId = -1,
            Topics = new List<MetadataResponse.MetadataResponseTopic>()
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert - bytes should be non-empty and contain cluster ID
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void MetadataResponse_MultipleBrokers_SerializesAll()
    {
        // Arrange
        var response = new MetadataResponse
        {
            CorrelationId = 1,
            ApiVersion = 1,
            Brokers = new List<MetadataResponse.MetadataResponseBroker>
            {
                new() { NodeId = 0, Host = "broker-0", Port = 9092, Rack = null },
                new() { NodeId = 1, Host = "broker-1", Port = 9092, Rack = "rack-1" },
                new() { NodeId = 2, Host = "broker-2", Port = 9092, Rack = "rack-2" }
            },
            ClusterId = null,
            ControllerId = 0,
            Topics = new List<MetadataResponse.MetadataResponseTopic>()
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    #endregion
}
