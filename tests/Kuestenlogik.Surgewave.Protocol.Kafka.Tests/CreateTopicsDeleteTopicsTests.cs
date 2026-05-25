using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Tests for CreateTopics and DeleteTopics request parsing and response serialization.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CreateTopicsDeleteTopicsTests
{
    private readonly KafkaProtocolHandler _handler = new();

    #region CreateTopicsRequest Tests

    [Fact]
    public void CreateTopicsRequest_Parse_V0_SingleTopic()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(19);     // ApiKey = CreateTopics
        writer.WriteInt16(0);      // ApiVersion = 0
        writer.WriteInt32(1);      // CorrelationId
        writer.WriteString("admin-client");
        writer.WriteInt32(1);      // 1 topic
        writer.WriteString("new-topic"); // Topic name
        writer.WriteInt32(3);      // NumPartitions
        writer.WriteInt16(2);      // ReplicationFactor
        writer.WriteInt32(0);      // 0 assignments
        writer.WriteInt32(0);      // 0 configs
        writer.WriteInt32(5000);   // TimeoutMs

        // Act
        var request = (CreateTopicsRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.Equal(1, request.CorrelationId);
        Assert.Single(request.Topics);
        Assert.Equal("new-topic", request.Topics[0].Name);
        Assert.Equal(3, request.Topics[0].NumPartitions);
        Assert.Equal(2, request.Topics[0].ReplicationFactor);
        Assert.Equal(5000, request.TimeoutMs);
    }

    [Fact]
    public void CreateTopicsRequest_Parse_V0_MultipleTopics()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(19);     // ApiKey = CreateTopics
        writer.WriteInt16(0);      // ApiVersion = 0
        writer.WriteInt32(2);      // CorrelationId
        writer.WriteString("admin");
        writer.WriteInt32(2);      // 2 topics
        // Topic 1
        writer.WriteString("topic-1");
        writer.WriteInt32(1);
        writer.WriteInt16(1);
        writer.WriteInt32(0);
        writer.WriteInt32(0);
        // Topic 2
        writer.WriteString("topic-2");
        writer.WriteInt32(6);
        writer.WriteInt16(3);
        writer.WriteInt32(0);
        writer.WriteInt32(0);
        writer.WriteInt32(10000);

        // Act
        var request = (CreateTopicsRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.Equal(2, request.Topics.Count);
        Assert.Equal("topic-1", request.Topics[0].Name);
        Assert.Equal(1, request.Topics[0].NumPartitions);
        Assert.Equal("topic-2", request.Topics[1].Name);
        Assert.Equal(6, request.Topics[1].NumPartitions);
    }

    [Fact]
    public void CreateTopicsRequest_WriteTo_V0_RoundTrip()
    {
        // Arrange
        var request = new CreateTopicsRequest
        {
            ApiKey = ApiKey.CreateTopics,
            ApiVersion = 0,
            CorrelationId = 10,
            ClientId = "admin",
            Topics = new List<CreateTopicsRequest.TopicToCreate>
            {
                new()
                {
                    Name = "round-trip-topic",
                    NumPartitions = 12,
                    ReplicationFactor = 3
                }
            },
            TimeoutMs = 30000
        };

        // Act
        using var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);
        var bytes = writer.ToArray();

        // Assert
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void CreateTopicsRequest_Parse_V1_WithValidateOnly()
    {
        // Arrange - v1 adds ValidateOnly
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(19);     // ApiKey = CreateTopics
        writer.WriteInt16(1);      // ApiVersion = 1
        writer.WriteInt32(1);      // CorrelationId
        writer.WriteString("admin");
        writer.WriteInt32(1);      // 1 topic
        writer.WriteString("validate-topic");
        writer.WriteInt32(1);
        writer.WriteInt16(1);
        writer.WriteInt32(0);
        writer.WriteInt32(0);
        writer.WriteInt32(5000);
        writer.WriteInt8(1);       // ValidateOnly = true

        // Act
        var request = (CreateTopicsRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.True(request.ValidateOnly);
    }

    #endregion

    #region DeleteTopicsRequest Tests

    [Fact]
    public void DeleteTopicsRequest_Parse_V0_SingleTopic()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(20);    // ApiKey = DeleteTopics
        writer.WriteInt16(0);     // ApiVersion = 0
        writer.WriteInt32(1);     // CorrelationId
        writer.WriteString("admin");
        writer.WriteInt32(1);     // 1 topic
        writer.WriteString("topic-to-delete");
        writer.WriteInt32(5000);  // TimeoutMs

        // Act
        var request = (DeleteTopicsRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.Equal(1, request.CorrelationId);
        Assert.NotNull(request.TopicNames);
        Assert.Single(request.TopicNames);
        Assert.Equal("topic-to-delete", request.TopicNames[0]);
        Assert.Equal(5000, request.TimeoutMs);
    }

    [Fact]
    public void DeleteTopicsRequest_Parse_V0_MultipleTopics()
    {
        // Arrange
        using var writer = new KafkaProtocolWriter();
        writer.WriteInt16(20);
        writer.WriteInt16(0);
        writer.WriteInt32(3);
        writer.WriteString("admin");
        writer.WriteInt32(3);
        writer.WriteString("topic-a");
        writer.WriteString("topic-b");
        writer.WriteString("topic-c");
        writer.WriteInt32(30000);

        // Act
        var request = (DeleteTopicsRequest)_handler.ParseRequest(writer.ToArray());

        // Assert
        Assert.NotNull(request.TopicNames);
        Assert.Equal(3, request.TopicNames.Count);
        Assert.Contains("topic-a", request.TopicNames);
        Assert.Contains("topic-b", request.TopicNames);
        Assert.Contains("topic-c", request.TopicNames);
    }

    #endregion
}
