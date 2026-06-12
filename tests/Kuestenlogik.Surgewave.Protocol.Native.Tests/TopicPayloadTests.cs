using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Tests for topic-related payload serialization/deserialization
/// </summary>
public sealed class TopicPayloadTests
{
    [Fact]
    public void CreateTopicRequest_RoundTrip_NoConfigs()
    {
        var payload = new CreateTopicRequestPayload
        {
            Name = "my-topic",
            Partitions = 4,
            ReplicationFactor = 2,
            Configs = null
        };

        var size = payload.EstimateSize();
        var buffer = new byte[size + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = CreateTopicRequestPayload.Read(ref reader);

        Assert.Equal("my-topic", parsed.Name);
        Assert.Equal(4, parsed.Partitions);
        Assert.Equal(2, parsed.ReplicationFactor);
        Assert.Null(parsed.Configs);
    }

    [Fact]
    public void CreateTopicRequest_RoundTrip_WithConfigs()
    {
        var payload = new CreateTopicRequestPayload
        {
            Name = "configured-topic",
            Partitions = 8,
            ReplicationFactor = 3,
            Configs =
            [
                new TopicConfigPayload { Key = "retention.ms", Value = "604800000" },
                new TopicConfigPayload { Key = "cleanup.policy", Value = "delete" }
            ]
        };

        var size = payload.EstimateSize();
        var buffer = new byte[size + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = CreateTopicRequestPayload.Read(ref reader);

        Assert.Equal("configured-topic", parsed.Name);
        Assert.Equal(8, parsed.Partitions);
        Assert.Equal(3, parsed.ReplicationFactor);
        Assert.NotNull(parsed.Configs);
        Assert.Equal(2, parsed.Configs.Length);
        Assert.Equal("retention.ms", parsed.Configs[0].Key);
        Assert.Equal("604800000", parsed.Configs[0].Value);
        Assert.Equal("cleanup.policy", parsed.Configs[1].Key);
        Assert.Equal("delete", parsed.Configs[1].Value);
    }

    [Fact]
    public void DeleteTopicRequest_RoundTrip()
    {
        var payload = new DeleteTopicRequestPayload { Name = "topic-to-delete" };

        var buffer = new byte[payload.EstimateSize() + 5];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = DeleteTopicRequestPayload.Read(ref reader);

        Assert.Equal("topic-to-delete", parsed.Name);
    }

    [Fact]
    public void TopicInfoPayload_RoundTrip()
    {
        var payload = new TopicInfoPayload { Name = "events", PartitionCount = 12 };

        var buffer = new byte[payload.EstimateSize() + 5];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = TopicInfoPayload.Read(ref reader);

        Assert.Equal("events", parsed.Name);
        Assert.Equal(12, parsed.PartitionCount);
        // Default strategy is Replicated — the existing pre-G21 callers that
        // don't set the field still get the right behavior.
        Assert.Equal(ProduceStrategy.Replicated, parsed.Strategy);
    }

    [Theory]
    [InlineData(ProduceStrategy.Replicated)]
    [InlineData(ProduceStrategy.WalViaBroker)]
    [InlineData(ProduceStrategy.StatelessViaBroker)]
    [InlineData(ProduceStrategy.StatelessDirect)]
    public void TopicInfoPayload_RoundTrip_PreservesStrategy(ProduceStrategy strategy)
    {
        var payload = new TopicInfoPayload
        {
            Name = "orders",
            PartitionCount = 4,
            Strategy = strategy,
        };

        var buffer = new byte[payload.EstimateSize() + 5];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = TopicInfoPayload.Read(ref reader);

        Assert.Equal(strategy, parsed.Strategy);
    }

    [Fact]
    public void ListTopicsResponse_RoundTrip_PreservesPerTopicStrategy()
    {
        // Verifies that the per-topic strategy survives an array round-trip.
        // The risk regression-target: a missing per-entry byte in Write would
        // misalign the second entry's Name and surface as a corrupted string.
        var payload = new ListTopicsResponsePayload
        {
            Topics =
            [
                new TopicInfoPayload { Name = "fast", PartitionCount = 1, Strategy = ProduceStrategy.Replicated },
                new TopicInfoPayload { Name = "cheap", PartitionCount = 2, Strategy = ProduceStrategy.StatelessViaBroker },
                new TopicInfoPayload { Name = "warm", PartitionCount = 3, Strategy = ProduceStrategy.WalViaBroker },
            ]
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = ListTopicsResponsePayload.Read(ref reader);

        Assert.Equal(3, parsed.Topics.Length);
        Assert.Equal("fast", parsed.Topics[0].Name);
        Assert.Equal(ProduceStrategy.Replicated, parsed.Topics[0].Strategy);
        Assert.Equal("cheap", parsed.Topics[1].Name);
        Assert.Equal(ProduceStrategy.StatelessViaBroker, parsed.Topics[1].Strategy);
        Assert.Equal("warm", parsed.Topics[2].Name);
        Assert.Equal(ProduceStrategy.WalViaBroker, parsed.Topics[2].Strategy);
    }

    [Fact]
    public void TopicConfigPayload_RoundTrip()
    {
        var payload = new TopicConfigPayload { Key = "max.message.bytes", Value = "1048576" };

        var buffer = new byte[payload.EstimateSize() + 5];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = TopicConfigPayload.Read(ref reader);

        Assert.Equal("max.message.bytes", parsed.Key);
        Assert.Equal("1048576", parsed.Value);
    }

    [Fact]
    public void ListTopicsResponse_RoundTrip_EmptyList()
    {
        var payload = new ListTopicsResponsePayload { Topics = [] };

        var buffer = new byte[payload.EstimateSize() + 5];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = ListTopicsResponsePayload.Read(ref reader);

        Assert.NotNull(parsed.Topics);
        Assert.Empty(parsed.Topics);
    }

    [Fact]
    public void ListTopicsResponse_RoundTrip_MultipleTopics()
    {
        var payload = new ListTopicsResponsePayload
        {
            Topics =
            [
                new TopicInfoPayload { Name = "orders", PartitionCount = 4 },
                new TopicInfoPayload { Name = "payments", PartitionCount = 8 },
                new TopicInfoPayload { Name = "events", PartitionCount = 16 }
            ]
        };

        var buffer = new byte[payload.EstimateSize() + 10];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        var reader = new SurgewavePayloadReader(buffer);
        var parsed = ListTopicsResponsePayload.Read(ref reader);

        Assert.Equal(3, parsed.Topics.Length);
        Assert.Equal("orders", parsed.Topics[0].Name);
        Assert.Equal(4, parsed.Topics[0].PartitionCount);
        Assert.Equal("payments", parsed.Topics[1].Name);
        Assert.Equal(8, parsed.Topics[1].PartitionCount);
        Assert.Equal("events", parsed.Topics[2].Name);
        Assert.Equal(16, parsed.Topics[2].PartitionCount);
    }

    [Fact]
    public void CreateTopicRequest_EstimateSize_IsAccurate()
    {
        var payload = new CreateTopicRequestPayload
        {
            Name = "test",
            Partitions = 1,
            ReplicationFactor = 1,
            Configs = null
        };

        var estimated = payload.EstimateSize();
        var buffer = new byte[estimated + 20];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);

        // Actual written size should be <= estimated (estimate is conservative)
        Assert.True(writer.Position <= estimated + 5);
    }

    [Fact]
    public void TopicInfoPayload_EstimateSize_IsAccurate()
    {
        var payload = new TopicInfoPayload { Name = "test-topic", PartitionCount = 4 };
        var estimated = payload.EstimateSize();
        // Name: 2 (len prefix) + 10 (bytes) + 4 (partitions) + 1 (strategy byte) = 17 (G21/P4)
        Assert.Equal(17, estimated);
    }
}
