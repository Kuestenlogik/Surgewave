using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — Topic admin payloads. Covers the
/// <c>Write(ref SurgewavePayloadWriter)</c>-shaped payloads in the
/// Topics sub-namespace:
/// <see cref="TopicConfigPayload"/>,
/// <see cref="DescribeConfigRequestPayload"/> + Response,
/// <see cref="AlterConfigRequestPayload"/>,
/// <see cref="CreatePartitionsRequestPayload"/>,
/// <see cref="DeleteRecordsRequestPayload"/> + Response,
/// <see cref="DescribeTopicRequestPayload"/>.
///
/// <see cref="DescribeTopicResponsePayload"/> and
/// <see cref="PartitionMetadataPayload"/> only expose
/// <c>WriteTo(IPayloadWriter)</c> — covering them needs a managed-class
/// IPayloadWriter adapter that doesn't yet live in the Protocol.Native
/// assembly. Documented follow-up.
/// </summary>
public sealed class TopicAdminPayloadRoundTripTests
{
    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // TopicConfig (used by Alter/Describe Config payloads)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void TopicConfigPayload_RoundTrip_PreservesKeyAndValue()
    {
        var original = new TopicConfigPayload { Key = "retention.ms", Value = "604800000" };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return TopicConfigPayload.Read(ref r); });
        Assert.Equal("retention.ms", parsed.Key);
        Assert.Equal("604800000", parsed.Value);
    }

    // ───────────────────────────────────────────────────────────────
    // DescribeConfig
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DescribeConfigRequest_RoundTrip_PreservesTopicName()
    {
        var original = new DescribeConfigRequestPayload { TopicName = "orders" };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeConfigRequestPayload.Read(ref r); });
        Assert.Equal("orders", parsed.TopicName);
    }

    [Fact]
    public void DescribeConfigResponse_RoundTrip_PreservesConfigList()
    {
        var original = new DescribeConfigResponsePayload
        {
            TopicName = "orders",
            Configs = new[]
            {
                new TopicConfigPayload { Key = "cleanup.policy", Value = "compact" },
                new TopicConfigPayload { Key = "retention.ms", Value = "-1" },
                new TopicConfigPayload { Key = "min.insync.replicas", Value = "2" },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeConfigResponsePayload.Read(ref r); });

        Assert.Equal("orders", parsed.TopicName);
        Assert.Equal(3, parsed.Configs.Length);
        Assert.Equal("compact", parsed.Configs[0].Value);
        Assert.Equal("-1", parsed.Configs[1].Value);
        Assert.Equal("min.insync.replicas", parsed.Configs[2].Key);
    }

    [Fact]
    public void DescribeConfigResponse_EmptyConfigList_RoundTrips()
    {
        var original = new DescribeConfigResponsePayload
        {
            TopicName = "no-config",
            Configs = Array.Empty<TopicConfigPayload>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeConfigResponsePayload.Read(ref r); });
        Assert.Empty(parsed.Configs);
    }

    // ───────────────────────────────────────────────────────────────
    // AlterConfig
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void AlterConfigRequest_RoundTrip_PreservesConfigList()
    {
        var original = new AlterConfigRequestPayload
        {
            TopicName = "events",
            Configs = new[]
            {
                new TopicConfigPayload { Key = "retention.ms", Value = "86400000" },
                new TopicConfigPayload { Key = "max.message.bytes", Value = "10485760" },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return AlterConfigRequestPayload.Read(ref r); });

        Assert.Equal("events", parsed.TopicName);
        Assert.Equal(2, parsed.Configs.Length);
        Assert.Equal("86400000", parsed.Configs[0].Value);
        Assert.Equal("max.message.bytes", parsed.Configs[1].Key);
    }

    // ───────────────────────────────────────────────────────────────
    // CreatePartitions
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void CreatePartitionsRequest_RoundTrip_PreservesAllFields()
    {
        var original = new CreatePartitionsRequestPayload { TopicName = "events", TotalPartitions = 32 };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return CreatePartitionsRequestPayload.Read(ref r); });
        Assert.Equal("events", parsed.TopicName);
        Assert.Equal(32, parsed.TotalPartitions);
    }

    // ───────────────────────────────────────────────────────────────
    // DeleteRecords
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DeleteRecordsRequest_RoundTrip_PreservesAllFields()
    {
        var original = new DeleteRecordsRequestPayload
        {
            TopicName = "events",
            Partition = 3,
            BeforeOffset = 1_000_000L,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DeleteRecordsRequestPayload.Read(ref r); });

        Assert.Equal("events", parsed.TopicName);
        Assert.Equal(3, parsed.Partition);
        Assert.Equal(1_000_000L, parsed.BeforeOffset);
    }

    [Fact]
    public void DeleteRecordsResponse_RoundTrip_PreservesLowWatermark()
    {
        var original = new DeleteRecordsResponsePayload
        {
            TopicName = "events",
            Partition = 3,
            LowWatermark = 999_999L,
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DeleteRecordsResponsePayload.Read(ref r); });

        Assert.Equal(999_999L, parsed.LowWatermark);
    }

    // ───────────────────────────────────────────────────────────────
    // DescribeTopic
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DescribeTopicRequest_RoundTrip_PreservesTopicName()
    {
        var original = new DescribeTopicRequestPayload { TopicName = "orders" };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return DescribeTopicRequestPayload.Read(ref r); });
        Assert.Equal("orders", parsed.TopicName);
    }
}
