using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// KIP-1242 — ApiVersions v5 adds ClusterId + NodeId to the request and
/// REBOOTSTRAP_REQUIRED (error code 129) to the response. Both fields are
/// flagged <c>ignorable: true</c> in the upstream schema, but the broker
/// still has to PARSE them so the trailing tagged-fields varint lines up.
/// These tests pin the wire framing — without v5 parsing, a v5 client would
/// either get an UnsupportedVersion error or the broker would mis-read the
/// tagged-fields count out of the wrong byte.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1242ApiVersionsV5Tests
{
    [Fact]
    public void RoundTrip_V5_PreservesClusterIdAndNodeId()
    {
        var original = new ApiVersionsRequest
        {
            ApiKey = ApiKey.ApiVersions,
            ApiVersion = 5,
            CorrelationId = 42,
            ClientId = "surgewave-kip1242-test",
            ClientSoftwareName = "confluent-kafka-dotnet",
            ClientSoftwareVersion = "2.13.0",
            ClusterId = "surgewave-cluster-7f3c",
            NodeId = 3,
        };

        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var payload = writer.ToArray();

        var reader = new KafkaProtocolReader(payload);
        var parsed = ApiVersionsRequest.ReadFrom(reader, apiVersion: 5, correlationId: 42, clientId: "surgewave-kip1242-test");

        Assert.Equal("confluent-kafka-dotnet", parsed.ClientSoftwareName);
        Assert.Equal("2.13.0", parsed.ClientSoftwareVersion);
        Assert.Equal("surgewave-cluster-7f3c", parsed.ClusterId);
        Assert.Equal(3, parsed.NodeId);
        Assert.Equal(0, reader.Remaining); // every byte consumed → no framing drift
    }

    [Fact]
    public void RoundTrip_V5_NullClusterIdAndDefaultNodeId_AreLegal()
    {
        // Spec: ClusterId nullable @v5+, NodeId default -1. Modern clients
        // that haven't yet learned the cluster's identity send this shape on
        // the first connect.
        var original = new ApiVersionsRequest
        {
            ApiKey = ApiKey.ApiVersions,
            ApiVersion = 5,
            CorrelationId = 1,
            ClientId = "first-connect",
            ClientSoftwareName = "librdkafka",
            ClientSoftwareVersion = "2.16.0",
            ClusterId = null,
            NodeId = -1,
        };

        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray());
        var parsed = ApiVersionsRequest.ReadFrom(reader, apiVersion: 5, correlationId: 1, clientId: "first-connect");

        Assert.Null(parsed.ClusterId);
        Assert.Equal(-1, parsed.NodeId);
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void V4_RoundTrip_DoesNotEmitV5Fields()
    {
        // Regression guard: at v4 the wire must NOT include ClusterId/NodeId
        // even if the request object happens to carry them — otherwise a v4
        // client would mis-parse the tagged-fields varint that follows.
        var original = new ApiVersionsRequest
        {
            ApiKey = ApiKey.ApiVersions,
            ApiVersion = 4,
            CorrelationId = 7,
            ClientId = "v4-client",
            ClientSoftwareName = "java",
            ClientSoftwareVersion = "3.9.0",
            ClusterId = "should-not-be-on-the-wire",
            NodeId = 99,
        };

        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray());
        var parsed = ApiVersionsRequest.ReadFrom(reader, apiVersion: 4, correlationId: 7, clientId: "v4-client");

        Assert.Null(parsed.ClusterId);
        Assert.Equal(-1, parsed.NodeId);
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void RebootstrapRequired_IsErrorCode129()
    {
        // KIP-1242 specifies REBOOTSTRAP_REQUIRED = 129 (canonical from
        // Apache Kafka's Errors.java). Client libraries hard-code this value
        // when remapping ApiVersions v5 errors, so a drift here would silently
        // break the rebootstrap path on real clients.
        Assert.Equal(129, (short)ErrorCode.RebootstrapRequired);
    }
}
