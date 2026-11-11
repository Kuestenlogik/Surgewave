using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// ApiVersions announces exactly the feature levels of <see cref="BrokerFeatureLevels"/>, and a
/// client decoding the wire reads metadata.version 14–29 finalized at 29 (#199).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ApiVersionsFeatureLevelsTests
{
    [Fact]
    public void Default_SupportedAndFinalizedFeatures_AreTheTable()
    {
        var response = ApiVersionsResponse.CreateDefault(1, 3);

        Assert.Equal(
            BrokerFeatureLevels.Announced.Select(f => (f.Name, f.MinSupported, f.MaxSupported)),
            response.SupportedFeatures!.Select(f => (f.Name, f.MinVersion, f.MaxVersion)));
        Assert.Equal(
            BrokerFeatureLevels.Announced.Select(f => (f.Name, f.Finalized, f.Finalized)),
            response.FinalizedFeatures!.Select(f => (f.Name, f.MinVersionLevel, f.MaxVersionLevel)));
    }

    [Fact]
    public void WireRoundTrip_ReadsMetadataVersion14To29_Finalized29()
    {
        using var writer = new KafkaProtocolWriter();
        ApiVersionsResponse.CreateDefault(correlationId: 7, apiVersion: 3).WriteTo(writer);

        var decoded = Decode(writer.ToArray());

        Assert.Equal(((short)14, (short)29), decoded.Supported["metadata.version"]);
        Assert.Equal(((short)29, (short)29), decoded.Finalized["metadata.version"]);
        foreach (var feature in BrokerFeatureLevels.Announced)
        {
            Assert.Equal((feature.MinSupported, feature.MaxSupported), decoded.Supported[feature.Name]);
            Assert.Equal((feature.Finalized, feature.Finalized), decoded.Finalized[feature.Name]);
        }
    }

    private sealed record DecodedFeatures(
        Dictionary<string, (short Min, short Max)> Supported,
        Dictionary<string, (short Min, short Max)> Finalized);

    /// <summary>
    /// Decodes an ApiVersions v3 response as Kafka's ApiVersionsResponse.json defines it,
    /// independent of the writer: header without tagged fields, ErrorCode, ApiKeys, ThrottleTimeMs,
    /// then tagged fields 0 (SupportedFeatures: name, min, max) and 2 (FinalizedFeatures: name,
    /// max level, min level).
    /// </summary>
    private static DecodedFeatures Decode(byte[] bytes)
    {
        var reader = new KafkaProtocolReader(bytes);
        reader.ReadInt32(); // correlation id
        Assert.Equal(0, reader.ReadInt16()); // error code
        var apiKeys = reader.ReadVarInt() - 1;
        for (var i = 0; i < apiKeys; i++)
        {
            reader.ReadInt16();
            reader.ReadInt16();
            reader.ReadInt16();
            Assert.Equal(0, reader.ReadVarInt()); // no tagged fields per key
        }
        reader.ReadInt32(); // throttle time

        var supported = new Dictionary<string, (short, short)>();
        var finalized = new Dictionary<string, (short, short)>();
        var tags = reader.ReadVarInt();
        for (var t = 0; t < tags; t++)
        {
            var tag = reader.ReadVarInt();
            var size = reader.ReadVarInt();
            var start = reader.Position;
            switch (tag)
            {
                case 0:
                    ReadFeatures(reader, supported, maxFirst: false);
                    break;
                case 1:
                    reader.ReadInt64(); // finalized features epoch
                    break;
                case 2:
                    ReadFeatures(reader, finalized, maxFirst: true);
                    break;
                default:
                    Assert.Fail($"unexpected tag {tag}");
                    break;
            }
            Assert.Equal(size, reader.Position - start);
        }

        Assert.Equal(0, reader.Remaining);
        return new DecodedFeatures(supported, finalized);
    }

    private static void ReadFeatures(KafkaProtocolReader reader, Dictionary<string, (short, short)> into, bool maxFirst)
    {
        var count = reader.ReadVarInt() - 1;
        for (var i = 0; i < count; i++)
        {
            var name = reader.ReadCompactString()!;
            var first = reader.ReadInt16();
            var second = reader.ReadInt16();
            Assert.Equal(0, reader.ReadVarInt()); // no tagged fields per feature
            into[name] = maxFirst ? (second, first) : (first, second);
        }
    }
}
