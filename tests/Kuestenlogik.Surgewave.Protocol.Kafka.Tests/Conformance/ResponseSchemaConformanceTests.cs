using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests.Conformance;

/// <summary>
/// Every response the broker advertises, at every advertised version, written by the broker and read back as
/// Kafka's message schema declares it (#195): the read must use up exactly the bytes written, and every field the
/// response type exposes must come back with the value it holds. A field written in the wrong place, one left out,
/// or a struct's tagged fields left off is what a Kafka client fails on.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ResponseSchemaConformanceTests
{
    private const short ApiVersionsKey = 18;
    private static readonly string SchemaDirectory = Path.Combine(AppContext.BaseDirectory, "KafkaSchemas");

    private static readonly Dictionary<short, KafkaMessageSchema> ResponseSchemas = Directory
        .GetFiles(SchemaDirectory, "*Response.json")
        .Select(KafkaMessageSchema.Load)
        .Where(s => s.Type == "response")
        .ToDictionary(s => s.ApiKey);

    private static readonly Dictionary<string, Type> ResponseTypes = typeof(KafkaResponse).Assembly.GetTypes()
        .Where(t => typeof(KafkaResponse).IsAssignableFrom(t) && !t.IsAbstract)
        .GroupBy(t => t.Name)
        .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);

    /// <summary>Every advertised (API, version) that Kafka's schema defines.</summary>
    public static TheoryData<string, short, short> AdvertisedResponses()
    {
        var data = new TheoryData<string, short, short>();
        foreach (var api in ApiVersionsResponse.CreateDefault(0, 3).ApiVersions)
        {
            if (!ResponseSchemas.TryGetValue(api.ApiKey, out var schema))
                continue;
            for (var version = api.MinVersion; version <= api.MaxVersion; version++)
            {
                if (schema.ValidVersions.Contains(version))
                    data.Add(schema.Name, api.ApiKey, version);
            }
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(AdvertisedResponses))]
    public void AKafkaClientReadsTheResponseAsTheBrokerWroteIt(string response, short apiKey, short version)
    {
        var schema = ResponseSchemas[apiKey];
        Assert.True(ResponseTypes.TryGetValue(schema.Name, out var type), $"no response type named {schema.Name}");

        var instance = (KafkaResponse)new SampleObjectFactory().Create(type);
        SetEnvelope(instance, version);

        var writer = new KafkaProtocolWriter();
        instance.WriteTo(writer);
        var bytes = writer.ToArray();

        // Response header: the correlation id, and in flexible versions the header's tagged fields — except
        // ApiVersions, whose response header is always v0 so any client can read it.
        var offset = 4;
        if (schema.FlexibleVersions.Contains(version) && apiKey != ApiVersionsKey)
        {
            Assert.True(bytes.Length > offset && bytes[offset] == 0, $"{response} v{version}: no response header tagged fields");
            offset++;
        }

        ExpectedStruct decoded;
        int remaining;
        try
        {
            (decoded, remaining) = SchemaMessageDecoder.Decode(schema, version, bytes, offset);
        }
        catch (Exception ex)
        {
            Assert.Fail($"{response} v{version}: a Kafka client could not read it — {ex.GetType().Name}: {ex.Message}");
            return;
        }

        Assert.True(remaining == 0, $"{response} v{version}: {remaining} byte(s) left over after the schema's fields");
        var mismatches = ParsedValueComparer.Compare(instance, decoded);
        Assert.True(mismatches.Count == 0, $"{response} v{version} written wrong:\n  " + string.Join("\n  ", mismatches));
    }

    private static void SetEnvelope(KafkaResponse response, short version)
    {
        var type = response.GetType();
        type.GetProperty(nameof(KafkaResponse.CorrelationId))!.SetValue(response, 4242);
        type.GetProperty(nameof(KafkaResponse.ApiVersion))!.SetValue(response, version);
    }
}
