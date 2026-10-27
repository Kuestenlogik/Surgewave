using System.Text;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests.Conformance;

/// <summary>
/// Every request the broker advertises, at every advertised version, written as Kafka's message schema declares it
/// (KafkaSchemas/, Kafka 46dc4b51f7) and parsed by the broker (#195). The parse must succeed and every field the
/// request type exposes must hold the value that was written. Hand-written codecs drifted from the schemas in
/// exactly this way — a field left out, a tagged field read as a fixed one, a header misread — and only a client
/// speaking the newest versions (Java 4.x) ever hit it.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed partial class RequestSchemaConformanceTests
{
    private static readonly string SchemaDirectory = Path.Combine(AppContext.BaseDirectory, "KafkaSchemas");

    private static readonly Dictionary<short, KafkaMessageSchema> RequestSchemas = Directory
        .GetFiles(SchemaDirectory, "*Request.json")
        .Select(KafkaMessageSchema.Load)
        .Where(s => s.Type == "request")
        .ToDictionary(s => s.ApiKey);

    /// <summary>Every advertised (API, version) that Kafka's schema defines.</summary>
    public static TheoryData<string, short, short> AdvertisedRequests()
    {
        var data = new TheoryData<string, short, short>();
        foreach (var api in ApiVersionsResponse.CreateDefault(0, 3).ApiVersions)
        {
            if (!RequestSchemas.TryGetValue(api.ApiKey, out var schema))
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
    [MemberData(nameof(AdvertisedRequests))]
    public void TheBrokerReadsTheRequestAsTheSchemaDeclaresIt(string request, short apiKey, short version)
    {
        var schema = RequestSchemas[apiKey];
        var (body, expected) = SchemaMessageEncoder.Encode(schema, version);
        var message = WithHeader(apiKey, version, schema.FlexibleVersions.Contains(version), body);

        KafkaRequest parsed;
        try
        {
            parsed = KafkaProtocolHandler.ParseRequest(message, message.Length);
        }
        catch (Exception ex)
        {
            Assert.Fail($"{request} v{version}: the broker could not parse it — {ex.GetType().Name}: {ex.Message}");
            return;
        }

        var unexposed = new List<string>();
        var mismatches = ParsedValueComparer.Compare(parsed, expected, unexposed: unexposed);
        Assert.True(mismatches.Count == 0, $"{request} v{version} read wrong:\n  " + string.Join("\n  ", mismatches));

        // A field the request type does not expose is either there under another name or deliberately unused —
        // each listed in NotExposed with its reason. Anything else is a field the parser may silently not read,
        // as ListGroups' TypesFilter and InitProducerId's KIP-939 flags were.
        var unlisted = unexposed
            .Select(path => IndexSuffix().Replace(path, string.Empty))
            .Where(path => !NotExposed.ContainsKey($"{request}: {path}"))
            .Distinct()
            .ToList();
        Assert.True(unlisted.Count == 0,
            $"{request} v{version}: schema field(s) the request type does not expose — read them, or list them in NotExposed with the reason: {string.Join(", ", unlisted)}");
    }

    /// <summary>Schema fields the request types do not expose under the schema's name, and why that is right.</summary>
    private static readonly Dictionary<string, string> NotExposed = new(StringComparer.Ordinal)
    {
        ["AddPartitionsToTxnRequest: V3AndBelowTransactionalId"] = "exposed as TransactionalId",
        ["AddPartitionsToTxnRequest: V3AndBelowProducerId"] = "exposed as ProducerId",
        ["AddPartitionsToTxnRequest: V3AndBelowProducerEpoch"] = "exposed as ProducerEpoch",
        ["AddPartitionsToTxnRequest: V3AndBelowTopics"] = "exposed as Topics",
        ["AlterReplicaLogDirsRequest: Dirs.Topics.Name"] = "exposed as Topic",
        ["CreateAclsRequest: Creations.ResourcePatternType"] = "exposed as PatternType",
        ["CreatePartitionsRequest: Topics.Assignments.BrokerIds"] = "read into the assignment list",
        ["CreateTopicsRequest: Topics.Assignments.PartitionIndex"] = "exposed as Partition",
        ["CreateTopicsRequest: Topics.Configs.Name"] = "the configs are a name-to-value dictionary",
        ["DeleteAclsRequest: Filters.Operation"] = "exposed as OperationFilter",
        ["DeleteAclsRequest: Filters.PermissionType"] = "exposed as PermissionTypeFilter",
        ["DeleteGroupsRequest: GroupsNames"] = "exposed as GroupIds",
        ["DeleteRecordsRequest: Topics.Name"] = "exposed as Topic",
        ["DeleteRecordsRequest: Topics.Partitions.PartitionIndex"] = "exposed as Partition",
        ["DeleteTopicsRequest: Topics"] = "exposed as TopicNames",
        ["DescribeAclsRequest: Operation"] = "exposed as OperationFilter",
        ["DescribeAclsRequest: PermissionType"] = "exposed as PermissionTypeFilter",
        ["DescribeGroupsRequest: Groups"] = "exposed as GroupIds",
        ["DescribeTopicPartitionsRequest: Cursor"] = "exposed as StartingCursor",
        ["ElectLeadersRequest: TopicPartitions"] = "exposed as TopicPartitionsList",
        ["FetchRequest: Topics.Partitions.PartitionMaxBytes"] = "exposed as MaxBytes",
        ["FetchRequest: Topics.Partitions.HighWatermark"] = "a leader's high watermark sent to followers; skipped, Surgewave replicates over its own protocol",
        ["ListOffsetsRequest: Topics.Name"] = "the topic name is the key of Topics",
        ["MetadataRequest: IncludeClusterAuthorizedOperations"] = "read and not used: cluster authorized operations (v8-10) are not answered",
        ["OffsetCommitRequest: Topics.Name"] = "the topic name is the key of Topics",
        ["OffsetCommitRequest: Topics.Partitions.CommittedMetadata"] = "exposed as Metadata",
        ["OffsetFetchRequest: Groups.Topics.Name"] = "the topic name is the key of Topics",
        ["OffsetFetchRequest: Topics.Name"] = "the topic name is the key of Topics",
        ["ProduceRequest: Acks"] = "exposed as RequiredAcks",
        ["TxnOffsetCommitRequest: GenerationIdOrMemberEpoch"] = "exposed as GenerationId",
        ["TxnOffsetCommitRequest: Topics.Partitions.PartitionIndex"] = "exposed as Partition",
        ["TxnOffsetCommitRequest: Topics.Partitions.CommittedMetadata"] = "exposed as Metadata",
    };

    [System.Text.RegularExpressions.GeneratedRegex(@"\[\d+\]")]
    private static partial System.Text.RegularExpressions.Regex IndexSuffix();

    /// <summary>Request header v1 (classic) or v2 (flexible): the client id is a classic string in both.</summary>
    private static byte[] WithHeader(short apiKey, short version, bool flexible, byte[] body)
    {
        var output = new MemoryStream();
        SchemaMessageEncoder.WriteInt16(output, apiKey);
        SchemaMessageEncoder.WriteInt16(output, version);
        SchemaMessageEncoder.WriteInt32(output, 4242);
        var clientId = Encoding.UTF8.GetBytes("conformance");
        SchemaMessageEncoder.WriteInt16(output, (short)clientId.Length);
        output.Write(clientId);
        if (flexible)
            SchemaMessageEncoder.WriteUnsignedVarInt(output, 0);
        output.Write(body);
        return output.ToArray();
    }
}
