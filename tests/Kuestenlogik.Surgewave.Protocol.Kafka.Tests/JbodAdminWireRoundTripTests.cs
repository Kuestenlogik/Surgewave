using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Coverage-push batch — JBOD (KIP-858) admin RPCs.
/// Covers <see cref="AlterReplicaLogDirsRequest"/> + Response
/// (API key 34, v0-2 — v0/v1 non-flexible, v2 flexible) and
/// <see cref="AssignReplicasToDirsRequest"/> + Response (API key 73,
/// v0+ flexible).
///
/// Together they're the JBOD admin surface — AlterReplicaLogDirs is
/// the legacy "move partitions between disks" RPC, AssignReplicasToDirs
/// is the KIP-858 modern version that uses topic UUIDs and directory
/// UUIDs instead of names + paths.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class JbodAdminWireRoundTripTests
{
    private static readonly Guid DirIdA = new("11111111-1111-1111-1111-111111111111");
    private static readonly Guid DirIdB = new("22222222-2222-2222-2222-222222222222");
    private static readonly Guid TopicIdA = new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly Guid TopicIdB = new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    private static KafkaProtocolReader SkipFlexibleHeader(byte[] payload)
    {
        var reader = new KafkaProtocolReader(payload);
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadCompactString(); reader.SkipTaggedFields();
        return reader;
    }

    private static KafkaProtocolReader SkipV0HeaderNonFlexible(byte[] payload)
    {
        var reader = new KafkaProtocolReader(payload);
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadString(); // non-compact ClientId
        return reader;
    }

    // ───────────────────────────────────────────────────────────────
    // AlterReplicaLogDirs (API key 34, v0-2)
    // ───────────────────────────────────────────────────────────────

    private static AlterReplicaLogDirsRequest NewAlterReplicaRequest(short apiVersion) => new()
    {
        ApiKey = ApiKey.AlterReplicaLogDirs,
        ApiVersion = apiVersion,
        CorrelationId = 1,
        ClientId = "jbod-admin",
        Dirs =
        [
            new AlterReplicaLogDirsRequest.DirEntry
            {
                Path = "/var/kafka/data-1",
                Topics =
                [
                    new AlterReplicaLogDirsRequest.TopicEntry
                    {
                        Topic = "orders",
                        Partitions = [0, 1, 2],
                    },
                    new AlterReplicaLogDirsRequest.TopicEntry
                    {
                        Topic = "events",
                        Partitions = [0],
                    },
                ],
            },
            new AlterReplicaLogDirsRequest.DirEntry
            {
                Path = "/var/kafka/data-2",
                Topics =
                [
                    new AlterReplicaLogDirsRequest.TopicEntry
                    {
                        Topic = "audit",
                        Partitions = [0, 1],
                    },
                ],
            },
        ],
    };

    [Fact]
    public void AlterReplicaLogDirsRequest_V1_NonFlexible_RoundTrips()
    {
        var original = NewAlterReplicaRequest(apiVersion: 1);
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipV0HeaderNonFlexible(writer.ToArray());
        var parsed = AlterReplicaLogDirsRequest.ReadFrom(reader, apiVersion: 1, correlationId: 1, clientId: "jbod-admin");

        Assert.Equal(2, parsed.Dirs.Count);
        Assert.Equal("/var/kafka/data-1", parsed.Dirs[0].Path);
        Assert.Equal(2, parsed.Dirs[0].Topics.Count);
        Assert.Equal("orders", parsed.Dirs[0].Topics[0].Topic);
        Assert.Equal(new[] { 0, 1, 2 }, parsed.Dirs[0].Topics[0].Partitions);
        Assert.Equal("events", parsed.Dirs[0].Topics[1].Topic);
        Assert.Equal("/var/kafka/data-2", parsed.Dirs[1].Path);
        Assert.Single(parsed.Dirs[1].Topics);
    }

    [Fact]
    public void AlterReplicaLogDirsRequest_V2_Flexible_RoundTrips()
    {
        var original = NewAlterReplicaRequest(apiVersion: 2);
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterReplicaLogDirsRequest.ReadFrom(reader, apiVersion: 2, correlationId: 1, clientId: "jbod-admin");

        Assert.Equal(2, parsed.Dirs.Count);
        Assert.Equal("/var/kafka/data-1", parsed.Dirs[0].Path);
        Assert.Equal(new[] { 0, 1, 2 }, parsed.Dirs[0].Topics[0].Partitions);
    }

    [Fact]
    public void AlterReplicaLogDirsResponse_V1_PartialFailure_RoundTrips()
    {
        // Surgewave's KIP-113 binding rejects with LOG_DIR_NOT_FOUND (57)
        // per kips.md — Surgewave has a single data dir per broker.
        var original = new AlterReplicaLogDirsResponse
        {
            ApiVersion = 1,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            Results =
            [
                new AlterReplicaLogDirsResponse.TopicResult
                {
                    Topic = "orders",
                    Partitions =
                    [
                        new AlterReplicaLogDirsResponse.PartitionResult
                        {
                            PartitionIndex = 0,
                            ErrorCode = (ErrorCode)57, // LOG_DIR_NOT_FOUND
                        },
                        new AlterReplicaLogDirsResponse.PartitionResult
                        {
                            PartitionIndex = 1,
                            ErrorCode = (ErrorCode)57,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        // Response: v1 non-flexible. CorrelationId(4) → ThrottleTime → Results.
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterReplicaLogDirsResponse.ReadFrom(reader, apiVersion: 1, correlationId: 1);

        Assert.Single(parsed.Results);
        Assert.Equal(2, parsed.Results[0].Partitions.Count);
        Assert.Equal((ErrorCode)57, parsed.Results[0].Partitions[0].ErrorCode);
        Assert.Equal((ErrorCode)57, parsed.Results[0].Partitions[1].ErrorCode);
    }

    [Fact]
    public void AlterReplicaLogDirsResponse_V2_Flexible_Success_RoundTrips()
    {
        var original = new AlterReplicaLogDirsResponse
        {
            ApiVersion = 2,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            Results =
            [
                new AlterReplicaLogDirsResponse.TopicResult
                {
                    Topic = "events",
                    Partitions =
                    [
                        new AlterReplicaLogDirsResponse.PartitionResult { PartitionIndex = 0, ErrorCode = ErrorCode.None },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        // v2 flexible response: CorrelationId(4) + header tag-varint(1) before body.
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterReplicaLogDirsResponse.ReadFrom(reader, apiVersion: 2, correlationId: 1);

        Assert.Single(parsed.Results);
        Assert.Equal(ErrorCode.None, parsed.Results[0].Partitions[0].ErrorCode);
    }

    [Fact]
    public void AlterReplicaLogDirsResponse_EmptyResults_RoundTrips()
    {
        var original = new AlterReplicaLogDirsResponse
        {
            ApiVersion = 2,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            Results = [],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterReplicaLogDirsResponse.ReadFrom(reader, apiVersion: 2, correlationId: 1);
        Assert.Empty(parsed.Results);
    }

    // ───────────────────────────────────────────────────────────────
    // AssignReplicasToDirs (API key 73, v0)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void AssignReplicasToDirsRequest_FullShape_RoundTrips()
    {
        // Broker 3 assigns:
        // - DirA: TopicA partitions [0, 1], TopicB partition [0]
        // - DirB: TopicA partition [2]
        var original = new AssignReplicasToDirsRequest
        {
            ApiKey = ApiKey.AssignReplicasToDirs,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "broker-3",
            BrokerId = 3,
            BrokerEpoch = 17,
            Directories =
            [
                new AssignReplicasToDirsRequest.DirectoryData
                {
                    Id = DirIdA,
                    Topics =
                    [
                        new AssignReplicasToDirsRequest.TopicData
                        {
                            TopicId = TopicIdA,
                            Partitions =
                            [
                                new AssignReplicasToDirsRequest.PartitionData { PartitionIndex = 0 },
                                new AssignReplicasToDirsRequest.PartitionData { PartitionIndex = 1 },
                            ],
                        },
                        new AssignReplicasToDirsRequest.TopicData
                        {
                            TopicId = TopicIdB,
                            Partitions =
                            [
                                new AssignReplicasToDirsRequest.PartitionData { PartitionIndex = 0 },
                            ],
                        },
                    ],
                },
                new AssignReplicasToDirsRequest.DirectoryData
                {
                    Id = DirIdB,
                    Topics =
                    [
                        new AssignReplicasToDirsRequest.TopicData
                        {
                            TopicId = TopicIdA,
                            Partitions =
                            [
                                new AssignReplicasToDirsRequest.PartitionData { PartitionIndex = 2 },
                            ],
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AssignReplicasToDirsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "broker-3");

        Assert.Equal(3, parsed.BrokerId);
        Assert.Equal(17L, parsed.BrokerEpoch);
        Assert.Equal(2, parsed.Directories.Count);

        Assert.Equal(DirIdA, parsed.Directories[0].Id);
        Assert.Equal(2, parsed.Directories[0].Topics.Count);
        Assert.Equal(TopicIdA, parsed.Directories[0].Topics[0].TopicId);
        Assert.Equal(new[] { 0, 1 }, parsed.Directories[0].Topics[0].Partitions.ConvertAll(p => p.PartitionIndex));
        Assert.Equal(TopicIdB, parsed.Directories[0].Topics[1].TopicId);

        Assert.Equal(DirIdB, parsed.Directories[1].Id);
        Assert.Single(parsed.Directories[1].Topics);
        Assert.Equal(new[] { 2 }, parsed.Directories[1].Topics[0].Partitions.ConvertAll(p => p.PartitionIndex));
    }

    [Fact]
    public void AssignReplicasToDirsRequest_EmptyDirectories_RoundTrips()
    {
        var original = new AssignReplicasToDirsRequest
        {
            ApiKey = ApiKey.AssignReplicasToDirs,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "broker-3",
            BrokerId = 3,
            BrokerEpoch = 17,
            Directories = [],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AssignReplicasToDirsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "broker-3");

        Assert.Empty(parsed.Directories);
    }

    [Fact]
    public void AssignReplicasToDirsResponse_PerPartitionError_RoundTrips()
    {
        // KAFKA_STORAGE_ERROR (56) on one partition while another succeeds.
        var original = new AssignReplicasToDirsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            Directories =
            [
                new AssignReplicasToDirsResponse.DirectoryData
                {
                    Id = DirIdA,
                    Topics =
                    [
                        new AssignReplicasToDirsResponse.TopicData
                        {
                            TopicId = TopicIdA,
                            Partitions =
                            [
                                new AssignReplicasToDirsResponse.PartitionData
                                {
                                    PartitionIndex = 0,
                                    ErrorCode = ErrorCode.None,
                                },
                                new AssignReplicasToDirsResponse.PartitionData
                                {
                                    PartitionIndex = 1,
                                    ErrorCode = (ErrorCode)56, // KAFKA_STORAGE_ERROR
                                },
                            ],
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AssignReplicasToDirsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        var partitions = parsed.Directories[0].Topics[0].Partitions;
        Assert.Equal(ErrorCode.None, partitions[0].ErrorCode);
        Assert.Equal((ErrorCode)56, partitions[1].ErrorCode);
    }

    [Fact]
    public void AssignReplicasToDirsResponse_TopLevelError_RoundTrips()
    {
        var original = new AssignReplicasToDirsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.NotController,
            Directories = [],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AssignReplicasToDirsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.NotController, parsed.ErrorCode);
        Assert.Empty(parsed.Directories);
    }
}
