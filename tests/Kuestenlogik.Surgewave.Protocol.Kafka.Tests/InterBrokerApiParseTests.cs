using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Gate 1 for #69: the broker parser must decode the inter-broker ApiKeys a peer
/// actually sends. The controller-push keys LeaderAndIsr(4)/StopReplica(5)/
/// UpdateMetadata(6) are gone with the push path (#163 step 3) — the controller
/// replicates through the Raft log now — so what remains is the reverse report a
/// leader sends its controller. This serializes the exact request shape
/// ControllerClient sends and asserts the parser returns the typed request
/// rather than throwing.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class InterBrokerApiParseTests
{
    private static readonly KafkaProtocolHandler Handler = new();

    [Fact]
    public void ParseRequest_AlterPartition_V3_ReturnsTypedRequest()
    {
        // Reverse ISR propagation (#69 Phase 2): a leader sends AlterPartition v3
        // to the controller. AlterPartition is flexible at v0+, so the header has
        // trailing tagged fields AND ClientId must be a regular (non-compact)
        // string — the two header fixes this test locks in. The 9 existing
        // round-trip tests bypass ReadRequestHeader and cannot catch them.
        var request = new AlterPartitionRequest
        {
            ApiKey = ApiKey.AlterPartition,
            ApiVersion = 3, // v3: NewIsrWithEpochs + TopicId — exactly what ControllerClient sends
            CorrelationId = 11,
            ClientId = "surgewave-leader-2",
            BrokerId = 2,
            BrokerEpoch = -1,
            Topics =
            [
                new AlterPartitionRequest.TopicData
                {
                    TopicId = Guid.NewGuid(),
                    Partitions =
                    [
                        new AlterPartitionRequest.PartitionData
                        {
                            PartitionIndex = 1,
                            LeaderEpoch = 4,
                            PartitionEpoch = 4,
                            LeaderRecoveryState = 0,
                            NewIsrWithEpochs =
                            [
                                new AlterPartitionRequest.BrokerState { BrokerId = 2, BrokerEpoch = -1 },
                                new AlterPartitionRequest.BrokerState { BrokerId = 3, BrokerEpoch = -1 },
                                new AlterPartitionRequest.BrokerState { BrokerId = 1, BrokerEpoch = -1 },
                            ],
                        },
                    ],
                },
            ],
        };

        var parsed = Handler.ParseRequest(request.Serialize());

        var alter = Assert.IsType<AlterPartitionRequest>(parsed);
        Assert.Equal(2, alter.BrokerId);
        var partition = Assert.Single(Assert.Single(alter.Topics).Partitions);
        Assert.Equal(1, partition.PartitionIndex);
        Assert.Equal(4, partition.LeaderEpoch);
        Assert.NotNull(partition.NewIsrWithEpochs);
        Assert.Equal(new[] { 2, 3, 1 }, partition.NewIsrWithEpochs!.Select(b => b.BrokerId).ToArray());
    }
}
