using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc2 — round-trip coverage for the native SRWV inter-broker payloads. These serialize the
/// neutral Clustering domain records with the shared SurgewavePayloadReader/Writer; the property
/// under test is self-consistency (Write then Read reproduces the input), since native frames only
/// travel between native-capable peers and never need to match the Kafka wire byte layout.
/// </summary>
public class InterBrokerPayloadRoundTripTests
{
    private static T RoundTrip<T>(T payload) where T : ISerializablePayload<T>
    {
        var buffer = new byte[payload.EstimateSize()];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);
        var reader = new SurgewavePayloadReader(buffer.AsSpan(0, writer.Position));
        return T.Read(ref reader);
    }

    [Fact]
    public void BrokerHeartbeatRequest_RoundTrips()
    {
        var input = new BrokerHeartbeatInput(BrokerId: 7, BrokerEpoch: 42, CurrentMetadataOffset: 12345, WantFence: true, WantShutDown: false, OfflineLogDirs: []);
        var result = RoundTrip(new BrokerHeartbeatRequestPayload(input));
        AssertSameHeartbeat(input, result.Input);
    }

    // The native heartbeat carries the offline log directories as the Kafka wire's KIP-858 field does (#162), so a
    // broker on either wire reports a failed directory the same.
    [Fact]
    public void BrokerHeartbeatRequest_RoundTrips_OfflineLogDirs()
    {
        Guid[] offline = [Guid.NewGuid(), Guid.NewGuid()];
        var input = new BrokerHeartbeatInput(BrokerId: 3, BrokerEpoch: 9, CurrentMetadataOffset: 9, WantFence: false, WantShutDown: false, OfflineLogDirs: offline);
        var payload = new BrokerHeartbeatRequestPayload(input);

        var result = RoundTrip(payload);

        AssertSameHeartbeat(input, result.Input);
    }

    private static void AssertSameHeartbeat(BrokerHeartbeatInput expected, BrokerHeartbeatInput actual)
    {
        Assert.Equal(expected with { OfflineLogDirs = [] }, actual with { OfflineLogDirs = [] });
        Assert.Equal(expected.OfflineLogDirs, actual.OfflineLogDirs);
    }

    [Fact]
    public void BrokerHeartbeatResponse_RoundTrips()
    {
        var outcome = new BrokerHeartbeatOutcome(ClusterRpcStatus.StaleBrokerEpoch, IsFenced: true, IsCaughtUp: false, ShouldShutDown: true);
        var result = RoundTrip(new BrokerHeartbeatResponsePayload(outcome));
        Assert.Equal(outcome, result.Outcome);
    }

    [Fact]
    public void BrokerRegistrationRequest_RoundTrips_WithRack()
    {
        var input = new BrokerRegistrationInput(
            BrokerId: 3,
            ClusterId: "surgewave-prod",
            IncarnationId: Guid.NewGuid(),
            Listeners: [new ListenerSpec("PLAINTEXT", "10.0.0.1", 9092, 0), new ListenerSpec("SSL", "10.0.0.1", 9093, 1)],
            Features: [new FeatureSpec("metadata.version", 1, 20), new FeatureSpec("inter.broker.protocol", 0, 1)],
            Rack: "rack-a",
            PreviousBrokerEpoch: -1);

        var result = RoundTrip(new BrokerRegistrationRequestPayload(input)).Input;

        Assert.Equal(input.BrokerId, result.BrokerId);
        Assert.Equal(input.ClusterId, result.ClusterId);
        Assert.Equal(input.IncarnationId, result.IncarnationId);
        Assert.Equal(input.Listeners, result.Listeners);
        Assert.Equal(input.Features, result.Features);
        Assert.Equal(input.Rack, result.Rack);
        Assert.Equal(input.PreviousBrokerEpoch, result.PreviousBrokerEpoch);
    }

    [Fact]
    public void BrokerRegistrationRequest_RoundTrips_NullRack_EmptyLists()
    {
        var input = new BrokerRegistrationInput(
            BrokerId: 0,
            ClusterId: "c",
            IncarnationId: Guid.Empty,
            Listeners: [],
            Features: [],
            Rack: null,
            PreviousBrokerEpoch: 99);

        var result = RoundTrip(new BrokerRegistrationRequestPayload(input)).Input;

        Assert.Equal(input.IncarnationId, result.IncarnationId);
        Assert.Empty(result.Listeners);
        Assert.Empty(result.Features);
        Assert.Null(result.Rack);
        Assert.Equal(input.PreviousBrokerEpoch, result.PreviousBrokerEpoch);
    }

    [Fact]
    public void BrokerRegistrationResponse_RoundTrips()
    {
        var outcome = new BrokerRegistrationOutcome(ClusterRpcStatus.None, BrokerEpoch: 1);
        var result = RoundTrip(new BrokerRegistrationResponsePayload(outcome));
        Assert.Equal(outcome, result.Outcome);
    }

    [Fact]
    public void AlterPartition_RoundTrips()
    {
        var payload = new AlterPartitionPayload(
            LeaderId: 3, LeaderEpoch: 8, new TopicPartition { Topic = "orders", Partition = 4 }, NewIsr: [3, 1, 2]);

        var result = RoundTrip(payload);

        Assert.Equal(3, result.LeaderId);
        Assert.Equal(8, result.LeaderEpoch);
        Assert.Equal(payload.Tp, result.Tp);
        Assert.Equal([3, 1, 2], result.NewIsr);
    }

    [Fact]
    public void WriteTxnMarkersRequest_RoundTrips()
    {
        var payload = new WriteTxnMarkersRequestPayload(
            TransactionalId: "txn-1",
            ProducerId: 555,
            ProducerEpoch: 3,
            Partitions: [new TopicPartition { Topic = "t", Partition = 0 }, new TopicPartition { Topic = "t", Partition = 1 }],
            Commit: true,
            CoordinatorEpoch: 12);

        var result = RoundTrip(payload);

        Assert.Equal(payload.TransactionalId, result.TransactionalId);
        Assert.Equal(payload.ProducerId, result.ProducerId);
        Assert.Equal(payload.ProducerEpoch, result.ProducerEpoch);
        Assert.Equal(payload.Partitions, result.Partitions);
        Assert.Equal(payload.Commit, result.Commit);
        Assert.Equal(payload.CoordinatorEpoch, result.CoordinatorEpoch);
    }

    [Fact]
    public void WriteTxnMarkersResponse_RoundTrips()
    {
        var result = RoundTrip(new WriteTxnMarkersResponsePayload(ClusterRpcStatus.NotLeaderForPartition));
        Assert.Equal(ClusterRpcStatus.NotLeaderForPartition, result.Status);
    }
}
