using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Lightweight wire-shape contracts for the three controller-driven admin
/// RPCs in <see cref="ClusterAdminHandler"/>. Building a full
/// <c>ClusterController</c> needs a working <c>ReplicaManager</c> and
/// peer transport — out of scope for unit tests; the handler's full
/// happy-path is exercised by the existing <c>RaftIntegrationTests</c>
/// once a real cluster is in scope. These tests focus on the parts that
/// don't need a working controller: <c>ListPartitionReassignments</c>
/// returns an empty response when the manager has nothing in flight, the
/// <c>AlterPartitionReassignmentsRequest</c> with a missing manager
/// answers with NotController rather than throwing, and the handler
/// advertises every controller-admin ApiKey.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ClusterAdminHandlerTests
{
    [Fact]
    public void Handler_AdvertisesAllThreeControllerApiKeys()
    {
        var handler = BuildHandlerWithoutController();
        var keys = handler.SupportedApiKeys.ToHashSet();
        Assert.Contains(ApiKey.ElectLeaders, keys);
        Assert.Contains(ApiKey.AlterPartitionReassignments, keys);
        Assert.Contains(ApiKey.ListPartitionReassignments, keys);
    }

    [Fact]
    public async Task ListPartitionReassignments_NoManager_ReturnsEmptyTopics()
    {
        var handler = BuildHandlerWithoutController();

        var resp = (ListPartitionReassignmentsResponse)await handler.HandleAsync(
            new ListPartitionReassignmentsRequest
            {
                ApiKey = ApiKey.ListPartitionReassignments,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
                TimeoutMs = 1000,
                Topics = null,
            },
            BuildContext(),
            CancellationToken.None);

        Assert.Equal(ErrorCode.None, resp.ErrorCode);
        Assert.Empty(resp.Topics);
    }

    [Fact]
    public async Task AlterPartitionReassignments_NoManager_ReturnsNotControllerAtTopLevel()
    {
        var handler = BuildHandlerWithoutController();

        var resp = (AlterPartitionReassignmentsResponse)await handler.HandleAsync(
            new AlterPartitionReassignmentsRequest
            {
                ApiKey = ApiKey.AlterPartitionReassignments,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "admin",
                TimeoutMs = 1000,
                Topics =
                [
                    new AlterPartitionReassignmentsRequest.ReassignableTopic
                    {
                        Name = "orders",
                        Partitions =
                        [
                            new AlterPartitionReassignmentsRequest.ReassignablePartition
                            {
                                PartitionIndex = 0,
                                Replicas = [1, 2, 3],
                            },
                        ],
                    },
                ],
            },
            BuildContext(),
            CancellationToken.None);

        Assert.Equal(ErrorCode.NotController, resp.ErrorCode);
        Assert.NotNull(resp.ErrorMessage);
        Assert.Empty(resp.Responses); // no per-partition rows when top-level rejected
    }

    /// <summary>
    /// Build a handler with both controller and reassignment manager null
    /// so the tests can run without spinning up the full broker. Since the
    /// handler is reflection-friendly via the constructor accepting null
    /// dependencies, this gives us the rejection-path coverage cheaply.
    /// </summary>
    private static ClusterAdminHandler BuildHandlerWithoutController()
    {
        // We never actually call ClusterController — every test path either
        // checks the api-key set (no call), or the no-manager path
        // (controller never reached). The constructor signature requires
        // a non-null controller, so we use reflection to construct an
        // uninitialised instance — every method we exercise short-circuits
        // before touching it.
        var controller = (Kuestenlogik.Surgewave.Clustering.Replication.ClusterController)System.Runtime.CompilerServices
            .RuntimeHelpers.GetUninitializedObject(typeof(Kuestenlogik.Surgewave.Clustering.Replication.ClusterController));

        var clusterState = new ClusterState();
        return new ClusterAdminHandler(
            controller,
            reassignmentManager: null,
            clusterState,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ClusterAdminHandler>.Instance);
    }

    private static RequestContext BuildContext() => new()
    {
        ConnectionState = new ConnectionState("test-host"),
        ClientId = "admin",
    };
}
