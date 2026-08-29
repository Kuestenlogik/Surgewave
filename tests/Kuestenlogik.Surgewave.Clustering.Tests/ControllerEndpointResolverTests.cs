using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// How a broker finds the controller when it is not in the quorum (#169).
/// </summary>
/// <remarks>
/// A voter can work this out from having been part of the quorum. An observer never was, so it
/// has nothing but the configuration to start from — and once it is replicating, the log's own
/// view of the leader has to take over, or a stale configured list would be fatal instead of
/// merely out of date.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class ControllerEndpointResolverTests
{
    private const int LocalBroker = 7;
    private const string Quorum = "1@controller-1:9093,2@controller-2:9093,3@controller-3:9093";

    [Fact]
    public void WithoutAQuorumTheLowestIdPeerIsTheController()
    {
        // Combined mode, unchanged: the convention every existing deployment runs on.
        var resolver = NewResolver(new ClusteringConfig { BrokerId = LocalBroker }, brokers: [2, 5, 9]);

        Assert.Equal(("host-2", 10_002), resolver.Resolve());
    }

    [Fact]
    public void TheSeedResolvesNobody()
    {
        // This broker IS the controller. Falling back to a peer here would make the controller
        // register against a follower and loop on NotController.
        var state = NewClusterState([1, 2]);
        state.ControllerId = LocalBroker;
        var resolver = new ControllerEndpointResolver(new ClusteringConfig { BrokerId = LocalBroker }, state);

        Assert.Null(resolver.Resolve());
    }

    [Fact]
    public void FirstContactGoesToAConfiguredVoter()
    {
        // The observer case: nothing is known about the cluster yet, and the quorum list is the
        // only thing that says where to look. Without this the node would fall through to the
        // lowest-id convention and dial a broker that does not lead anything.
        var resolver = NewResolver(
            new ClusteringConfig { BrokerId = LocalBroker, ControllerQuorumVoters = Quorum },
            brokers: []);

        Assert.Equal(("controller-1", 9093), resolver.Resolve());
    }

    [Fact]
    public void ALowerIdBrokerDoesNotDisplaceTheQuorum()
    {
        // Broker 2 is the lowest id but not a voter. In combined mode it would be the answer;
        // with a quorum configured, dialling it means registering with a node that cannot
        // accept the registration.
        var resolver = NewResolver(
            new ClusteringConfig { BrokerId = LocalBroker, ControllerQuorumVoters = "5@controller-5:9093" },
            brokers: [2]);

        Assert.Equal(("controller-5", 9093), resolver.Resolve());
    }

    [Fact]
    public void AVoterThatDoesNotAnswerCostsOneAttempt()
    {
        // Rotating is what keeps one dead controller from blocking every attempt: the node
        // moves to the next voter rather than waiting on the same machine forever.
        var resolver = NewResolver(
            new ClusteringConfig { BrokerId = LocalBroker, ControllerQuorumVoters = Quorum },
            brokers: []);

        Assert.Equal(("controller-1", 9093), resolver.Resolve());
        resolver.ReportFailure();
        Assert.Equal(("controller-2", 9093), resolver.Resolve());
        resolver.ReportFailure();
        Assert.Equal(("controller-3", 9093), resolver.Resolve());
    }

    [Fact]
    public void TheControllerClusterStateNamesWinsOverTheConfiguredList()
    {
        // Once the node is replicating, cluster state tracks the Raft leader — so a leadership
        // change is followed from the log rather than by starting over from configuration.
        var state = NewClusterState([1, 2, 3]);
        state.ControllerId = 3;
        var resolver = new ControllerEndpointResolver(
            new ClusteringConfig { BrokerId = LocalBroker, ControllerQuorumVoters = Quorum }, state);

        Assert.Equal(("host-3", 10_003), resolver.Resolve());
    }

    [Fact]
    public void ANamedControllerThatStopsAnsweringIsAbandonedForTheQuorum()
    {
        // The failure this guards: a controller that is named but down would otherwise be
        // returned forever, and the node would wait on one machine while a quorum it could
        // reach sits there.
        var state = NewClusterState([1, 2, 3]);
        state.ControllerId = 3;
        var resolver = new ControllerEndpointResolver(
            new ClusteringConfig { BrokerId = LocalBroker, ControllerQuorumVoters = Quorum }, state);

        Assert.Equal(("host-3", 10_003), resolver.Resolve());
        resolver.ReportFailure();
        resolver.ReportFailure();

        Assert.NotEqual(("host-3", 10_003), resolver.Resolve());
    }

    [Fact]
    public void AnAnswerRestoresTrustInTheNamedController()
    {
        var state = NewClusterState([1, 2, 3]);
        state.ControllerId = 3;
        var resolver = new ControllerEndpointResolver(
            new ClusteringConfig { BrokerId = LocalBroker, ControllerQuorumVoters = Quorum }, state);

        resolver.ReportFailure();
        resolver.ReportFailure();
        resolver.ReportSuccess();

        Assert.Equal(("host-3", 10_003), resolver.Resolve());
    }

    [Fact]
    public void WhatTheClusterReportsBeatsWhatWasConfigured()
    {
        // The configured list only has to be good enough for first contact. After that the
        // cluster's own view is newer than what an operator typed, which is what makes a list
        // that has gone stale survivable.
        var state = NewClusterState([1]);
        var resolver = new ControllerEndpointResolver(
            new ClusteringConfig { BrokerId = LocalBroker, ControllerQuorumVoters = "1@stale-host:9999" }, state);

        Assert.Equal(("host-1", 10_001), resolver.Resolve());
    }

    [Fact]
    public void ThisBrokerIsNeverItsOwnCandidate()
    {
        // A quorum list names every voter including this one when it is a controller; dialling
        // self would be a registration against itself.
        var resolver = NewResolver(
            new ClusteringConfig { BrokerId = 1, ControllerQuorumVoters = "1@controller-1:9093,2@controller-2:9093" },
            brokers: []);

        Assert.Equal(("controller-2", 9093), resolver.Resolve());
    }

    [Fact]
    public void WalkingTheWholeQuorumWithoutAnAnswerIsReportable()
    {
        // The bootstrap corner: on a first start there is no log to learn from, so a mistyped
        // voter list is indistinguishable from an unreachable network unless something says so.
        var resolver = NewResolver(
            new ClusteringConfig { BrokerId = LocalBroker, ControllerQuorumVoters = Quorum },
            brokers: []);

        Assert.False(resolver.ExhaustedQuorumWithoutContact);

        resolver.ReportFailure();
        resolver.ReportFailure();
        Assert.False(resolver.ExhaustedQuorumWithoutContact);

        resolver.ReportFailure();
        Assert.True(resolver.ExhaustedQuorumWithoutContact);
        Assert.Contains("controller-2:9093", resolver.DescribeConfiguredQuorum());
    }

    [Fact]
    public void AQuorumThatAnsweredOnceIsNeverReportedAsUnreachable()
    {
        // Losing contact later is an outage, not a configuration mistake — and saying "your
        // endpoints are wrong" about endpoints that demonstrably worked sends the operator
        // looking in the wrong place.
        var resolver = NewResolver(
            new ClusteringConfig { BrokerId = LocalBroker, ControllerQuorumVoters = Quorum },
            brokers: []);

        resolver.ReportSuccess();
        resolver.ReportFailure();
        resolver.ReportFailure();
        resolver.ReportFailure();
        resolver.ReportFailure();

        Assert.False(resolver.ExhaustedQuorumWithoutContact);
    }

    [Fact]
    public void CombinedModeIsNeverReportedAsAnUnreachableQuorum()
    {
        var resolver = NewResolver(new ClusteringConfig { BrokerId = LocalBroker }, brokers: [1]);

        resolver.ReportFailure();
        resolver.ReportFailure();
        resolver.ReportFailure();

        Assert.False(resolver.HasConfiguredQuorum);
        Assert.False(resolver.ExhaustedQuorumWithoutContact);
    }

    private static ControllerEndpointResolver NewResolver(ClusteringConfig config, int[] brokers)
        => new(config, NewClusterState(brokers));

    private static ClusterState NewClusterState(int[] brokers)
    {
        var state = new ClusterState();
        foreach (var brokerId in brokers)
        {
            state.AddBroker(new BrokerNode
            {
                BrokerId = brokerId,
                Host = $"host-{brokerId}",
                Port = 9092 + brokerId,
                ReplicationPort = 10_000 + brokerId,
            });
        }

        return state;
    }
}
