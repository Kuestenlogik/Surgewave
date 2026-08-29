using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Node roles and the explicit controller quorum (#168).
/// </summary>
/// <remarks>
/// A quorum list is typed into a deployment file by hand and usually read again only after
/// the cluster has failed to form. Most of what is pinned here is therefore the error text:
/// the point of validating at all is that the message names the mistake.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class ControllerQuorumTests
{
    [Theory]
    [InlineData("1@localhost:9093", 1, "localhost", 9093)]
    [InlineData(" 2@broker-2.svc:9094 ", 2, "broker-2.svc", 9094)]
    [InlineData("3@[::1]:9095", 3, "[::1]", 9095)]
    public void AVoterIsIdAtHostColonPort(string entry, int nodeId, string host, int port)
    {
        // The IPv6 case is why the port is split on the LAST colon: "[::1]:9095" has three.
        Assert.True(ControllerQuorumVoter.TryParse(entry, out var voter, out _));

        Assert.Equal(nodeId, voter.NodeId);
        Assert.Equal(host, voter.Host);
        Assert.Equal(port, voter.Port);
    }

    [Theory]
    [InlineData("localhost:9093")]      // no id
    [InlineData("1@localhost")]         // no port
    [InlineData("one@localhost:9093")]  // id is not a number
    [InlineData("1@localhost:0")]       // port out of range
    [InlineData("1@localhost:70000")]   // port out of range
    [InlineData("@localhost:9093")]     // empty id
    public void AMalformedVoterIsNamedInTheError(string entry)
    {
        Assert.False(ControllerQuorumVoter.TryParse(entry, out _, out var error));

        Assert.NotNull(error);
        Assert.Contains(entry.Trim(), error);
    }

    [Fact]
    public void RolesDefaultToCombinedMode()
    {
        // What every node did before this setting existed, and the shape a single broker and
        // an embedded host stay in.
        Assert.True(NodeRolesParser.TryParse(new ClusteringConfig().ProcessRoles, out var roles, out _));

        Assert.True(roles.HasFlag(NodeRoles.Broker));
        Assert.True(roles.HasFlag(NodeRoles.Controller));
    }

    [Fact]
    public void AMisspelledRoleIsAnErrorRatherThanIgnored()
    {
        // Dropping "contoller" silently would leave the node a plain broker, and the cluster
        // would then fail to form a quorum for a reason nothing connects back to the typo.
        Assert.False(NodeRolesParser.TryParse("broker,contoller", out _, out var error));

        Assert.NotNull(error);
        Assert.Contains("contoller", error);
    }

    [Fact]
    public void AnEmptyRoleListIsAnError()
    {
        Assert.False(NodeRolesParser.TryParse("", out _, out var error));

        Assert.NotNull(error);
    }

    [Fact]
    public void TheDefaultConfigurationValidates()
    {
        // The load-bearing case: nothing configured, nothing complained about. Every existing
        // deployment is this one.
        Assert.Empty(Validate(new ClusteringConfig { BrokerId = 1 }));
    }

    [Fact]
    public void AControllerMustBeInItsOwnQuorum()
    {
        var errors = Validate(new ClusteringConfig
        {
            BrokerId = 3,
            ProcessRoles = "broker,controller",
            ControllerQuorumVoters = "1@localhost:9093,2@localhost:9193",
            ClusterNodes = "1:localhost:9092,2:localhost:9192,3:localhost:9292",
        });

        Assert.Contains(errors, e => e.Contains("not in") && e.Contains(nameof(ClusteringConfig.ControllerQuorumVoters)));
    }

    [Fact]
    public void ANodeInTheQuorumMustCarryTheControllerRole()
    {
        // The other direction, and the worse one: the rest of the quorum counts this node
        // towards the majority and waits for a vote it never casts.
        var errors = Validate(new ClusteringConfig
        {
            BrokerId = 1,
            ProcessRoles = "broker",
            ControllerQuorumVoters = "1@localhost:9093,2@localhost:9193,3@localhost:9293",
            ClusterNodes = "1:localhost:9092,2:localhost:9192,3:localhost:9292",
        });

        Assert.Contains(errors, e => e.Contains(nameof(ClusteringConfig.ProcessRoles)) && e.Contains("excludes"));
    }

    [Fact]
    public void DroppingTheControllerRoleNeedsAQuorumToDropItTo()
    {
        // Without a voter list every node votes. A node that then refuses the role leaves
        // nobody named to keep it.
        var errors = Validate(new ClusteringConfig { BrokerId = 1, ProcessRoles = "broker" });

        Assert.Contains(errors, e => e.Contains(nameof(ClusteringConfig.ControllerQuorumVoters)));
    }

    [Fact]
    public void AVoterNoBrokerKnowsAboutIsAnError()
    {
        // Node 9 is counted towards the majority and can never answer, so a three-voter
        // quorum needs both real nodes from the start — a typo that costs the cluster its
        // fault tolerance without saying so.
        var errors = Validate(new ClusteringConfig
        {
            BrokerId = 1,
            ControllerQuorumVoters = "1@localhost:9093,2@localhost:9193,9@localhost:9993",
            ClusterNodes = "1:localhost:9092,2:localhost:9192",
        });

        Assert.Contains(errors, e => e.Contains("node 9") && e.Contains(nameof(ClusteringConfig.ClusterNodes)));
    }

    [Fact]
    public void ARepeatedVoterIsAnError()
    {
        var errors = Validate(new ClusteringConfig
        {
            BrokerId = 1,
            ControllerQuorumVoters = "1@localhost:9093,2@localhost:9193,2@localhost:9194",
            ClusterNodes = "1:localhost:9092,2:localhost:9192",
        });

        Assert.Contains(errors, e => e.Contains("more than once"));
    }

    [Fact]
    public void AQuorumThatNamesOnlyKnownBrokersValidates()
    {
        Assert.Empty(Validate(new ClusteringConfig
        {
            BrokerId = 1,
            ProcessRoles = "broker,controller",
            ControllerQuorumVoters = "1@localhost:9093,2@localhost:9193,3@localhost:9293",
            ClusterNodes = "1:localhost:9092,2:localhost:9192,3:localhost:9292",
        }));
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(2, true)]
    [InlineData(3, false)]
    [InlineData(4, true)]
    [InlineData(5, false)]
    public void AnEvenVoterCountIsWorthWarningAbout(int voterCount, bool expected)
    {
        // Four voters need three to agree, exactly as five do — the fourth adds a node that
        // can fail without adding a failure the quorum survives. A warning, not an error:
        // during a rolling change from three to five it is a legitimate intermediate state.
        var voters = Enumerable.Range(1, voterCount)
            .Select(id => new ControllerQuorumVoter(id, "localhost", 9093 + id))
            .ToArray();

        Assert.Equal(expected, ControllerQuorum.IsEvenVoterCount(voters));
    }

    [Fact]
    public void NoVotersIsNotAnEvenCount()
    {
        // Zero is even, but "nothing configured" is combined mode and must not warn.
        Assert.False(ControllerQuorum.IsEvenVoterCount([]));
    }

    [Theory]
    [InlineData(new[] { 1 }, 1)]
    [InlineData(new[] { 1, 2 }, 2)]
    [InlineData(new[] { 1, 2, 3 }, 2)]
    [InlineData(new[] { 1, 2, 3, 4, 5 }, 3)]
    public void TheConfiguredSetCountsAStrictMajority(int[] voterIds, int expected)
    {
        Assert.Equal(expected, new ConfiguredVoterSet(voterIds).Majority);
    }

    [Fact]
    public void TheConfiguredSetDoesNotCountAVoterTwice()
    {
        // Belt and braces against the duplicate the validator rejects: counting it twice
        // would inflate the majority and could leave a healthy quorum unable to elect.
        var set = new ConfiguredVoterSet([1, 2, 2, 3]);

        Assert.Equal(3, set.VoterIds.Count);
        Assert.Equal(2, set.Majority);
    }

    private static IReadOnlyList<string> Validate(ClusteringConfig config)
    {
        var errors = new List<string>();
        ControllerQuorum.Validate(config, errors);
        return errors;
    }
}
