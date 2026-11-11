using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The feature level table (#199): every finalized level lies within its supported range, Kafka's
/// hard dependencies between levels hold, and the broker registration carries the table's
/// metadata.version range.
/// </summary>
public class BrokerFeatureLevelsTests
{
    [Fact]
    public void MetadataVersion_IsSupported14To29_Finalized29()
    {
        Assert.Equal(new FeatureLevel("metadata.version", 14, 29, 29), BrokerFeatureLevels.MetadataVersion);
    }

    [Fact]
    public void EveryAnnouncedFeature_IsFinalizedWithinItsSupportedRange()
    {
        Assert.NotEmpty(BrokerFeatureLevels.Announced);
        foreach (var feature in BrokerFeatureLevels.Announced)
        {
            Assert.True(feature.MinSupported <= feature.MaxSupported, $"{feature.Name}: empty supported range");
            Assert.InRange(feature.Finalized, feature.MinSupported, feature.MaxSupported);
        }
    }

    [Fact]
    public void AnnouncedFeatureNames_AreUnique()
    {
        var names = BrokerFeatureLevels.Announced.Select(f => f.Name).ToList();
        Assert.Equal(names.Count, names.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void AnnouncedLevels_HoldKafkasDependencies()
    {
        Assert.Empty(DependencyViolations(BrokerFeatureLevels.Announced));
    }

    [Fact]
    public void EligibleLeaderReplicas1_NeedsMetadataVersion23()
    {
        // The rule fires: ELR 1 announced next to a metadata.version below 23 is a violation, at 23 it is not.
        var elr = new FeatureLevel("eligible.leader.replicas.version", 0, 1, 1);

        Assert.NotEmpty(DependencyViolations([elr, new FeatureLevel("metadata.version", 14, 22, 22)]));
        Assert.NotEmpty(DependencyViolations([elr]));
        Assert.Empty(DependencyViolations([elr, new FeatureLevel("metadata.version", 14, 23, 23)]));
        Assert.Empty(DependencyViolations([elr, BrokerFeatureLevels.MetadataVersion]));
    }

    [Fact]
    public async Task Registration_CarriesTheTablesMetadataVersion_NextToInterBrokerProtocol()
    {
        var rpc = new CapturingRpc();
        var config = new ClusteringConfig
        {
            BrokerId = 2,
            Host = "localhost",
            Port = 9094,
            ReplicationPort = 10094,
            ClusterNodes = "0:localhost:9092", // non-empty so the loop runs
            HeartbeatIntervalMs = 30,
            RebalanceCheckIntervalSeconds = 5,
        };
        await using var loop = new BrokerLifecycleLoop(rpc, config, NullLogger<BrokerLifecycleLoop>.Instance);

        await loop.StartAsync(TestContext.Current.CancellationToken);
        var input = await rpc.FirstRegistration.Task.WaitAsync(TestContext.Current.CancellationToken);

        var metadataVersion = Assert.Single(input.Features, f => f.Name == BrokerFeatureLevels.MetadataVersion.Name);
        Assert.Equal(BrokerFeatureLevels.MetadataVersion.ToFeatureSpec(), metadataVersion);
        Assert.Contains(InterBrokerProtocolFeature.LocalFeatureSpec, input.Features);
        // The controller still reads only inter.broker.protocol from the list.
        Assert.Equal(InterBrokerProtocolFeature.LocalMaxSupported, InterBrokerProtocolFeature.LevelFrom(input.Features));
    }

    /// <summary>
    /// Every <see cref="BrokerFeatureLevels.Dependencies"/> rule a set of announced levels breaks: a
    /// dependent level that is finalized (or supported) needs the required feature finalized (or
    /// supported) at the required level or higher.
    /// </summary>
    private static List<string> DependencyViolations(IReadOnlyList<FeatureLevel> announced)
    {
        var violations = new List<string>();
        foreach (var rule in BrokerFeatureLevels.Dependencies)
        {
            var dependent = announced.FirstOrDefault(f => f.Name == rule.Feature);
            if (dependent is null)
                continue;

            var required = announced.FirstOrDefault(f => f.Name == rule.RequiredFeature);
            if (dependent.Finalized >= rule.Level && (required is null || required.Finalized < rule.RequiredLevel))
                violations.Add($"{rule.Feature} finalized at {dependent.Finalized} needs {rule.RequiredFeature} finalized >= {rule.RequiredLevel}");
            if (dependent.MaxSupported >= rule.Level && (required is null || required.MaxSupported < rule.RequiredLevel))
                violations.Add($"{rule.Feature} supported up to {dependent.MaxSupported} needs {rule.RequiredFeature} supported >= {rule.RequiredLevel}");
        }

        return violations;
    }

    private sealed class CapturingRpc : IBrokerLifecycleRpc
    {
        public TaskCompletionSource<BrokerRegistrationInput> FirstRegistration { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<BrokerRegistrationOutcome> RegisterAsync(BrokerRegistrationInput input, CancellationToken ct = default)
        {
            FirstRegistration.TrySetResult(input);
            return Task.FromResult(new BrokerRegistrationOutcome(ClusterRpcStatus.None, 1));
        }

        public Task<BrokerHeartbeatOutcome> HeartbeatAsync(BrokerHeartbeatInput input, CancellationToken ct = default) =>
            Task.FromResult(new BrokerHeartbeatOutcome(ClusterRpcStatus.None, IsFenced: false, IsCaughtUp: true, ShouldShutDown: false));
    }
}
