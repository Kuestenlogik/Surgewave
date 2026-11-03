using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// Full-host boot smoke test: launches the REAL built broker executable
/// (<c>surgewave-broker.dll</c>) as a subprocess with Connect enabled and asserts it boots to
/// "ready" without crashing, then shuts it down.
///
/// <para>
/// Why a subprocess and not the embedded runtime: every other broker test boots the lightweight
/// in-process <c>SurgewaveRuntime</c>, which never runs the real <c>WebApplication</c> host, the
/// <c>BrokerPluginActivator</c>, or the WebApplication-based broker plugins (e.g.
/// <c>SurgewaveConnectBrokerPlugin</c>). That gap once let a startup crash pass CI green while the
/// actual <c>dotnet run</c> broker crashed: #59 moved the Kafka wire client into the optional
/// <c>Client.Kafka</c> assembly, and the Connect broker plugin built a Kafka-protocol client at
/// startup that then required that (absent) assembly. Only a real process boot exercises the full
/// plugin-activation closure, so only a subprocess test guards against that class of regression.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
[Collection("BrokerBoot")]
public sealed class BrokerBootSmokeTests
{
    // HeartbeatManager.StartAsync's own line. Present only if the shipped host constructs and starts
    // the failure detector — which until #121 it did not.
    private const string HeartbeatManagerStartedLine = "Heartbeat manager started";

    // BrokerPluginActivator's line once the Kafka protocol plugin from plugins/ is active.
    private const string KafkaPluginActivatedLine = "Activated protocol plugin: Kafka Protocol";

    [Fact(Timeout = 180_000)]
    public async Task Broker_BootsToReady_WithConnectEnabled()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var dataDir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);

        try
        {
            await using var broker = await BrokerProcess.StartAsync(dataDir, cancellationToken,
                "--Surgewave:Connect:Enabled=true", // the regression's trigger — force it on regardless of appsettings
                "--Surgewave:AutoCreateTopics=true");

            // #121 — the same class of gap this test exists for, one layer down: the failure
            // detector was wired in the embedded runtime only, so the shipped process could never
            // shrink an ISR on failure, elect away from a dead broker, or replace a dead controller
            // (HandleBrokerFailedAsync is reachable only from HeartbeatManager.OnBrokerFailed).
            // What the behaviour does once wired is covered by ControllerFailoverTests against the
            // embedded runtime; only a real process boot can show it is wired HERE.
            Assert.Contains(HeartbeatManagerStartedLine, broker.Log, StringComparison.Ordinal);

            // The build staged the Kafka plugin next to the broker (CopyKafkaPluginForDevRun), and the
            // broker found and activated it from there — what a `dotnet run` relies on. The test used
            // to stage the plugin itself, which hid that the build put it where the broker never looks.
            Assert.Contains(KafkaPluginActivatedLine, broker.Log, StringComparison.Ordinal);
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }
}

[CollectionDefinition("BrokerBoot", DisableParallelization = true)]
public sealed class BrokerBootCollection;
