using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Kuestenlogik.Surgewave.Runtime;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests.Fixtures;

/// <summary>
/// A three-broker cluster with a declared quorum and a Java Kafka client — Apache Kafka's own tools in an apache/kafka
/// container — that reaches every broker (#220): each broker binds the host's outbound address. The container only
/// sleeps; each tool runs in it through <see cref="RunAsync"/>. Without Docker the fixture records why, and every test
/// skips. A fresh cluster per test class, since a test may stop a broker.
/// </summary>
public sealed class JavaClusterFixture : IAsyncLifetime
{
    /// <summary>The Java client under test: the newest Kafka the broker targets.</summary>
    private const string ClientImage = "apache/kafka:4.1.0";

    private IContainer? _client;

    /// <summary>The cluster; null when the Java client is not available.</summary>
    public CoordinatorCluster? Cluster { get; private set; }

    /// <summary>Why the Java client could not be started, or null when it runs.</summary>
    public string? Unavailable { get; private set; }

    public async ValueTask InitializeAsync()
    {
        var host = ContainerReachableHost.OutboundIPv4Address();
        if (host is null)
        {
            Unavailable = "the host has no IPv4 address a container can reach";
            return;
        }

        try
        {
            _client = new ContainerBuilder(ClientImage)
                .WithEntrypoint("sleep")
                .WithCommand("infinity")
                .Build();
            await _client.StartAsync();
        }
        catch (Exception ex) when (ex is DockerUnavailableException or TimeoutException or HttpRequestException)
        {
            Unavailable = $"Docker is not available ({ex.GetType().Name}: {ex.Message})";
            return;
        }

        Cluster = await CoordinatorCluster.StartAsync(new ConsoleOutput(), host.ToString());
    }

    /// <summary>Skips the calling test when the Java client is not available.</summary>
    public CoordinatorCluster ClusterOrSkip()
    {
        if (Unavailable is not null || Cluster is null)
            Assert.Skip($"Java client E2E skipped: {Unavailable}");
        return Cluster;
    }

    /// <summary>
    /// Runs a shell command in the client container, with the Kafka tools on the path and <c>$BOOTSTRAP</c> set to
    /// <paramref name="bootstrap"/>.
    /// </summary>
    public async Task<ExecResult> RunAsync(string command, SurgewaveRuntime bootstrap, CancellationToken cancellationToken)
    {
        var client = _client ?? throw new InvalidOperationException("The Java client is not running.");
        return await client.ExecAsync(
            ["sh", "-c", $"export PATH=/opt/kafka/bin:$PATH BOOTSTRAP={bootstrap.BootstrapServers}; {command}"],
            cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (_client is not null)
            await _client.DisposeAsync();
        if (Cluster is not null)
            await Cluster.DisposeAsync();
    }

    // The cluster outlives any one test, so its log goes to the console rather than to a test's output.
    private sealed class ConsoleOutput : ITestOutputHelper
    {
        public string Output => string.Empty;

        public void Write(string message) => Console.Write(message);

        public void Write(string format, params object[] args) => Console.Write(format, args);

        public void WriteLine(string message) => Console.WriteLine(message);

        public void WriteLine(string format, params object[] args) => Console.WriteLine(format, args);
    }
}
