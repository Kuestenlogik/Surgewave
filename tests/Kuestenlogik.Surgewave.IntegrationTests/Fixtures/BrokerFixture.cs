using Confluent.Kafka;
using Kuestenlogik.Surgewave.Runtime;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests.Fixtures;

/// <summary>
/// Shared test fixture that starts and stops the Surgewave broker.
/// Use with xUnit's IClassFixture to share a single broker instance across tests in a class.
/// Use with xUnit's CollectionFixture to share across multiple test classes.
///
/// This fixture uses the SurgewaveRuntime infrastructure for simplified broker management.
/// After SurgewaveRuntime reports TCP readiness, the fixture performs an additional Kafka
/// protocol readiness check (GetMetadata) with retries — ensuring the Kafka request
/// dispatcher is fully initialised before any test sends AdminClient or Producer calls.
/// </summary>
public sealed class BrokerFixture : IAsyncLifetime, IDisposable
{
    private SurgewaveRuntime? _surgewave;
    private readonly ILoggerFactory _loggerFactory;
    private bool _disposed;

    // Static instance for test compatibility (collection fixture is singleton per collection)
    private static BrokerFixture? _instance;

    /// <summary>
    /// The actual bootstrap servers string (dynamically assigned port).
    /// Static accessor for backward compatibility with existing tests.
    /// </summary>
    public static string BootstrapServers => _instance?._surgewave?.BootstrapServers ?? throw new InvalidOperationException("Broker not started");

    /// <summary>
    /// The actual port (dynamically assigned).
    /// </summary>
    public int Port => _surgewave?.Port ?? 0;

    public BrokerFixture()
    {
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddConsole();
        });
    }

    public async ValueTask InitializeAsync()
    {
        // Use dynamic port (0) to avoid port conflicts between test runs
        _surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithPartitions(3)
            .WithAutoCreateTopics(true)
            .WithShutdownTimeout(5)
            .WithLogging(_loggerFactory)
            .Build()
            .StartAsync();

        // Set static instance for backward compatibility
        _instance = this;

        // SurgewaveRuntime.WaitForBrokerReadyAsync only checks TCP connectivity.
        // Wait until the Kafka protocol layer is actually responding — the accept
        // loop, request dispatcher and ApiVersions handler need a few extra ms after
        // the TCP listener is up. Without this, early tests sporadically get
        // "Broker transport failure" from Confluent.Kafka because the initial
        // ApiVersions handshake times out on a socket that is listening but not yet
        // dispatching.
        await WaitForProtocolReadyAsync();
    }

    private async Task WaitForProtocolReadyAsync(int maxRetries = 30)
    {
        var bootstrapServers = BootstrapServers;
        for (var i = 0; i < maxRetries; i++)
        {
            try
            {
                var adminConfig = new AdminClientConfig
                {
                    BootstrapServers = bootstrapServers,
                    SocketTimeoutMs = 2000,
                };
                using var admin = new AdminClientBuilder(adminConfig).Build();
                // GetMetadata does a full Kafka handshake (ApiVersions + Metadata).
                // If this succeeds, the protocol layer is ready.
                admin.GetMetadata(TimeSpan.FromSeconds(2));
                return; // success
            }
            catch
            {
                await Task.Delay(200);
            }
        }
        // Don't throw — let individual tests report their own failures with clear
        // context rather than a generic "protocol not ready" from the fixture.
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_instance == this)
            _instance = null;

        if (_surgewave != null)
        {
            await _surgewave.DisposeAsync();
        }
        _loggerFactory.Dispose();
    }

    public void Dispose()
    {
        if (_disposed) return;
        DisposeAsync().GetAwaiter().GetResult();
    }
}

/// <summary>
/// Collection definition for sharing the broker fixture across multiple test classes.
/// </summary>
[CollectionDefinition("Broker")]
public class BrokerCollection : ICollectionFixture<BrokerFixture>
{
}
