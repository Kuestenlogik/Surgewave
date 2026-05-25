using Kuestenlogik.Surgewave.Protocol.Quic;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

public class QuicBrokerAdapterLifecycleTests
{
    private static QuicConfig DisabledConfig => new()
    {
        Enabled = false, Port = 0, MaxConnections = 10,
        MaxStreamsPerConnection = 4, IdleTimeoutSeconds = 10
    };

    [Fact]
    public async Task StartAsync_WhenDisabled_CompletesImmediately()
    {
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            var adapter = new QuicBrokerAdapter(Options.Create(DisabledConfig), NullLogger<QuicBrokerAdapter>.Instance);
            using (adapter)
            {
                await adapter.StartAsync(CancellationToken.None);
                await adapter.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task StopAsync_WithoutStart_DoesNotThrow()
    {
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            var adapter = new QuicBrokerAdapter(Options.Create(DisabledConfig), NullLogger<QuicBrokerAdapter>.Instance);
            using (adapter)
            {
                await adapter.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task Dispose_Idempotent()
    {
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            var adapter = new QuicBrokerAdapter(Options.Create(DisabledConfig), NullLogger<QuicBrokerAdapter>.Instance);
            await adapter.StartAsync(CancellationToken.None);
            adapter.Dispose();
            adapter.Dispose();
        }
    }

    [Fact]
    public async Task StartAsync_ThenCancelledToken_StopsGracefully()
    {
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            var adapter = new QuicBrokerAdapter(Options.Create(DisabledConfig), NullLogger<QuicBrokerAdapter>.Instance);
            using (adapter)
            {
                using var cts = new CancellationTokenSource();
                await adapter.StartAsync(cts.Token);
                await cts.CancelAsync();
                await adapter.StopAsync(CancellationToken.None);
            }
        }
    }
}
