using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt.Tests;

/// <summary>
/// Pins the hosted-service lifecycle of <see cref="MqttProtocolAdapter"/> when the protocol is
/// disabled: ExecuteAsync must return immediately without binding a port, and the adapter must
/// stop and dispose cleanly without ever having created an MQTT server.
/// </summary>
public sealed class MqttProtocolAdapterLifecycleTests : IDisposable
{
    private readonly LogManager _logManager = TestLogManager.CreateInMemory();

    public void Dispose() => _logManager.Dispose();

    private MqttProtocolAdapter CreateAdapter(MqttConfig config)
        => new(Options.Create(config), _logManager, NullLogger<MqttProtocolAdapter>.Instance);

    [Fact]
    public void ActiveClients_IsZeroBeforeStart()
    {
        using var adapter = CreateAdapter(new MqttConfig { Enabled = false });

        Assert.Equal(0, adapter.ActiveClients);
    }

    [Fact]
    public async Task DisabledAdapter_CompletesImmediatelyWithoutServer()
    {
        using var adapter = CreateAdapter(new MqttConfig { Enabled = false });

        await adapter.StartAsync(CancellationToken.None);

        Assert.NotNull(adapter.ExecuteTask);
        Assert.True(adapter.ExecuteTask!.IsCompletedSuccessfully);
        Assert.Equal(0, adapter.ActiveClients);

        await adapter.StopAsync(CancellationToken.None);
    }

    [Fact]
    public void Dispose_WithoutStart_DoesNotThrow()
    {
        var adapter = CreateAdapter(new MqttConfig { Enabled = false });

        var exception = Record.Exception(adapter.Dispose);

        Assert.Null(exception);
    }
}
