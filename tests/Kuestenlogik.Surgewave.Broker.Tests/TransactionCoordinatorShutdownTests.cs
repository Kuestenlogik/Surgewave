using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// A clean stop disposes the transaction coordinator without an exception (#209). The broker app registers it once
/// and forwards three contracts to it with factories; the container disposes every factory result, so the same
/// instance is disposed several times. The second dispose threw <see cref="ObjectDisposedException"/> from the timeout
/// manager's cancellation source, and the broker exited with code 1 on every Ctrl+C.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransactionCoordinatorShutdownTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "surgewave-txn-shutdown-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _stateStore;

    public TransactionCoordinatorShutdownTests()
    {
        Directory.CreateDirectory(_dir);
        _logManager = TestLogManager.CreateInMemory(_dir);
        _offsetStore = OffsetStore.InMemory();
        _stateStore = new TransactionStateStore(_dir, NullLogger<TransactionStateStore>.Instance);
    }

    public void Dispose()
    {
        _stateStore.Dispose();
        _offsetStore.Dispose();
        _logManager.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task TheContainerDisposesTheCoordinatorBehindEveryForwardedContractWithoutAnException()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_ => new TransactionCoordinator(
            new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore, _stateStore,
            NullLogger<TransactionCoordinator>.Instance));
        services.AddSingleton<ITransactionCoordinator>(sp => sp.GetRequiredService<TransactionCoordinator>());
        services.AddSingleton<IProduceTransactionCoordinator>(sp => sp.GetRequiredService<TransactionCoordinator>());
        services.AddSingleton<ITransactionMarkerSink>(sp => sp.GetRequiredService<TransactionCoordinator>());

        var provider = services.BuildServiceProvider();
        var coordinator = provider.GetRequiredService<TransactionCoordinator>();
        Assert.Same(coordinator, provider.GetRequiredService<ITransactionCoordinator>());
        Assert.Same(coordinator, provider.GetRequiredService<IProduceTransactionCoordinator>());
        Assert.Same(coordinator, provider.GetRequiredService<ITransactionMarkerSink>());

        await provider.DisposeAsync();
    }
}
