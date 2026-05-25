using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// KIP-892 Transactions Server-Side Defense: the broker — not the client — owns the
/// producer epoch. A request that names a stale (producer-id, epoch) for an existing
/// transactional-id is fenced with <see cref="ErrorCode.InvalidProducerEpoch"/>; a
/// fresh-init from the same transactional-id over no in-flight txn forces the epoch
/// upward so any zombie still using the previous incarnation is fenced on its very
/// next request.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip892TransactionDefenseTests : IAsyncDisposable
{
    private const long NoProducerId = -1;
    private const short NoProducerEpoch = -1;

    private readonly string _testDirectory;
    private readonly LogManager _logManager;
    private readonly ProducerStateManager _producerStateManager;
    private readonly TransactionIndex _transactionIndex;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _stateStore;
    private readonly TransactionCoordinator _coordinator;

    public Kip892TransactionDefenseTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"surgewave-kip892-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);
        _logManager = new LogManager(_testDirectory, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _producerStateManager = new ProducerStateManager();
        _transactionIndex = new TransactionIndex();
        _offsetStore = new OffsetStore(_testDirectory, NullLogger<OffsetStore>.Instance);
        _stateStore = new TransactionStateStore(Path.Combine(_testDirectory, "txn-state"), NullLogger<TransactionStateStore>.Instance);

        _coordinator = new TransactionCoordinator(
            _producerStateManager,
            _logManager,
            _transactionIndex,
            _offsetStore,
            _stateStore,
            NullLogger<TransactionCoordinator>.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        await _coordinator.DisposeAsync();
        _offsetStore.Dispose();
        _stateStore.Dispose();
        _logManager.Dispose();
        try { Directory.Delete(_testDirectory, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task FreshInit_AllocatesProducerIdAndEpochFromBroker()
    {
        var resp = await Init("tx-fresh", NoProducerId, NoProducerEpoch);
        Assert.Equal(ErrorCode.None, resp.ErrorCode);
        Assert.True(resp.ProducerId > 0);
        // First epoch after the fresh-init bump is 1 (initial 0 → bumped to 1).
        Assert.True(resp.ProducerEpoch >= 1);
    }

    [Fact]
    public async Task ReInit_WithCurrentPidAndEpoch_IsIdempotent()
    {
        var first = await Init("tx-idem", NoProducerId, NoProducerEpoch);

        var retry = await Init("tx-idem", first.ProducerId, first.ProducerEpoch);

        Assert.Equal(ErrorCode.None, retry.ErrorCode);
        Assert.Equal(first.ProducerId, retry.ProducerId);
        Assert.Equal(first.ProducerEpoch, retry.ProducerEpoch);
    }

    [Fact]
    public async Task ZombieProducer_OlderEpoch_IsFencedWithInvalidProducerEpoch()
    {
        // Live producer establishes pid + epoch.
        var live = await Init("tx-zombie", NoProducerId, NoProducerEpoch);

        // A fresh init from a new client over the same txn-id — bumps the server's epoch.
        var renewed = await Init("tx-zombie", NoProducerId, NoProducerEpoch);
        Assert.True(renewed.ProducerEpoch > live.ProducerEpoch);

        // The original producer (now a zombie) tries to InitProducerId again with its
        // old (pid, epoch). Server must fence.
        var zombie = await Init("tx-zombie", live.ProducerId, live.ProducerEpoch);

        Assert.Equal(ErrorCode.InvalidProducerEpoch, zombie.ErrorCode);
        // The response carries the current authoritative state so a polite client
        // can update its own bookkeeping.
        Assert.Equal(renewed.ProducerEpoch, zombie.ProducerEpoch);
    }

    [Fact]
    public async Task ForeignProducerId_OnExistingTransactionalId_IsFenced()
    {
        var live = await Init("tx-foreign", NoProducerId, NoProducerEpoch);

        // A request that claims a different producer-id for the same txn-id is
        // by definition not the current incarnation and must be fenced.
        var foreigner = await Init("tx-foreign", producerId: live.ProducerId + 9999, live.ProducerEpoch);

        Assert.Equal(ErrorCode.InvalidProducerEpoch, foreigner.ErrorCode);
    }

    [Fact]
    public async Task FreshInit_OnExistingTransactionalId_BumpsEpoch()
    {
        var first = await Init("tx-bump", NoProducerId, NoProducerEpoch);
        var second = await Init("tx-bump", NoProducerId, NoProducerEpoch);

        Assert.Equal(first.ProducerId, second.ProducerId); // same identity
        Assert.True(second.ProducerEpoch > first.ProducerEpoch); // server bumped
    }

    private Task<InitProducerIdResponse> Init(string txnId, long producerId, short producerEpoch) =>
        _coordinator.HandleInitProducerIdAsync(new InitProducerIdRequest
        {
            ApiKey = ApiKey.InitProducerId,
            ApiVersion = 4,
            CorrelationId = 0,
            ClientId = "kip892-test",
            TransactionalId = txnId,
            TransactionTimeoutMs = 60_000,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
        }, CancellationToken.None);
}
