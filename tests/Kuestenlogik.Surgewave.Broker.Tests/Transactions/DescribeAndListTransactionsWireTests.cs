using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// Wire-binding contract for the two transaction admin RPCs that previously
/// only existed as gRPC paths: <c>DescribeTransactions</c> (API 65) and
/// <c>ListTransactions</c> (API 66). The semantics are already covered by
/// <c>Kip994ListTransactionsFiltersTests</c> and the unit-level
/// DescribeTransactions tests; these tests pin the wire-shape mapping so a
/// refactor of the response builders can't silently corrupt the on-wire
/// representation that admin clients decode.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DescribeAndListTransactionsWireTests : IAsyncDisposable
{
    private readonly string _testDirectory;
    private readonly LogManager _logManager;
    private readonly ProducerStateManager _producerStateManager;
    private readonly TransactionIndex _transactionIndex;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _stateStore;
    private readonly TransactionCoordinator _coordinator;

    public DescribeAndListTransactionsWireTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "surgewave-tx-wire-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDirectory);
        _logManager = new LogManager(_testDirectory, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _producerStateManager = new ProducerStateManager();
        _transactionIndex = new TransactionIndex();
        _offsetStore = new OffsetStore(_testDirectory, NullLogger<OffsetStore>.Instance);
        _stateStore = new TransactionStateStore(Path.Combine(_testDirectory, "txn"), NullLogger<TransactionStateStore>.Instance);
        _coordinator = new TransactionCoordinator(
            _producerStateManager, _logManager, _transactionIndex, _offsetStore, _stateStore,
            NullLogger<TransactionCoordinator>.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        await _coordinator.DisposeAsync();
        _stateStore.Dispose();
        _offsetStore.Dispose();
        _logManager.Dispose();
        try { Directory.Delete(_testDirectory, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task HandleDescribeTransactions_KnownId_ReturnsPopulatedState()
    {
        await Init("orders-eu");
        var resp = _coordinator.HandleDescribeTransactions(new DescribeTransactionsRequest
        {
            ApiKey = ApiKey.DescribeTransactions,
            ApiVersion = 0,
            CorrelationId = 7,
            ClientId = "admin",
            TransactionalIds = ["orders-eu"],
        });

        var state = Assert.Single(resp.TransactionStates);
        Assert.Equal(7, resp.CorrelationId);
        Assert.Equal("orders-eu", state.TransactionalId);
        Assert.NotEqual(-1, state.ProducerId);
        Assert.Equal(ErrorCode.None, state.ErrorCode);
    }

    [Fact]
    public async Task HandleDescribeTransactions_UnknownId_ReturnsErrorRow()
    {
        await Init("present");
        var resp = _coordinator.HandleDescribeTransactions(new DescribeTransactionsRequest
        {
            ApiKey = ApiKey.DescribeTransactions,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "admin",
            TransactionalIds = ["present", "absent"],
        });

        // Unknown ids must surface a row with an error rather than be dropped —
        // admin clients correlate request/response indices.
        Assert.Equal(2, resp.TransactionStates.Count);
        var absent = resp.TransactionStates.Single(s => s.TransactionalId == "absent");
        Assert.NotEqual(ErrorCode.None, absent.ErrorCode);
    }

    [Fact]
    public async Task HandleListTransactions_NoFilters_ReturnsAllRegisteredIds()
    {
        await Init("a");
        await Init("b");
        await Init("c");

        var resp = _coordinator.HandleListTransactions(new ListTransactionsRequest
        {
            ApiKey = ApiKey.ListTransactions,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "admin",
            StateFilters = [],
            ProducerIdFilters = [],
        });

        Assert.Equal(ErrorCode.None, resp.ErrorCode);
        Assert.Equal(3, resp.TransactionStates.Count);
    }

    [Fact]
    public async Task HandleListTransactions_TransactionalIdPattern_FiltersByRegex()
    {
        // The semantic suite (Kip994ListTransactionsFiltersTests) covers the
        // pattern logic; this test only verifies the wire handler hands the
        // pattern through to the underlying ListTransactions call.
        await Init("orders-eu");
        await Init("orders-us");
        await Init("payments-eu");

        var resp = _coordinator.HandleListTransactions(new ListTransactionsRequest
        {
            ApiKey = ApiKey.ListTransactions,
            ApiVersion = 2,
            CorrelationId = 1,
            ClientId = "admin",
            StateFilters = [],
            ProducerIdFilters = [],
            DurationFilter = -1,
            TransactionalIdPattern = "^orders.*$",
        });

        Assert.Equal(2, resp.TransactionStates.Count);
        Assert.All(resp.TransactionStates, t => Assert.StartsWith("orders", t.TransactionalId));
    }

    [Fact]
    public async Task HandleListTransactions_PreservesProducerIdFromCoordinator()
    {
        await Init("only-one");

        var resp = _coordinator.HandleListTransactions(new ListTransactionsRequest
        {
            ApiKey = ApiKey.ListTransactions,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "admin",
            StateFilters = [],
            ProducerIdFilters = [],
        });

        var listing = Assert.Single(resp.TransactionStates);
        Assert.Equal("only-one", listing.TransactionalId);
        Assert.NotEqual(-1, listing.ProducerId); // assigned by InitProducerId
        Assert.Equal("Empty", listing.TransactionState); // fresh init → Empty
    }

    private Task Init(string txnId) =>
        _coordinator.HandleInitProducerIdAsync(new InitProducerIdRequest
        {
            ApiKey = ApiKey.InitProducerId,
            ApiVersion = 4,
            CorrelationId = 0,
            ClientId = "wire-test",
            TransactionalId = txnId,
            TransactionTimeoutMs = 60_000,
            ProducerId = -1,
            ProducerEpoch = -1,
        }, CancellationToken.None);
}
