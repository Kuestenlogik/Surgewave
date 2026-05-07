using System.Reflection;
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
/// KIP-994 (DurationFilter) and KIP-1152 (TransactionalIdPattern) — admin filters
/// on <c>ListTransactions</c> v1+/v2+. The wire-level fields are covered elsewhere;
/// this suite drives <see cref="TransactionCoordinator.ListTransactions"/> directly
/// to verify the server-side filter semantics.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip994ListTransactionsFiltersTests : IAsyncDisposable
{
    private readonly string _testDirectory;
    private readonly LogManager _logManager;
    private readonly ProducerStateManager _producerStateManager;
    private readonly TransactionIndex _transactionIndex;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _stateStore;
    private readonly TransactionCoordinator _coordinator;

    public Kip994ListTransactionsFiltersTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "surgewave-kip994-" + Guid.NewGuid().ToString("N"));
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
    public async Task DurationFilter_ReturnsOnlyTransactionsOlderThanCutoff()
    {
        await Init("old-1");
        await Init("old-2");
        var seedTime = DateTimeOffset.UtcNow.AddSeconds(-30);
        BackdateLastActivity("old-1", seedTime);
        BackdateLastActivity("old-2", seedTime);

        await Init("fresh-1"); // last-activity = now

        // Older than 10 seconds → only the two back-dated ids.
        var result = _coordinator.ListTransactions(minDurationMs: 10_000);

        var ids = result.Select(t => t.TransactionalId).OrderBy(s => s).ToList();
        Assert.Equal(["old-1", "old-2"], ids);
    }

    [Fact]
    public async Task DurationFilter_ZeroOrNegative_DisablesFilter()
    {
        await Init("a");
        await Init("b");

        var allWithZero = _coordinator.ListTransactions(minDurationMs: 0);
        var allWithNegative = _coordinator.ListTransactions(minDurationMs: -1);

        Assert.Equal(2, allWithZero.Count);
        Assert.Equal(2, allWithNegative.Count);
    }

    [Fact]
    public async Task TransactionalIdPattern_AppliesRegex()
    {
        await Init("orders-eu");
        await Init("orders-us");
        await Init("payments-eu");

        var ordersOnly = _coordinator.ListTransactions(transactionalIdPattern: "^orders-.*$");

        var ids = ordersOnly.Select(t => t.TransactionalId).OrderBy(s => s).ToList();
        Assert.Equal(["orders-eu", "orders-us"], ids);
    }

    [Fact]
    public async Task TransactionalIdPattern_InvalidRegex_FallsThroughToNoFilter()
    {
        await Init("a");
        await Init("b");

        // Bad regex: unclosed bracket. Surgewave degrades to "no filter" instead of
        // failing the listing — matching librdkafka's permissive client side.
        var result = _coordinator.ListTransactions(transactionalIdPattern: "[unclosed");

        Assert.Equal(2, result.Count);
    }

    [Fact]
    public async Task TransactionalIdPattern_NullOrEmpty_NoFilter()
    {
        await Init("a");

        var nullResult = _coordinator.ListTransactions(transactionalIdPattern: null);
        var emptyResult = _coordinator.ListTransactions(transactionalIdPattern: string.Empty);

        Assert.Single(nullResult);
        Assert.Single(emptyResult);
    }

    [Fact]
    public async Task DurationFilter_AndTransactionalIdPattern_CombineAsAnd()
    {
        await Init("orders-eu");
        await Init("orders-us");
        await Init("payments-eu");
        BackdateLastActivity("orders-eu", DateTimeOffset.UtcNow.AddSeconds(-30));
        BackdateLastActivity("payments-eu", DateTimeOffset.UtcNow.AddSeconds(-30));

        // Older than 10 s AND matches ^orders.*$ — only orders-eu qualifies.
        var result = _coordinator.ListTransactions(
            minDurationMs: 10_000,
            transactionalIdPattern: "^orders.*$");

        var listing = Assert.Single(result);
        Assert.Equal("orders-eu", listing.TransactionalId);
    }

    [Fact]
    public async Task AllFourFilters_ComposeAsAnd()
    {
        // States × producer ids × duration × pattern — verify they combine
        // as a logical AND so an admin tool can narrow a listing to a single
        // candidate without server-side surprises.
        await Init("orders-eu");      // producer 0
        await Init("orders-us");      // producer 1
        await Init("payments-eu");    // producer 2
        await Init("orders-asia");    // producer 3

        var producers = ProducerIdsByTxnId();

        BackdateLastActivity("orders-eu", DateTimeOffset.UtcNow.AddSeconds(-30));
        BackdateLastActivity("orders-us", DateTimeOffset.UtcNow.AddSeconds(-30));

        // Empty state (PrepareCommit etc. would be set by EndTxn — we don't
        // get there in this test) → all transactions are in Empty after init,
        // so filtering on Empty must include them; filtering on Ongoing
        // excludes them.
        var emptyMatches = _coordinator.ListTransactions(
            statesFilter: ["Empty"],
            producerIdFilter: [producers["orders-eu"], producers["orders-us"]],
            minDurationMs: 10_000,
            transactionalIdPattern: "^orders.*$");

        var ids = emptyMatches.Select(t => t.TransactionalId).OrderBy(s => s).ToList();
        Assert.Equal(["orders-eu", "orders-us"], ids);

        // Ongoing rules out everything — same producer / duration / pattern
        // narrows to no results because all four conditions must hold.
        var ongoingMatches = _coordinator.ListTransactions(
            statesFilter: ["Ongoing"],
            producerIdFilter: [producers["orders-eu"], producers["orders-us"]],
            minDurationMs: 10_000,
            transactionalIdPattern: "^orders.*$");
        Assert.Empty(ongoingMatches);
    }

    [Fact]
    public async Task TransactionalIdPattern_PathologicalRegex_TimesOutWithoutHanging()
    {
        // (a+)+$ on a long pure-'a' string is the canonical Cox/RE2-bait
        // regex — backtracking-based engines hang. Surgewave compiles patterns
        // with a 50 ms MatchTimeout so the broker can't be DoS'd by an
        // admin who supplies a bad regex; the coordinator catches the
        // RegexMatchTimeoutException and treats the txnId as "no match",
        // mirroring the bad-pattern-degrades-to-no-filter contract.
        var pathological = new string('a', 4096) + "!";
        await Init(pathological);
        await Init("normal-id");

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = _coordinator.ListTransactions(transactionalIdPattern: "^(a+)+$");
        sw.Stop();

        // Must complete in well under a second — the regex timeout caps each
        // IsMatch at 50 ms, and we only have two ids to scan.
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2),
            $"Pathological regex took {sw.ElapsedMilliseconds} ms — timeout did not engage");

        // The "normal-id" never matched — the pathological id either
        // matched or threw (caught + skipped). Neither outcome should
        // produce more than one result, and zero is also acceptable.
        Assert.True(result.Count <= 1);
    }

    private Dictionary<string, long> ProducerIdsByTxnId()
    {
        var dictField = typeof(TransactionCoordinator)
            .GetField("_transactionsByTxnId", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var dict = (System.Collections.IDictionary)dictField.GetValue(_coordinator)!;
        var result = new Dictionary<string, long>();
        foreach (System.Collections.DictionaryEntry entry in dict)
        {
            var txnId = (string)entry.Key;
            var producerId = (long)entry.Value!.GetType().GetProperty("ProducerId")!.GetValue(entry.Value)!;
            result[txnId] = producerId;
        }
        return result;
    }

    private Task Init(string txnId) =>
        _coordinator.HandleInitProducerIdAsync(new InitProducerIdRequest
        {
            ApiKey = ApiKey.InitProducerId,
            ApiVersion = 4,
            CorrelationId = 0,
            ClientId = "kip994-test",
            TransactionalId = txnId,
            TransactionTimeoutMs = 60_000,
            ProducerId = -1,
            ProducerEpoch = -1,
        }, CancellationToken.None);

    /// <summary>
    /// Reach into the coordinator's private dictionary and back-date a transaction's
    /// LastActivityTime so the duration filter can fire without waiting wall-clock.
    /// </summary>
    private void BackdateLastActivity(string txnId, DateTimeOffset newTime)
    {
        var dictField = typeof(TransactionCoordinator)
            .GetField("_transactionsByTxnId", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var dict = dictField.GetValue(_coordinator)!;
        var indexer = dict.GetType().GetProperty("Item", new[] { typeof(string) })!;
        var txn = indexer.GetValue(dict, [txnId])!;
        var lastActivity = txn.GetType().GetProperty("LastActivityTime")!;
        lastActivity.SetValue(txn, newTime);
    }
}
