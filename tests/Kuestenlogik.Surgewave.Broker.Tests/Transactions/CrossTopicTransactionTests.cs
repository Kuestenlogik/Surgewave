using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// Tests for cross-topic transaction support.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class CrossTopicTransactionTests : IAsyncDisposable
{
    private readonly string _testDirectory;
    private readonly LogManager _logManager;
    private readonly RecordBatchSerializer _serializer;
    private readonly CrossTopicTransactionConfig _config;
    private readonly CrossTopicTransactionManager _manager;

    public CrossTopicTransactionTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"surgewave-cross-txn-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);

        _logManager = new LogManager(_testDirectory, new MemoryLogSegmentFactory());
        _serializer = new RecordBatchSerializer(NullLogger<RecordBatchSerializer>.Instance);
        _config = new CrossTopicTransactionConfig
        {
            Enabled = true,
            DefaultTimeout = TimeSpan.FromSeconds(60),
            MaxTimeout = TimeSpan.FromMinutes(15),
            MaxPendingWrites = 10_000,
            CleanupIntervalSeconds = 300 // Long interval for tests
        };
        _manager = new CrossTopicTransactionManager(
            _logManager, _serializer, _config,
            NullLogger<CrossTopicTransactionManager>.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        await _manager.DisposeAsync();
        _logManager.Dispose();

        if (Directory.Exists(_testDirectory))
        {
            try { Directory.Delete(_testDirectory, recursive: true); }
            catch { /* ignore */ }
        }
    }

    [Fact]
    public void Begin_ReturnsTransaction()
    {
        var txn = _manager.Begin();

        Assert.NotNull(txn);
        Assert.NotEmpty(txn.TransactionId);
        Assert.Equal(CrossTopicTransactionState.Open, txn.State);
        Assert.Empty(txn.PendingWrites);
    }

    [Fact]
    public void Begin_WithProducerId_SetsProducerId()
    {
        var txn = _manager.Begin(producerId: "my-producer");

        Assert.Equal("my-producer", txn.ProducerId);
    }

    [Fact]
    public void Begin_WithTimeout_UsesCustomTimeout()
    {
        var txn = _manager.Begin(timeout: TimeSpan.FromSeconds(30));

        Assert.Equal(TimeSpan.FromSeconds(30), txn.Timeout);
    }

    [Fact]
    public void Begin_WithExcessiveTimeout_ClampedToMax()
    {
        var txn = _manager.Begin(timeout: TimeSpan.FromHours(1));

        Assert.Equal(_config.MaxTimeout, txn.Timeout);
    }

    [Fact]
    public void AddWrite_BuffersMessage()
    {
        var txn = _manager.Begin();
        var value = "test-value"u8.ToArray();

        _manager.AddWrite(txn.TransactionId, "topic-a", 0, null, value);

        Assert.Single(txn.PendingWrites);
        Assert.Equal("topic-a", txn.PendingWrites[0].Topic);
        Assert.Equal(0, txn.PendingWrites[0].Partition);
        Assert.Equal(value, txn.PendingWrites[0].Value);
    }

    [Fact]
    public void AddWrite_MultipleTopics_BuffersAll()
    {
        var txn = _manager.Begin();

        _manager.AddWrite(txn.TransactionId, "orders", 0, null, "order1"u8.ToArray());
        _manager.AddWrite(txn.TransactionId, "inventory", 0, null, "update1"u8.ToArray());
        _manager.AddWrite(txn.TransactionId, "notifications", 0, null, "notify1"u8.ToArray());

        Assert.Equal(3, txn.PendingWrites.Count);
        var topics = txn.GetTopics();
        Assert.Contains("orders", topics);
        Assert.Contains("inventory", topics);
        Assert.Contains("notifications", topics);
    }

    [Fact]
    public async Task Commit_WritesAllMessages()
    {
        // Create topics first
        await _logManager.CreateTopicAsync("orders", 1);
        await _logManager.CreateTopicAsync("inventory", 1);
        await _logManager.CreateTopicAsync(_config.TransactionLogTopic, 1);

        var txn = _manager.Begin();
        _manager.AddWrite(txn.TransactionId, "orders", 0, null, "order-data"u8.ToArray());
        _manager.AddWrite(txn.TransactionId, "inventory", 0, null, "inventory-data"u8.ToArray());

        var result = await _manager.CommitAsync(txn.TransactionId);

        Assert.True(result.Success);
        Assert.Equal(2, result.TopicsWritten);
        Assert.Equal(2, result.MessagesWritten);
        Assert.NotNull(result.Offsets);
        Assert.Contains("orders-0", result.Offsets.Keys);
        Assert.Contains("inventory-0", result.Offsets.Keys);
        Assert.Null(result.Error);
    }

    [Fact]
    public async Task Commit_EmptyTransaction_Succeeds()
    {
        var txn = _manager.Begin();

        var result = await _manager.CommitAsync(txn.TransactionId);

        Assert.True(result.Success);
        Assert.Equal(0, result.TopicsWritten);
        Assert.Equal(0, result.MessagesWritten);
    }

    [Fact]
    public async Task Commit_ReturnsOffsets()
    {
        await _logManager.CreateTopicAsync("test-topic", 1);
        await _logManager.CreateTopicAsync(_config.TransactionLogTopic, 1);

        var txn = _manager.Begin();
        _manager.AddWrite(txn.TransactionId, "test-topic", 0, null, "msg1"u8.ToArray());

        var result = await _manager.CommitAsync(txn.TransactionId);

        Assert.True(result.Success);
        Assert.NotNull(result.Offsets);
        Assert.True(result.Offsets.ContainsKey("test-topic-0"));
        Assert.True(result.Offsets["test-topic-0"] >= 0);
    }

    [Fact]
    public async Task Abort_DiscardsWrites()
    {
        var txn = _manager.Begin();
        _manager.AddWrite(txn.TransactionId, "topic-a", 0, null, "data"u8.ToArray());

        await _manager.AbortAsync(txn.TransactionId);

        Assert.Equal(CrossTopicTransactionState.Aborted, txn.State);
        Assert.Null(_manager.GetTransaction(txn.TransactionId));
    }

    [Fact]
    public async Task Abort_NonExistentTransaction_NoError()
    {
        await _manager.AbortAsync("non-existent-id");
        // Should not throw
    }

    [Fact]
    public async Task Timeout_AutoAborts()
    {
        var shortTimeoutConfig = new CrossTopicTransactionConfig
        {
            Enabled = true,
            DefaultTimeout = TimeSpan.FromMilliseconds(50),
            CleanupIntervalSeconds = 300
        };
        await using var manager = new CrossTopicTransactionManager(
            _logManager, _serializer, shortTimeoutConfig,
            NullLogger<CrossTopicTransactionManager>.Instance);

        var txn = manager.Begin(timeout: TimeSpan.FromMilliseconds(50));
        manager.AddWrite(txn.TransactionId, "topic", 0, null, "data"u8.ToArray());

        // Wait for timeout
        await Task.Delay(100);

        Assert.True(txn.IsTimedOut);

        // Commit should fail because timed out
        var result = await manager.CommitAsync(txn.TransactionId);
        Assert.False(result.Success);
    }

    [Fact]
    public async Task MultipleTopics_AtomicWrite()
    {
        await _logManager.CreateTopicAsync("topic-1", 1);
        await _logManager.CreateTopicAsync("topic-2", 1);
        await _logManager.CreateTopicAsync("topic-3", 1);
        await _logManager.CreateTopicAsync(_config.TransactionLogTopic, 1);

        var txn = _manager.Begin();
        _manager.AddWrite(txn.TransactionId, "topic-1", 0, "key1"u8.ToArray(), "value1"u8.ToArray());
        _manager.AddWrite(txn.TransactionId, "topic-2", 0, "key2"u8.ToArray(), "value2"u8.ToArray());
        _manager.AddWrite(txn.TransactionId, "topic-3", 0, "key3"u8.ToArray(), "value3"u8.ToArray());

        var result = await _manager.CommitAsync(txn.TransactionId);

        Assert.True(result.Success);
        Assert.Equal(3, result.TopicsWritten);
        Assert.Equal(3, result.MessagesWritten);
        Assert.Equal(3, result.Offsets!.Count);
    }

    [Fact]
    public async Task MaxPendingWrites_Enforced()
    {
        var limitedConfig = new CrossTopicTransactionConfig
        {
            MaxPendingWrites = 3,
            CleanupIntervalSeconds = 300
        };
        await using var limitedManager = new CrossTopicTransactionManager(
            _logManager, _serializer, limitedConfig,
            NullLogger<CrossTopicTransactionManager>.Instance);

        var txn = limitedManager.Begin();
        limitedManager.AddWrite(txn.TransactionId, "t", 0, null, "v1"u8.ToArray());
        limitedManager.AddWrite(txn.TransactionId, "t", 0, null, "v2"u8.ToArray());
        limitedManager.AddWrite(txn.TransactionId, "t", 0, null, "v3"u8.ToArray());

        Assert.Throws<InvalidOperationException>(() =>
            limitedManager.AddWrite(txn.TransactionId, "t", 0, null, "v4"u8.ToArray()));
    }

    [Fact]
    public void TransactionState_Transitions()
    {
        var txn = _manager.Begin();
        Assert.Equal(CrossTopicTransactionState.Open, txn.State);

        // After commit begins, state transitions
        // (we test the model directly)
        txn.State = CrossTopicTransactionState.Committing;
        Assert.Equal(CrossTopicTransactionState.Committing, txn.State);

        txn.State = CrossTopicTransactionState.Committed;
        Assert.Equal(CrossTopicTransactionState.Committed, txn.State);
    }

    [Fact]
    public void TransactionState_CannotAddWrites_WhenNotOpen()
    {
        var txn = _manager.Begin();
        txn.State = CrossTopicTransactionState.Committing;

        Assert.Throws<InvalidOperationException>(() =>
            txn.AddWrite("topic", 0, null, "data"u8.ToArray()));
    }

    [Fact]
    public void Config_Defaults()
    {
        var cfg = new CrossTopicTransactionConfig();

        Assert.True(cfg.Enabled);
        Assert.Equal(TimeSpan.FromSeconds(60), cfg.DefaultTimeout);
        Assert.Equal(TimeSpan.FromMinutes(15), cfg.MaxTimeout);
        Assert.Equal(10_000, cfg.MaxPendingWrites);
        Assert.Equal(30, cfg.CleanupIntervalSeconds);
        Assert.Equal("__cross_topic_txn_log", cfg.TransactionLogTopic);
    }

    [Fact]
    public void GetTransaction_ReturnsTransaction()
    {
        var txn = _manager.Begin();
        var retrieved = _manager.GetTransaction(txn.TransactionId);

        Assert.NotNull(retrieved);
        Assert.Equal(txn.TransactionId, retrieved.TransactionId);
    }

    [Fact]
    public void GetTransaction_NonExistent_ReturnsNull()
    {
        var result = _manager.GetTransaction("non-existent");
        Assert.Null(result);
    }

    [Fact]
    public void ListActive_ReturnsActiveTransactions()
    {
        var txn1 = _manager.Begin();
        var txn2 = _manager.Begin();

        var active = _manager.ListActive();

        Assert.Equal(2, active.Count);
    }

    [Fact]
    public async Task CleanupExpired_RemovesTimedOutTransactions()
    {
        var shortTimeoutConfig = new CrossTopicTransactionConfig
        {
            DefaultTimeout = TimeSpan.FromMilliseconds(10),
            CleanupIntervalSeconds = 300
        };
        await using var manager = new CrossTopicTransactionManager(
            _logManager, _serializer, shortTimeoutConfig,
            NullLogger<CrossTopicTransactionManager>.Instance);

        manager.Begin(timeout: TimeSpan.FromMilliseconds(10));
        manager.Begin(timeout: TimeSpan.FromMilliseconds(10));

        await Task.Delay(50);
        await manager.CleanupExpiredAsync();

        var active = manager.ListActive();
        Assert.Empty(active);
    }

    [Fact]
    public async Task Commit_NonExistentTransaction_ReturnsFailed()
    {
        var result = await _manager.CommitAsync("non-existent");

        Assert.False(result.Success);
        Assert.Equal("Transaction not found", result.Error);
    }

    [Fact]
    public async Task Commit_AlreadyCommitted_ReturnsFailed()
    {
        await _logManager.CreateTopicAsync("t", 1);
        await _logManager.CreateTopicAsync(_config.TransactionLogTopic, 1);

        var txn = _manager.Begin();
        _manager.AddWrite(txn.TransactionId, "t", 0, null, "data"u8.ToArray());

        var result1 = await _manager.CommitAsync(txn.TransactionId);
        Assert.True(result1.Success);

        // Second commit should fail (transaction removed after first commit)
        var result2 = await _manager.CommitAsync(txn.TransactionId);
        Assert.False(result2.Success);
    }
}
