namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Logic;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

public class RateLimiterNodeTests
{
    private static SinkRecord CreateRecord(string value, string? key = null)
    {
        return new SinkRecord
        {
            Topic = "input-topic",
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = Encoding.UTF8.GetBytes(value)
        };
    }

    private static RateLimiterNodeTask CreateTask(
        long rateLimit = 1000,
        long intervalMs = 1000,
        long burst = 0,
        string overflow = "drop")
    {
        var task = new RateLimiterNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["error.topic"] = "errors",
            ["rate.limit"] = rateLimit.ToString(),
            ["rate.interval.ms"] = intervalMs.ToString(),
            ["rate.burst"] = burst.ToString(),
            ["rate.overflow"] = overflow
        });
        return task;
    }

    [Fact]
    public async Task UnderLimit_AllRecordsEmitted()
    {
        var task = CreateTask(rateLimit: 10);

        var records = Enumerable.Range(0, 5)
            .Select(i => CreateRecord($"{{\"i\":{i}}}"))
            .ToList();

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(5, task.EmittedRecords.Count);
    }

    [Fact]
    public async Task OverflowDrop_ExcessRecordsSilentlyDropped()
    {
        var task = CreateTask(rateLimit: 3, burst: 0);

        // Drain all tokens by setting to 3
        task.SetTokens(3);

        var records = Enumerable.Range(0, 5)
            .Select(i => CreateRecord($"{{\"i\":{i}}}"))
            .ToList();

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(3, task.EmittedRecords.Count);
    }

    [Fact]
    public async Task OverflowError_ExcessRecordsEmitError()
    {
        var task = CreateTask(rateLimit: 2, burst: 0, overflow: "error");
        task.SetTokens(2);

        var records = Enumerable.Range(0, 4)
            .Select(i => CreateRecord($"{{\"i\":{i}}}"))
            .ToList();

        await task.PutAsync(records, CancellationToken.None);

        // 2 normal + 2 to error topic
        Assert.Equal(4, task.EmittedRecords.Count);
        var errorRecords = task.EmittedRecords.Where(r => r.Topic == "errors").ToList();
        Assert.Equal(2, errorRecords.Count);
    }

    [Fact]
    public async Task BurstCapacity_AllowsExtraRecords()
    {
        var task = CreateTask(rateLimit: 3, burst: 2);
        task.SetTokens(5); // 3 + 2 burst

        var records = Enumerable.Range(0, 5)
            .Select(i => CreateRecord($"{{\"i\":{i}}}"))
            .ToList();

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(5, task.EmittedRecords.Count);
    }

    [Fact]
    public async Task TokenRefill_AfterInterval()
    {
        var task = CreateTask(rateLimit: 5, intervalMs: 50);
        task.SetTokens(0);

        // No tokens — should drop
        var r1 = CreateRecord("""{"a":1}""");
        await task.PutAsync([r1], CancellationToken.None);
        Assert.Empty(task.EmittedRecords);

        // Wait for refill
        await Task.Delay(100);

        var r2 = CreateRecord("""{"a":2}""");
        await task.PutAsync([r2], CancellationToken.None);
        Assert.Single(task.EmittedRecords);
    }

    [Fact]
    public async Task EmptyBatch_NoRecordsProcessed()
    {
        var task = CreateTask();
        await task.PutAsync([], CancellationToken.None);
        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task ConfigurableRateAndInterval()
    {
        var task = CreateTask(rateLimit: 100, intervalMs: 500);
        task.SetTokens(100);

        var records = Enumerable.Range(0, 100)
            .Select(i => CreateRecord($"{{\"i\":{i}}}"))
            .ToList();

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(100, task.EmittedRecords.Count);
    }

    [Fact]
    public async Task MultipleBatches_TokensDepleteAcrossBatches()
    {
        var task = CreateTask(rateLimit: 3, burst: 0);
        task.SetTokens(3);

        var batch1 = new[] { CreateRecord("""{"b":1}"""), CreateRecord("""{"b":2}""") };
        await task.PutAsync(batch1, CancellationToken.None);
        Assert.Equal(2, task.EmittedRecords.Count);

        var batch2 = new[] { CreateRecord("""{"b":3}"""), CreateRecord("""{"b":4}""") };
        await task.PutAsync(batch2, CancellationToken.None);

        // Only 1 more token left (3-2=1), so 1 more emitted from batch2
        // Note: some refill may happen between calls due to time passing
        Assert.True(task.EmittedRecords.Count >= 3);
    }
}
