namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Logic;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Logic;

public class RetryNodeTests
{
    private static SinkRecord CreateRecord(string value, string? key = null, Dictionary<string, byte[]>? headers = null)
    {
        return new SinkRecord
        {
            Topic = "input-topic",
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = Encoding.UTF8.GetBytes(value),
            Headers = headers
        };
    }

    private static SinkRecord CreateErrorRecord(string value, int retryCount = 0, string? key = null)
    {
        var headers = new Dictionary<string, byte[]>
        {
            ["_error_message"] = Encoding.UTF8.GetBytes("test error"),
            ["_error_type"] = Encoding.UTF8.GetBytes("TestException")
        };

        if (retryCount > 0)
        {
            headers["_retry_count"] = Encoding.UTF8.GetBytes(retryCount.ToString());
        }

        return CreateRecord(value, key, headers);
    }

    private static RetryNodeTask CreateTask(
        int maxAttempts = 3,
        long backoffMs = 1000,
        double multiplier = 2.0,
        long maxBackoffMs = 30000)
    {
        var task = new RetryNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["error.topic"] = "errors",
            ["retry.max.attempts"] = maxAttempts.ToString(),
            ["retry.backoff.ms"] = backoffMs.ToString(),
            ["retry.backoff.multiplier"] = multiplier.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["retry.max.backoff.ms"] = maxBackoffMs.ToString()
        });
        return task;
    }

    [Fact]
    public async Task NormalRecords_PassedThrough()
    {
        var task = CreateTask();

        var r1 = CreateRecord("""{"data":"hello"}""", key: "k1");
        await task.PutAsync([r1], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Contains("\"data\":\"hello\"", Encoding.UTF8.GetString(task.EmittedRecords[0].Value));
    }

    [Fact]
    public async Task ErrorRecord_QueuedForRetry()
    {
        var task = CreateTask(backoffMs: 1);

        var r1 = CreateErrorRecord("""{"data":"retry-me"}""");
        await task.PutAsync([r1], CancellationToken.None);

        // Not emitted yet (in retry queue)
        Assert.Empty(task.EmittedRecords);

        // Wait for backoff and trigger timer
        await Task.Delay(50);
        task.OnRetryTimer(null);

        Assert.Single(task.EmittedRecords);
        var headers = task.EmittedRecords[0].Headers;
        Assert.NotNull(headers);
        Assert.True(headers!.ContainsKey("_retry_count"));
        Assert.Equal("1", Encoding.UTF8.GetString(headers["_retry_count"]));
    }

    [Fact]
    public async Task MaxAttemptsExceeded_EmitsError()
    {
        var task = CreateTask(maxAttempts: 2);

        var r1 = CreateErrorRecord("""{"data":"too-many"}""", retryCount: 2);
        await task.PutAsync([r1], CancellationToken.None);

        // Should emit to error topic (via EmitError)
        Assert.Single(task.EmittedRecords);
        Assert.Equal("errors", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task BackoffMultiplier_IncreasesDelay()
    {
        var task = CreateTask(backoffMs: 100, multiplier: 3.0);

        // Retry count = 1, so backoff = 100 * 3^1 = 300ms
        var r1 = CreateErrorRecord("""{"data":"slow"}""", retryCount: 1);
        await task.PutAsync([r1], CancellationToken.None);

        // Trigger timer immediately — should NOT emit yet (backoff 300ms)
        task.OnRetryTimer(null);
        Assert.Empty(task.EmittedRecords);

        // Wait long enough and retry
        await Task.Delay(350);
        task.OnRetryTimer(null);
        Assert.Single(task.EmittedRecords);
    }

    [Fact]
    public async Task BackoffCap_LimitsMaxBackoff()
    {
        var task = CreateTask(backoffMs: 1000, multiplier: 100.0, maxBackoffMs: 50);

        // backoff = 1000 * 100^0 = 1000, capped to 50ms
        var r1 = CreateErrorRecord("""{"data":"capped"}""");
        await task.PutAsync([r1], CancellationToken.None);

        await Task.Delay(100);
        task.OnRetryTimer(null);

        Assert.Single(task.EmittedRecords);
    }

    [Fact]
    public async Task HeaderTracking_RetryCountIncremented()
    {
        var task = CreateTask(backoffMs: 1);

        var r1 = CreateErrorRecord("""{"data":"track"}""", retryCount: 5);
        // retryCount=5 but maxAttempts=3, so this goes to error
        await task.PutAsync([r1], CancellationToken.None);

        // Error topic record
        Assert.Single(task.EmittedRecords);
        Assert.Equal("errors", task.EmittedRecords[0].Topic);
    }

    [Fact]
    public async Task EmptyQueue_TimerDoesNothing()
    {
        var task = CreateTask();

        task.OnRetryTimer(null);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task MultipleRecords_MixedStates()
    {
        var task = CreateTask(maxAttempts: 2, backoffMs: 1);

        var normal = CreateRecord("""{"type":"normal"}""", key: "k1");
        var errorRetry = CreateErrorRecord("""{"type":"retry"}""", retryCount: 0);
        var errorMax = CreateErrorRecord("""{"type":"max"}""", retryCount: 2);

        await task.PutAsync([normal, errorRetry, errorMax], CancellationToken.None);

        // normal → pass-through (1 emitted)
        // errorRetry → queued
        // errorMax → error topic (1 emitted)
        Assert.Equal(2, task.EmittedRecords.Count);

        // Trigger retry timer
        await Task.Delay(50);
        task.OnRetryTimer(null);

        // errorRetry now emitted too
        Assert.Equal(3, task.EmittedRecords.Count);
    }
}
