using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Runs the coordinator's expiry pass on a timer: transactions past their timeout are aborted with the
/// producer fenced, and transactions left prepared (a restart, a cancelled EndTxn) are completed. The pass
/// itself lives in the coordinator, so it takes the same locks and transitions as EndTxn.
/// </summary>
internal sealed class TransactionTimeoutManager : IAsyncDisposable
{
    private readonly Func<DateTimeOffset, CancellationToken, Task> _expire;
    private readonly ILogger _logger;
    private readonly Timer _timeoutCheckTimer;
    private readonly CancellationTokenSource _shutdownCts = new();
    private int _running;
    private const int TimeoutCheckIntervalMs = 5000; // Check every 5 seconds

    public TransactionTimeoutManager(Func<DateTimeOffset, CancellationToken, Task> expire, ILogger logger)
    {
        _expire = expire;
        _logger = logger;

        _timeoutCheckTimer = new Timer(
            callback: _ => _ = CheckTransactionTimeoutsAsync(),
            state: null,
            dueTime: TimeSpan.FromMilliseconds(TimeoutCheckIntervalMs),
            period: TimeSpan.FromMilliseconds(TimeoutCheckIntervalMs));
    }

    private async Task CheckTransactionTimeoutsAsync()
    {
        // A pass that takes longer than the interval is not overlapped by the next one.
        if (Interlocked.Exchange(ref _running, 1) == 1)
            return;
        try
        {
            await _expire(DateTimeOffset.UtcNow, _shutdownCts.Token);
        }
        catch (OperationCanceledException) when (_shutdownCts.IsCancellationRequested)
        {
            // Shutting down.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking transaction timeouts");
        }
        finally
        {
            Volatile.Write(ref _running, 0);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _shutdownCts.CancelAsync();
        await _timeoutCheckTimer.DisposeAsync();
        _shutdownCts.Dispose();
    }
}
