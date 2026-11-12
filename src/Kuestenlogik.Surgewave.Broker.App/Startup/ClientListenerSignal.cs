namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// The moment the broker accepts its clients, for the parts of the host that are clients of the broker themselves
/// (#268): the embedded Connect worker connects to the broker's own listener, which accepts connections only once the
/// broker has caught up with its metadata log (#250).
/// </summary>
/// <remarks>
/// The broker is built after the services are, and a plugin is handed the services: the host passes the broker's
/// accept signal on through this one. A part that waits here does not hold up the host's start — it is the host's
/// start that brings the signal about.
/// </remarks>
public sealed class ClientListenerSignal
{
    private readonly TaskCompletionSource _accepting = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Completes when the broker accepts the connections made to its client listener. Fails with what kept the
    /// listener from binding; cancelled with <paramref name="cancellationToken"/>, and when the broker stops before it
    /// accepted a connection.
    /// </summary>
    public Task WaitUntilAcceptingAsync(CancellationToken cancellationToken) => _accepting.Task.WaitAsync(cancellationToken);

    /// <summary>
    /// Passes on <paramref name="accepting"/>, the broker's own signal
    /// (<see cref="SurgewaveBroker.WaitUntilAcceptingAsync"/>), as it ends.
    /// </summary>
    public void Follow(Task accepting)
    {
        ArgumentNullException.ThrowIfNull(accepting);
        _ = PassOnAsync(accepting);
    }

    private async Task PassOnAsync(Task accepting)
    {
        try
        {
            await accepting.ConfigureAwait(false);
            _accepting.TrySetResult();
        }
        catch (OperationCanceledException)
        {
            _accepting.TrySetCanceled();
        }
        catch (Exception ex)
        {
            _accepting.TrySetException(ex);
        }
    }
}
