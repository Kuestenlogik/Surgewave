namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Event arguments for consumer disconnection events.
/// </summary>
public sealed class DisconnectedEventArgs : EventArgs
{
    /// <summary>
    /// The exception that caused the disconnection.
    /// </summary>
    public Exception Exception { get; }

    /// <summary>
    /// Creates a new DisconnectedEventArgs.
    /// </summary>
    public DisconnectedEventArgs(Exception exception)
    {
        Exception = exception;
    }
}
