namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Listener for state store restoration progress.
/// Called during changelog-backed store restoration from topic.
/// </summary>
public interface IStateRestoreListener
{
    /// <summary>
    /// Called when restoration starts for a state store.
    /// </summary>
    void OnRestoreStart(StateRestoreContext context);

    /// <summary>
    /// Called when a batch of records has been restored.
    /// </summary>
    void OnBatchRestored(StateRestoreContext context, int numRestored);

    /// <summary>
    /// Called when restoration completes for a state store.
    /// </summary>
    void OnRestoreEnd(StateRestoreContext context, long totalRestored);
}

/// <summary>
/// Context information about a state store restoration.
/// </summary>
public sealed class StateRestoreContext
{
    /// <summary>
    /// Name of the state store being restored.
    /// </summary>
    public required string StoreName { get; init; }

    /// <summary>
    /// Topic being read for restoration.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Partition being restored.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// Starting offset for restoration.
    /// </summary>
    public long StartingOffset { get; init; }

    /// <summary>
    /// Ending offset (high watermark) for restoration.
    /// </summary>
    public long EndingOffset { get; init; }

    /// <summary>
    /// Total records restored so far.
    /// </summary>
    public long TotalRestored { get; set; }
}

/// <summary>
/// No-op implementation of IStateRestoreListener.
/// </summary>
public sealed class NoOpStateRestoreListener : IStateRestoreListener
{
    public static readonly NoOpStateRestoreListener Instance = new();

    public void OnRestoreStart(StateRestoreContext context) { }
    public void OnBatchRestored(StateRestoreContext context, int numRestored) { }
    public void OnRestoreEnd(StateRestoreContext context, long totalRestored) { }
}

/// <summary>
/// Delegate-based implementation of IStateRestoreListener.
/// </summary>
public sealed class DelegateStateRestoreListener : IStateRestoreListener
{
    private readonly Action<StateRestoreContext>? _onStart;
    private readonly Action<StateRestoreContext, int>? _onBatch;
    private readonly Action<StateRestoreContext, long>? _onEnd;

    public DelegateStateRestoreListener(
        Action<StateRestoreContext>? onStart = null,
        Action<StateRestoreContext, int>? onBatch = null,
        Action<StateRestoreContext, long>? onEnd = null)
    {
        _onStart = onStart;
        _onBatch = onBatch;
        _onEnd = onEnd;
    }

    public void OnRestoreStart(StateRestoreContext context) => _onStart?.Invoke(context);
    public void OnBatchRestored(StateRestoreContext context, int numRestored) => _onBatch?.Invoke(context, numRestored);
    public void OnRestoreEnd(StateRestoreContext context, long totalRestored) => _onEnd?.Invoke(context, totalRestored);
}
