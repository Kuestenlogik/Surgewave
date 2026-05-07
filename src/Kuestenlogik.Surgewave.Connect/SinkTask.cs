namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A task that consumes records from Surgewave topics and writes them to an external system.
/// Implement <see cref="PutAsync"/> to write batches of <see cref="SinkRecord"/> to your data store.
/// </summary>
public abstract class SinkTask : IConnectorTask
{
    /// <summary>Gets the task context provided during initialization.</summary>
    protected TaskContext Context { get; private set; } = null!;
    private bool _disposed;

    /// <inheritdoc />
    public abstract string Version { get; }

    /// <inheritdoc />
    public virtual void Initialize(TaskContext context)
    {
        Context = context;
    }

    /// <inheritdoc />
    public abstract void Start(IDictionary<string, string> config);

    /// <inheritdoc />
    public abstract void Stop();

    /// <summary>
    /// Put the records to the destination system.
    /// </summary>
    public abstract Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken);

    /// <summary>
    /// Called periodically to flush any buffered records to the destination.
    /// </summary>
    public virtual Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Called when the task should open a new set of partitions.
    /// </summary>
    public virtual void Open(IReadOnlyCollection<TopicPartition> partitions)
    {
    }

    /// <summary>
    /// Called when the task should close the given partitions.
    /// </summary>
    public virtual void Close(IReadOnlyCollection<TopicPartition> partitions)
    {
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            _disposed = true;
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}
