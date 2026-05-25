namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A task that reads records from an external system and produces them to Surgewave topics.
/// Implement <see cref="PollAsync"/> to return batches of <see cref="SourceRecord"/> from your data source.
/// </summary>
public abstract class SourceTask : IConnectorTask
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
    /// Poll for new records to produce to Kafka/Surgewave.
    /// Returns null or empty list if no data is currently available.
    /// </summary>
    public abstract Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Called when records have been successfully committed to Kafka/Surgewave.
    /// Override to acknowledge the source system.
    /// </summary>
    public virtual Task CommitAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Called when a specific record has been successfully committed.
    /// </summary>
    public virtual void CommitRecord(SourceRecord record, RecordMetadata metadata)
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
