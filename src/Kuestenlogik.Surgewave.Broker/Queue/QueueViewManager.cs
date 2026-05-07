using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Queue;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Queue;

/// <summary>
/// Registry of <see cref="QueueView"/> instances, one per enrolled topic.
/// Manages creation, retrieval, and removal of QueueView instances.
/// Thread-safe; safe to call from concurrent request handlers.
/// </summary>
public sealed class QueueViewManager : IQueueViewManager, IAsyncDisposable
{
    private readonly QueueViewConfig _config;
    private readonly LogManager? _logManager;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ConcurrentDictionary<string, QueueView> _views = new(StringComparer.Ordinal);
    private bool _disposed;

    /// <summary>
    /// Initialises the manager with shared configuration and optional LogManager for DLQ support.
    /// </summary>
    public QueueViewManager(
        QueueViewConfig config,
        ILoggerFactory loggerFactory,
        LogManager? logManager = null)
    {
        _config = config;
        _loggerFactory = loggerFactory;
        _logManager = logManager;
    }

    /// <summary>
    /// Returns the list of topic names that currently have an active QueueView.
    /// </summary>
    public IReadOnlyList<string> ActiveTopics => [.. _views.Keys];

    /// <summary>
    /// Returns the total number of in-flight messages across all active QueueViews.
    /// </summary>
    public int TotalInFlightCount => _views.Values.Sum(v => v.InFlightCount);

    /// <summary>
    /// Returns an existing QueueView for <paramref name="topic"/>, or creates a new one
    /// backed by the supplied <paramref name="log"/>.
    /// </summary>
    public IQueueView GetOrCreate(string topic, IPartitionLog log)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return _views.GetOrAdd(topic, _ =>
        {
            var logger = _loggerFactory.CreateLogger<QueueView>();
            return new QueueView(log, _config, logger, _logManager);
        });
    }

    /// <summary>
    /// Returns the QueueView for <paramref name="topic"/>, or <c>null</c> if none exists.
    /// </summary>
    public IQueueView? Get(string topic) =>
        _views.TryGetValue(topic, out var view) ? view : null;

    /// <summary>
    /// Removes the QueueView for <paramref name="topic"/> and disposes it asynchronously.
    /// </summary>
    public async ValueTask RemoveAsync(string topic)
    {
        if (_views.TryRemove(topic, out var view))
            await view.DisposeAsync();
    }

    /// <summary>
    /// Removes the QueueView for <paramref name="topic"/> (fire-and-forget convenience wrapper).
    /// The underlying dispose runs as a background task.
    /// </summary>
    public void Remove(string topic) => _ = RemoveAsync(topic).AsTask();

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var view in _views.Values)
            await view.DisposeAsync();

        _views.Clear();
    }
}
