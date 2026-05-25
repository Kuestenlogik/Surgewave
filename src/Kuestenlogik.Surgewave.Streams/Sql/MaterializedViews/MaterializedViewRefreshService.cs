using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;

/// <summary>
/// Background service that periodically refreshes every registered
/// materialized view.
///
/// MVP strategy: each refresh cycle re-reads the full source topic(s),
/// re-executes the SELECT body of the view via <see cref="SqlEngine"/>,
/// and atomically publishes the resulting snapshot to <see cref="MaterializedView"/>.
///
/// This is correct but O(N) per refresh cycle. Phase 2 will replace it
/// with incremental aggregation that only consumes new offsets since the
/// last cycle.
/// </summary>
public sealed class MaterializedViewRefreshService : BackgroundService
{
    private readonly MaterializedViewRegistry _registry;
    private readonly IRawTopicReader _topicReader;
    private readonly ILogger<MaterializedViewRefreshService> _logger;
    private readonly MaterializedViewOptions _options;

    public MaterializedViewRefreshService(
        MaterializedViewRegistry registry,
        IRawTopicReader topicReader,
        IOptions<MaterializedViewOptions> options,
        ILogger<MaterializedViewRefreshService> logger)
    {
        _registry = registry;
        _topicReader = topicReader;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Materialized view refresh service disabled (Surgewave:Streams:MaterializedViews:Enabled=false)");
            return;
        }

        _logger.LogInformation(
            "Materialized view refresh service started (interval={Interval})",
            _options.RefreshInterval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                RefreshAll();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Materialized view refresh cycle failed");
            }

            try
            {
                await Task.Delay(_options.RefreshInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Refreshes every registered view exactly once. Public so tests can drive it
    /// deterministically without waiting for the polling timer.
    /// </summary>
    public void RefreshAll()
    {
        foreach (var view in _registry.All)
        {
            try
            {
                RefreshView(view);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to refresh materialized view '{ViewName}'", view.Definition.Name);
            }
        }
    }

    private void RefreshView(MaterializedView view)
    {
        var def = view.Definition;

        // Build a fresh SqlEngine and bind each source topic to a SqlTopicSource
        // backed by the IRawTopicReader.
        var engine = new SqlEngine();
        foreach (var topic in def.SourceTopics)
        {
            var topicCopy = topic; // capture
            engine.RegisterTopicSource(topic, new SqlTopicSource(() => _topicReader.ReadTopic(topicCopy)));
        }

        var result = engine.Execute(def.SelectSql);
        view.PublishSnapshot(result.Rows, result.ColumnNames);

        _logger.LogTrace(
            "Refreshed view '{Name}': {RowCount} rows, refresh #{Count}",
            def.Name, result.Rows.Count, view.Snapshot.RefreshCount);
    }
}
