using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Streams.Sql;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Broker.Sql;

/// <summary>
/// Background service managing SQL query execution and continuous queries.
/// Bridges the LogManager (storage) with the SqlEngine (query execution).
/// </summary>
public sealed class SurgewaveSqlService : IHostedService, IDisposable
{
    private readonly LogManager _logManager;
    private readonly SqlServiceConfig _config;
    private readonly ILogger<SurgewaveSqlService> _logger;
    private readonly ConcurrentDictionary<string, ContinuousQuery> _continuousQueries = new(StringComparer.OrdinalIgnoreCase);
    private int _queryCounter;

    public SurgewaveSqlService(
        LogManager logManager,
        IOptions<SqlServiceConfig> config,
        ILogger<SurgewaveSqlService> logger)
    {
        _logManager = logManager;
        _config = config.Value;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Surgewave SQL Service started (MaxConcurrentQueries={Max})", _config.MaxConcurrentQueries);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        // Cancel all running continuous queries
        foreach (var (_, query) in _continuousQueries)
        {
            query.Cancel();
        }
        _logger.LogInformation("Surgewave SQL Service stopped, cancelled {Count} queries", _continuousQueries.Count);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Execute a one-shot SQL query and return results.
    /// </summary>
    public SqlExecuteResponse ExecuteQuery(string sql)
    {
        try
        {
            var engine = new SqlEngine();

            // Find table references and register each as a topic source
            var tableNames = SqlEngine.ExtractTableNamesFromSql(sql);
            foreach (var tableName in tableNames)
            {
                var topicSource = CreateTopicSource(tableName);
                engine.RegisterTopicSource(tableName, topicSource);
            }

            var result = engine.Execute(sql);

            return new SqlExecuteResponse
            {
                Columns = result.ColumnNames,
                Rows = result.Rows.Select(row =>
                    result.ColumnNames.Select(col =>
                        row.TryGetValue(col, out var val) ? val : null).ToList()).ToList(),
                RowCount = result.Rows.Count
            };
        }
        catch (SqlParseException ex)
        {
            return new SqlExecuteResponse { Error = ex.Message };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SQL execution failed: {Sql}", LogSanitizer.Sanitize(sql));
            return new SqlExecuteResponse { Error = $"Execution failed: {ex.Message}" };
        }
    }

    /// <summary>
    /// Create and start a continuous query.
    /// </summary>
    public ContinuousQueryInfo CreateContinuousQuery(string sql, string name)
    {
        if (_continuousQueries.Count >= _config.MaxConcurrentQueries)
        {
            throw new InvalidOperationException(
                $"Maximum concurrent queries ({_config.MaxConcurrentQueries}) exceeded");
        }

        var queryId = $"sq-{Interlocked.Increment(ref _queryCounter):D4}";
        var cts = new CancellationTokenSource();
        var query = new ContinuousQuery(queryId, name, sql, cts);

        if (!_continuousQueries.TryAdd(queryId, query))
        {
            cts.Dispose();
            throw new InvalidOperationException("Failed to register query");
        }

        // Start the continuous query in the background
        query.Task = Task.Run(async () =>
        {
            try
            {
                query.Status = QueryStatus.Running;
                _logger.LogInformation("Starting continuous query {Id}: {Sql}", LogSanitizer.Sanitize(queryId), LogSanitizer.Sanitize(sql));

                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var engine = new SqlEngine();
                        var tableNames = SqlEngine.ExtractTableNamesFromSql(sql);

                        foreach (var tableName in tableNames)
                        {
                            var topicSource = CreateTopicSource(tableName);
                            engine.RegisterTopicSource(tableName, topicSource);
                        }

                        var result = engine.Execute(sql);
                        query.RowsProcessed += result.Rows.Count;

                        // For CREATE STREAM ... AS SELECT, write results to output topic
                        if (result.IsCreateStatement && result.CreatedName != null)
                        {
                            await WriteResultsToTopic(result.CreatedName, result.Rows, cts.Token);
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogWarning(ex, "Continuous query {Id} iteration failed", queryId);
                    }

                    // Wait before next iteration
                    await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
                }
            }
            catch (OperationCanceledException)
            {
                // Normal cancellation
            }
            finally
            {
                query.Status = QueryStatus.Terminated;
                _logger.LogInformation("Continuous query {Id} terminated", queryId);
            }
        }, cts.Token);

        return query.ToInfo();
    }

    /// <summary>
    /// List all continuous queries.
    /// </summary>
    public IReadOnlyList<ContinuousQueryInfo> ListContinuousQueries()
    {
        return _continuousQueries.Values.Select(q => q.ToInfo()).ToList();
    }

    /// <summary>
    /// Terminate a continuous query by ID.
    /// </summary>
    public bool TerminateQuery(string queryId)
    {
        if (!_continuousQueries.TryRemove(queryId, out var query))
            return false;

        query.Cancel();
        return true;
    }

    private SqlTopicSource CreateTopicSource(string topicName)
    {
        return new SqlTopicSource(() => ReadMessagesFromTopic(topicName), _config.MaxMessagesPerQuery);
    }

    private IEnumerable<RawTopicMessage> ReadMessagesFromTopic(string topicName)
    {
        // Discover partitions by trying common partition IDs
        // In a real scenario, we'd query the log manager for partition count
        var partitions = GetTopicPartitions(topicName);

        foreach (var partition in partitions)
        {
            var tp = new TopicPartition { Topic = topicName, Partition = partition };
            var log = _logManager.GetLog(tp);
            if (log == null) continue;

            var offset = log.LogStartOffset;
            var highWatermark = log.HighWatermark;
            var batchesRead = 0;

            while (offset < highWatermark && batchesRead < 100)
            {
                List<byte[]> batches;
                try
                {
                    batches = _logManager.ReadBatchesAsync(tp, offset, maxBytes: 1024 * 1024)
                        .AsTask().GetAwaiter().GetResult();
                }
                catch
                {
                    break;
                }

                if (batches.Count == 0) break;

                foreach (var batchBytes in batches)
                {
                    var messages = ParseRecordBatch(batchBytes, topicName, partition);
                    foreach (var msg in messages)
                    {
                        yield return msg;
                        offset = msg.Offset + 1;
                    }
                }

                batchesRead++;
            }
        }
    }

    private List<int> GetTopicPartitions(string topicName)
    {
        // Try partitions 0..31 and return the ones that exist
        var partitions = new List<int>();
        for (var i = 0; i < 32; i++)
        {
            var tp = new TopicPartition { Topic = topicName, Partition = i };
            if (_logManager.GetLog(tp) != null)
                partitions.Add(i);
            else if (i > 0 && partitions.Count == 0)
                break; // Stop early if partition 0 doesn't exist
            else if (partitions.Count > 0 && _logManager.GetLog(tp) == null)
                break; // Stop after last existing partition
        }
        return partitions;
    }

    private static List<RawTopicMessage> ParseRecordBatch(byte[] batchBytes, string topic, int partition)
    {
        var messages = new List<RawTopicMessage>();
        var parsed = RecordBatchBrowser.Parse(batchBytes);
        if (parsed.DecompressionFailed)
        {
            return messages; // Undecodable batch — nothing the SQL engine can project.
        }

        foreach (var record in parsed.Records)
        {
            messages.Add(new RawTopicMessage(
                Offset: record.Offset,
                Partition: partition,
                Timestamp: DateTimeOffset.FromUnixTimeMilliseconds(record.TimestampMs),
                Key: record.Key is { Length: > 0 } ? Encoding.UTF8.GetString(record.Key) : null,
                Value: record.Value is { Length: > 0 } ? Encoding.UTF8.GetString(record.Value) : null,
                Headers: record.Headers.Count > 0 ? record.Headers : null));
        }

        return messages;
    }

    private Task WriteResultsToTopic(string topicName, List<Dictionary<string, object?>> rows, CancellationToken ct)
    {
        // Placeholder: in a full implementation this would produce messages to the topic.
        // For now, just log.
        _logger.LogDebug("Would write {Count} rows to topic {Topic}", rows.Count, LogSanitizer.Sanitize(topicName));
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        foreach (var (_, query) in _continuousQueries)
        {
            query.Cancel();
        }
    }
}

/// <summary>
/// Represents a running continuous SQL query.
/// </summary>
internal sealed class ContinuousQuery
{
    public string QueryId { get; }
    public string Name { get; }
    public string Sql { get; }
    public QueryStatus Status { get; set; } = QueryStatus.Created;
    public long RowsProcessed { get; set; }
    public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
    public Task? Task { get; set; }
    private readonly CancellationTokenSource _cts;

    public ContinuousQuery(string queryId, string name, string sql, CancellationTokenSource cts)
    {
        QueryId = queryId;
        Name = name;
        Sql = sql;
        _cts = cts;
    }

    public void Cancel() => _cts.Cancel();

    public ContinuousQueryInfo ToInfo() => new()
    {
        QueryId = QueryId,
        Name = Name,
        Sql = Sql,
        Status = Status.ToString().ToUpperInvariant(),
        RowsProcessed = RowsProcessed,
        CreatedAt = CreatedAt
    };
}

/// <summary>
/// Status of a continuous query.
/// </summary>
public enum QueryStatus
{
    Created,
    Running,
    Paused,
    Terminated
}

/// <summary>
/// Response from executing a SQL query.
/// </summary>
public sealed class SqlExecuteResponse
{
    public List<string>? Columns { get; init; }
    public List<List<object?>>? Rows { get; init; }
    public int RowCount { get; init; }
    public string? Error { get; init; }
}

/// <summary>
/// Information about a continuous query.
/// </summary>
public sealed class ContinuousQueryInfo
{
    public string QueryId { get; init; } = "";
    public string Name { get; init; } = "";
    public string Sql { get; init; } = "";
    public string Status { get; init; } = "";
    public long RowsProcessed { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
}

/// <summary>
/// Request body for creating a continuous query.
/// </summary>
public sealed class CreateQueryRequest
{
    public string Sql { get; init; } = "";
    public string Name { get; init; } = "";
}

/// <summary>
/// Request body for executing a SQL query.
/// </summary>
public sealed class ExecuteSqlRequest
{
    public string Sql { get; init; } = "";
}
