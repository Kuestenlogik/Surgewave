using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.PostgreSql.Protocol;
using Kuestenlogik.Surgewave.Streams.Sql;
using Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.PostgreSql;

/// <summary>
/// Executes SQL queries by bridging PostgreSQL client requests to Surgewave's
/// <see cref="SqlEngine"/> and <see cref="LogManager"/>. Handles:
/// <list type="bullet">
///   <item>SELECT queries against Surgewave topics via SqlEngine</item>
///   <item>SET commands (responds with CommandComplete)</item>
///   <item>SHOW commands (returns parameter values)</item>
///   <item>pg_catalog queries (returns empty stub results so psql doesn't crash)</item>
///   <item>BEGIN/COMMIT/ROLLBACK (no-ops, Surgewave has no transactions)</item>
/// </list>
/// </summary>
internal sealed class QueryExecutor
{
    private readonly LogManager _logManager;
    private readonly string _serverVersion;
    private readonly ILogger _logger;
    private readonly MaterializedViewRegistry? _viewRegistry;

    // Common psql-on-connect patterns that need special handling
    private static readonly Regex SetRegex = new(
        @"^\s*SET\s+", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex ShowRegex = new(
        @"^\s*SHOW\s+(\S+)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex PgCatalogRegex = new(
        @"pg_catalog\.|pg_type|pg_namespace|pg_class|pg_attribute|pg_settings|information_schema\.",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex BeginRegex = new(
        @"^\s*(BEGIN|START\s+TRANSACTION)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex CommitRegex = new(
        @"^\s*(COMMIT|END)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex RollbackRegex = new(
        @"^\s*ROLLBACK", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex DeallocateRegex = new(
        @"^\s*DEALLOCATE\s+", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex DiscardRegex = new(
        @"^\s*DISCARD\s+", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex ResetRegex = new(
        @"^\s*RESET\s+", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex CloseRegex = new(
        @"^\s*CLOSE\s+", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex ListenRegex = new(
        @"^\s*LISTEN\s+", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex UnlistenRegex = new(
        @"^\s*UNLISTEN\s+", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    public QueryExecutor(
        LogManager logManager,
        string serverVersion,
        ILogger logger,
        MaterializedViewRegistry? viewRegistry = null)
    {
        _logManager = logManager;
        _serverVersion = serverVersion;
        _logger = logger;
        _viewRegistry = viewRegistry;
    }

    /// <summary>
    /// Executes a SQL query and returns the result.
    /// </summary>
    public QueryResult Execute(string sql)
    {
        sql = sql.Trim().TrimEnd(';');

        if (string.IsNullOrWhiteSpace(sql))
            return QueryResult.Empty();

        // SET commands — just acknowledge
        if (SetRegex.IsMatch(sql))
            return QueryResult.Command("SET");

        // SHOW commands — return the parameter value
        var showMatch = ShowRegex.Match(sql);
        if (showMatch.Success)
            return HandleShow(showMatch.Groups[1].Value);

        // Transaction commands — no-op
        if (BeginRegex.IsMatch(sql))
            return QueryResult.Command("BEGIN");
        if (CommitRegex.IsMatch(sql))
            return QueryResult.Command("COMMIT");
        if (RollbackRegex.IsMatch(sql))
            return QueryResult.Command("ROLLBACK");

        // DEALLOCATE, DISCARD, RESET, CLOSE, LISTEN, UNLISTEN — no-op
        if (DeallocateRegex.IsMatch(sql))
            return QueryResult.Command("DEALLOCATE");
        if (DiscardRegex.IsMatch(sql))
            return QueryResult.Command("DISCARD ALL");
        if (ResetRegex.IsMatch(sql))
            return QueryResult.Command("RESET");
        if (CloseRegex.IsMatch(sql))
            return QueryResult.Command("CLOSE CURSOR");
        if (ListenRegex.IsMatch(sql))
            return QueryResult.Command("LISTEN");
        if (UnlistenRegex.IsMatch(sql))
            return QueryResult.Command("UNLISTEN");

        // pg_catalog / information_schema queries — return empty result set
        if (PgCatalogRegex.IsMatch(sql))
            return HandleCatalogQuery(sql);

        // Surgewave SQL via SqlEngine
        return ExecuteSurgewaveSql(sql);
    }

    private QueryResult HandleShow(string parameter)
    {
        var value = parameter.ToLowerInvariant() switch
        {
            "server_version" => _serverVersion,
            "server_encoding" => "UTF8",
            "client_encoding" => "UTF8",
            "is_superuser" => "on",
            "session_authorization" => "surgewave",
            "standard_conforming_strings" => "on",
            "datestyle" => "ISO, MDY",
            "timezone" => "UTC",
            "integer_datetimes" => "on",
            "intervalstyle" => "postgres",
            "transaction_isolation" => "read committed",
            "search_path" => "\"$user\", public",
            "lc_messages" or "lc_monetary" or "lc_numeric" or "lc_time" => "en_US.UTF-8",
            "default_transaction_read_only" => "off",
            "max_identifier_length" => "63",
            _ => "unknown",
        };

        return new QueryResult(
            Columns: [new PgColumnDescriptor(parameter, PgTypeOids.Text)],
            Rows: [[value]],
            CommandTag: "SHOW");
    }

    private static QueryResult HandleCatalogQuery(string sql)
    {
        // For pg_catalog queries, infer column names from SELECT list if possible.
        // Otherwise return an empty result set with no columns.
        // This satisfies psql's startup probing without breaking.
        var columns = InferColumnsFromSelect(sql);
        return new QueryResult(
            Columns: columns,
            Rows: [],
            CommandTag: "SELECT 0");
    }

    private QueryResult ExecuteSurgewaveSql(string sql)
    {
        try
        {
            var engine = new SqlEngine(_viewRegistry);

            var tableNames = SqlEngine.ExtractTableNamesFromSql(sql);
            foreach (var tableName in tableNames)
            {
                // A registered materialized view shadows topic discovery — do not
                // wire a topic source for it (the engine resolves it from the registry).
                if (_viewRegistry is not null && _viewRegistry.Contains(tableName))
                    continue;

                var source = CreateTopicSource(tableName);
                engine.RegisterTopicSource(tableName, source);
            }

            var result = engine.Execute(sql);

            // CREATE / DROP MATERIALIZED VIEW return no rows but a distinct command tag
            if (result.IsCreateStatement && result.Rows.Count == 0)
            {
                var tag = sql.TrimStart().StartsWith("DROP", StringComparison.OrdinalIgnoreCase)
                    ? "DROP MATERIALIZED VIEW"
                    : "CREATE MATERIALIZED VIEW";
                return QueryResult.Command(tag);
            }

            // Build column descriptors with type OIDs inferred from data
            var columns = new List<PgColumnDescriptor>(result.ColumnNames.Count);
            foreach (var colName in result.ColumnNames)
            {
                var typeOid = PgTypeOids.Text;
                // Infer type from first non-null row value
                foreach (var row in result.Rows)
                {
                    if (row.TryGetValue(colName, out var val) && val is not null)
                    {
                        typeOid = PgTypeOids.FromClrValue(val);
                        break;
                    }
                }
                columns.Add(new PgColumnDescriptor(colName, typeOid));
            }

            // Convert rows to text format
            var rows = new List<string?[]>(result.Rows.Count);
            foreach (var row in result.Rows)
            {
                var textRow = new string?[result.ColumnNames.Count];
                for (var i = 0; i < result.ColumnNames.Count; i++)
                {
                    var colName = result.ColumnNames[i];
                    textRow[i] = row.TryGetValue(colName, out var val) ? FormatValue(val) : null;
                }
                rows.Add(textRow);
            }

            return new QueryResult(
                Columns: columns,
                Rows: rows,
                CommandTag: $"SELECT {rows.Count}");
        }
        catch (SqlParseException ex)
        {
            return QueryResult.Error("ERROR", "42601", ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SQL execution failed: {Sql}", sql);
            return QueryResult.Error("ERROR", "XX000", $"Execution failed: {ex.Message}");
        }
    }

    private SqlTopicSource CreateTopicSource(string topicName)
    {
        return new SqlTopicSource(() => ReadTopicMessages(topicName));
    }

    private IEnumerable<RawTopicMessage> ReadTopicMessages(string topicName)
    {
        // Discover partitions by trying IDs 0..31 (same approach as SurgewaveSqlService)
        var partitions = GetTopicPartitions(topicName);

        foreach (var partition in partitions)
        {
            var tp = new TopicPartition { Topic = topicName, Partition = partition };
            var log = _logManager.GetLog(tp);
            if (log is null) continue;

            var offset = log.LogStartOffset;
            var highWatermark = log.HighWatermark;
            var batchesRead = 0;

            while (offset < highWatermark && batchesRead < 100)
            {
                List<byte[]> batches;
                try
                {
                    batches = _logManager.ReadBatchesAsync(tp, offset, maxBytes: 1_048_576)
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
        var partitions = new List<int>();
        for (var i = 0; i < 32; i++)
        {
            var tp = new TopicPartition { Topic = topicName, Partition = i };
            if (_logManager.GetLog(tp) != null)
                partitions.Add(i);
            else if (i > 0 && partitions.Count == 0)
                break;
            else if (partitions.Count > 0 && _logManager.GetLog(tp) == null)
                break;
        }
        return partitions;
    }

    private static List<RawTopicMessage> ParseRecordBatch(byte[] batchBytes, string topic, int partition)
    {
        var messages = new List<RawTopicMessage>();

        try
        {
            var span = batchBytes.AsSpan();
            if (span.Length < 61) return messages;

            var baseOffset = BinaryPrimitives.ReadInt64BigEndian(span);
            var attributes = BinaryPrimitives.ReadInt16BigEndian(span[21..]);
            var firstTimestamp = BinaryPrimitives.ReadInt64BigEndian(span[27..]);
            var recordCount = BinaryPrimitives.ReadInt32BigEndian(span[57..]);

            var compression = attributes & 0x07;
            if (compression != 0) return messages;

            var pos = 61;
            for (var i = 0; i < recordCount && pos < span.Length; i++)
            {
                try
                {
                    var recordLength = ReadVarInt(span, ref pos);
                    if (recordLength <= 0 || pos + recordLength > span.Length) break;

                    ReadVarInt(span, ref pos); // attributes
                    var timestampDelta = ReadVarLong(span, ref pos);
                    var offsetDelta = ReadVarInt(span, ref pos);

                    var keyLength = ReadVarInt(span, ref pos);
                    string? key = null;
                    if (keyLength > 0 && pos + keyLength <= span.Length)
                    {
                        key = Encoding.UTF8.GetString(span.Slice(pos, keyLength));
                        pos += keyLength;
                    }
                    else if (keyLength > 0) break;

                    var valueLength = ReadVarInt(span, ref pos);
                    string? value = null;
                    if (valueLength > 0 && pos + valueLength <= span.Length)
                    {
                        value = Encoding.UTF8.GetString(span.Slice(pos, valueLength));
                        pos += valueLength;
                    }
                    else if (valueLength > 0) break;

                    var headerCount = ReadVarInt(span, ref pos);
                    var headers = new Dictionary<string, string>();
                    for (var h = 0; h < headerCount && pos < span.Length; h++)
                    {
                        var hkLen = ReadVarInt(span, ref pos);
                        if (hkLen > 0 && pos + hkLen <= span.Length)
                        {
                            var hk = Encoding.UTF8.GetString(span.Slice(pos, hkLen));
                            pos += hkLen;
                            var hvLen = ReadVarInt(span, ref pos);
                            if (hvLen > 0 && pos + hvLen <= span.Length)
                            {
                                headers[hk] = Encoding.UTF8.GetString(span.Slice(pos, hvLen));
                                pos += hvLen;
                            }
                        }
                    }

                    messages.Add(new RawTopicMessage(
                        Offset: baseOffset + offsetDelta,
                        Partition: partition,
                        Timestamp: DateTimeOffset.FromUnixTimeMilliseconds(firstTimestamp + timestampDelta),
                        Key: key,
                        Value: value,
                        Headers: headers.Count > 0 ? headers : null));
                }
                catch
                {
                    break;
                }
            }
        }
        catch
        {
            // Failed to parse batch
        }

        return messages;
    }

    private static int ReadVarInt(ReadOnlySpan<byte> span, ref int pos)
    {
        var result = 0;
        var shift = 0;
        while (pos < span.Length)
        {
            var b = span[pos++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return (result >> 1) ^ -(result & 1);
            shift += 7;
            if (shift > 28) break;
        }
        return result;
    }

    private static long ReadVarLong(ReadOnlySpan<byte> span, ref int pos)
    {
        long result = 0;
        var shift = 0;
        while (pos < span.Length)
        {
            var b = span[pos++];
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return (result >> 1) ^ -(result & 1);
            shift += 7;
            if (shift > 63) break;
        }
        return result;
    }

    private static string? FormatValue(object? value) => value switch
    {
        null => null,
        bool b => b ? "t" : "f",
        DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture),
        DateTimeOffset dto => dto.ToString("yyyy-MM-dd HH:mm:ss.ffffffzzz", CultureInfo.InvariantCulture),
        DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        double d => d.ToString("G", CultureInfo.InvariantCulture),
        float f => f.ToString("G", CultureInfo.InvariantCulture),
        decimal m => m.ToString("G", CultureInfo.InvariantCulture),
        byte[] bytes => $"\\x{Convert.ToHexString(bytes).ToLowerInvariant()}",
        _ => Convert.ToString(value, CultureInfo.InvariantCulture),
    };

    /// <summary>
    /// Attempts to infer column names from a SELECT clause for catalog stub results.
    /// Returns at least one text column so the result set is valid.
    /// </summary>
    private static List<PgColumnDescriptor> InferColumnsFromSelect(string sql)
    {
        // Quick extraction: look for SELECT ... FROM
        var match = Regex.Match(sql, @"SELECT\s+(.+?)\s+FROM\s+", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success)
            return [new PgColumnDescriptor("result", PgTypeOids.Text)];

        var selectList = match.Groups[1].Value;
        var columns = new List<PgColumnDescriptor>();

        foreach (var item in selectList.Split(','))
        {
            var trimmed = item.Trim();
            if (string.IsNullOrEmpty(trimmed) || trimmed == "*")
            {
                columns.Add(new PgColumnDescriptor("column", PgTypeOids.Text));
                continue;
            }

            // Handle "expr AS alias" — use the alias
            var asMatch = Regex.Match(trimmed, @"\bAS\s+""?(\w+)""?\s*$", RegexOptions.IgnoreCase);
            if (asMatch.Success)
            {
                columns.Add(new PgColumnDescriptor(asMatch.Groups[1].Value, PgTypeOids.Text));
                continue;
            }

            // Handle "table.column" — use column part
            var dotIndex = trimmed.LastIndexOf('.');
            var name = dotIndex >= 0 ? trimmed[(dotIndex + 1)..] : trimmed;

            // Strip function calls, parens, etc. — take last identifier
            var identMatch = Regex.Match(name, @"(\w+)\s*$");
            if (identMatch.Success)
                columns.Add(new PgColumnDescriptor(identMatch.Groups[1].Value, PgTypeOids.Text));
            else
                columns.Add(new PgColumnDescriptor("column", PgTypeOids.Text));
        }

        return columns.Count > 0 ? columns : [new PgColumnDescriptor("result", PgTypeOids.Text)];
    }
}

/// <summary>
/// Represents the result of executing a query through the PostgreSQL adapter.
/// </summary>
internal sealed record QueryResult(
    List<PgColumnDescriptor> Columns,
    List<string?[]> Rows,
    string CommandTag)
{
    /// <summary>Error details, non-null when the query failed.</summary>
    public QueryError? ErrorInfo { get; init; }

    public bool IsError => ErrorInfo is not null;

    public static QueryResult Empty()
        => new([], [], "SELECT 0") { ErrorInfo = null };

    public static QueryResult Command(string tag)
        => new([], [], tag);

    public static QueryResult Error(string severity, string code, string message)
        => new([], [], "") { ErrorInfo = new QueryError(severity, code, message) };
}

/// <summary>
/// Error information for a failed query.
/// </summary>
internal sealed record QueryError(string Severity, string Code, string Message);
