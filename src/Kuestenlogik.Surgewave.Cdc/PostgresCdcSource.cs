using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Kuestenlogik.Surgewave.Cdc;

/// <summary>
/// CDC source for PostgreSQL using logical replication with the pgoutput plugin.
/// Creates a replication slot and reads WAL changes, converting them to <see cref="CdcEvent"/> instances.
/// </summary>
public sealed class PostgresCdcSource : ICdcSource
{
    private readonly CdcConfig _config;
    private readonly ILogger<PostgresCdcSource> _logger;
    private long _lastConfirmedLsn;

    // Pre-built SQL command texts (from trusted config, not user input)
    // These use identifiers from CdcConfig which is set by the operator, not end-user requests.
    private readonly string _slotCheckSql;
    private readonly string _slotCreateSql;
    private readonly string _pubCheckSql;
    private readonly string _pubCreateSql;
    private readonly string _changesSql;

    /// <inheritdoc />
    public string DatabaseType => "PostgreSQL";

    /// <summary>
    /// Gets the last confirmed LSN position.
    /// </summary>
    public long LastConfirmedLsn => _lastConfirmedLsn;

    /// <summary>
    /// Total number of events captured since this source was created.
    /// </summary>
    public long EventsCaptured { get; private set; }

    /// <summary>
    /// Initializes a new instance of <see cref="PostgresCdcSource"/>.
    /// </summary>
    /// <param name="config">CDC configuration.</param>
    /// <param name="logger">Logger instance.</param>
    public PostgresCdcSource(CdcConfig config, ILogger<PostgresCdcSource> logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Pre-build SQL strings from trusted configuration (operator-supplied, not user-supplied).
        // These identifiers are validated/escaped at construction time.
        var escapedSlot = EscapeIdentifier(config.SlotName);
        var escapedPub = EscapeIdentifier(config.PublicationName);

        _slotCheckSql = $"SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '{escapedSlot}'";
        _slotCreateSql = $"SELECT pg_create_logical_replication_slot('{escapedSlot}', 'pgoutput')";
        _pubCheckSql = $"SELECT COUNT(*) FROM pg_publication WHERE pubname = '{escapedPub}'";
        _changesSql = $"SELECT lsn, xid, data FROM pg_logical_slot_get_changes('{escapedSlot}', NULL, NULL, 'proto_version', '1', 'publication_names', '{escapedPub}')";

        var tableClause = config.Tables.Count > 0
            ? $"FOR TABLE {string.Join(", ", config.Tables.Select(EscapeIdentifier))}"
            : "FOR ALL TABLES";
        _pubCreateSql = $"CREATE PUBLICATION {escapedPub} {tableClause}";
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<CdcEvent> CaptureChangesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        _logger.LogInformation(
            "Starting PostgreSQL CDC capture (slot={Slot}, publication={Publication})",
            _config.SlotName, _config.PublicationName);

        await EnsureReplicationSlotAsync(ct);
        await EnsurePublicationAsync(ct);

        // Use a regular connection to poll for changes via pg_logical_slot_get_changes
        // This approach avoids the complexity of the streaming replication protocol
        // while still providing reliable change capture.
        await using var connection = new NpgsqlConnection(_config.ConnectionString);
        await connection.OpenAsync(ct);

        _logger.LogInformation("PostgreSQL CDC source connected and streaming");

        while (!ct.IsCancellationRequested)
        {
            var hasChanges = false;

#pragma warning disable CA2100 // SQL built from trusted operator config at construction time, not user input
            await using var cmd = new NpgsqlCommand(_changesSql, connection);
#pragma warning restore CA2100

            await using var reader = await cmd.ExecuteReaderAsync(ct);

            while (await reader.ReadAsync(ct))
            {
                hasChanges = true;
                var lsnString = reader.GetString(0);
                var data = reader.GetString(2);

                var lsn = ParseLsn(lsnString);

                // Parse the pgoutput textual representation
                var events = ParsePgOutputData(data, lsn);
                foreach (var evt in events)
                {
                    EventsCaptured++;
                    yield return evt;
                }

                _lastConfirmedLsn = lsn;
            }

            if (!hasChanges)
            {
                // No changes available, wait before polling again
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100), ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        _logger.LogInformation("PostgreSQL CDC capture stopped (lastLsn={Lsn})", _lastConfirmedLsn);
    }

    private async Task EnsureReplicationSlotAsync(CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(_config.ConnectionString);
        await connection.OpenAsync(ct);

#pragma warning disable CA2100 // SQL built from trusted operator config at construction time, not user input
        await using var checkCmd = new NpgsqlCommand(_slotCheckSql, connection);
#pragma warning restore CA2100

        var count = (long)(await checkCmd.ExecuteScalarAsync(ct))!;
        if (count == 0)
        {
            _logger.LogInformation("Creating replication slot: {SlotName}", _config.SlotName);
#pragma warning disable CA2100
            await using var createCmd = new NpgsqlCommand(_slotCreateSql, connection);
#pragma warning restore CA2100
            await createCmd.ExecuteNonQueryAsync(ct);
        }
        else
        {
            _logger.LogDebug("Replication slot already exists: {SlotName}", _config.SlotName);
        }
    }

    private async Task EnsurePublicationAsync(CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(_config.ConnectionString);
        await connection.OpenAsync(ct);

#pragma warning disable CA2100 // SQL built from trusted operator config at construction time, not user input
        await using var checkCmd = new NpgsqlCommand(_pubCheckSql, connection);
#pragma warning restore CA2100

        var count = (long)(await checkCmd.ExecuteScalarAsync(ct))!;
        if (count == 0)
        {
            _logger.LogInformation("Creating publication: {Publication}", _config.PublicationName);

#pragma warning disable CA2100
            await using var createCmd = new NpgsqlCommand(_pubCreateSql, connection);
#pragma warning restore CA2100
            await createCmd.ExecuteNonQueryAsync(ct);
        }
        else
        {
            _logger.LogDebug("Publication already exists: {Publication}", _config.PublicationName);
        }
    }

    private List<CdcEvent> ParsePgOutputData(string data, long lsn)
    {
        var events = new List<CdcEvent>();

        try
        {
            // pgoutput text format: "table schema.tablename: OPERATION: col1[type]:value1 col2[type]:value2"
            // This is a simplified parser for the textual representation
            if (string.IsNullOrWhiteSpace(data))
                return events;

            var lines = data.Split('\n', StringSplitOptions.RemoveEmptyEntries);
            foreach (var line in lines)
            {
                var evt = ParseSingleChange(line.Trim(), lsn);
                if (evt is not null)
                {
                    events.Add(evt);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to parse pgoutput data: {Data}", data);
        }

        return events;
    }

    private static CdcEvent? ParseSingleChange(string line, long lsn)
    {
        // Format: "table public.orders: INSERT: id[integer]:1 name[text]:'test'"
        // or:     "table public.orders: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:1 name[text]:'updated'"
        // or:     "table public.orders: DELETE: id[integer]:1"

        const string tablePrefix = "table ";
        if (!line.StartsWith(tablePrefix, StringComparison.Ordinal))
            return null;

        var colonIndex = line.IndexOf(':', tablePrefix.Length);
        if (colonIndex < 0)
            return null;

        var fullTableName = line[tablePrefix.Length..colonIndex].Trim();
        var schemaAndTable = fullTableName.Split('.');
        var schema = schemaAndTable.Length > 1 ? schemaAndTable[0] : "public";
        var table = schemaAndTable.Length > 1 ? schemaAndTable[1] : schemaAndTable[0];

        var rest = line[(colonIndex + 1)..].Trim();

        CdcOperation operation;
        Dictionary<string, object?>? before = null;
        Dictionary<string, object?>? after = null;

        if (rest.StartsWith("INSERT:", StringComparison.OrdinalIgnoreCase))
        {
            operation = CdcOperation.Insert;
            after = ParseColumns(rest["INSERT:".Length..].Trim());
        }
        else if (rest.StartsWith("UPDATE:", StringComparison.OrdinalIgnoreCase))
        {
            operation = CdcOperation.Update;
            var updateData = rest["UPDATE:".Length..].Trim();

            var oldKeyIndex = updateData.IndexOf("old-key:", StringComparison.OrdinalIgnoreCase);
            var newTupleIndex = updateData.IndexOf("new-tuple:", StringComparison.OrdinalIgnoreCase);

            if (oldKeyIndex >= 0 && newTupleIndex >= 0)
            {
                before = ParseColumns(updateData[(oldKeyIndex + "old-key:".Length)..newTupleIndex].Trim());
                after = ParseColumns(updateData[(newTupleIndex + "new-tuple:".Length)..].Trim());
            }
            else
            {
                after = ParseColumns(updateData);
            }
        }
        else if (rest.StartsWith("DELETE:", StringComparison.OrdinalIgnoreCase))
        {
            operation = CdcOperation.Delete;
            before = ParseColumns(rest["DELETE:".Length..].Trim());
        }
        else
        {
            return null;
        }

        return new CdcEvent
        {
            Operation = operation,
            Schema = schema,
            Table = table,
            Before = before,
            After = after,
            Timestamp = DateTimeOffset.UtcNow,
            Lsn = lsn
        };
    }

    private static Dictionary<string, object?> ParseColumns(string columnsStr)
    {
        var result = new Dictionary<string, object?>();
        if (string.IsNullOrWhiteSpace(columnsStr))
            return result;

        // Simple parser: "col1[type]:value1 col2[type]:value2"
        var i = 0;
        while (i < columnsStr.Length)
        {
            // Find column name (up to '[')
            var bracketStart = columnsStr.IndexOf('[', i);
            if (bracketStart < 0) break;

            var columnName = columnsStr[i..bracketStart].Trim();

            // Find type (up to ']')
            var bracketEnd = columnsStr.IndexOf(']', bracketStart);
            if (bracketEnd < 0) break;

            // Find value (after ':')
            var valueStart = bracketEnd + 1;
            if (valueStart >= columnsStr.Length || columnsStr[valueStart] != ':')
                break;

            valueStart++; // skip ':'

            // Find end of value (next space followed by a column name, or end of string)
            string value;
            var nextCol = FindNextColumn(columnsStr, valueStart);
            if (nextCol >= 0)
            {
                value = columnsStr[valueStart..nextCol].Trim();
                i = nextCol;
            }
            else
            {
                value = columnsStr[valueStart..].Trim();
                i = columnsStr.Length;
            }

            // Parse the value
            if (value == "null")
            {
                result[columnName] = null;
            }
            else if (value.StartsWith('\'') && value.EndsWith('\'') && value.Length >= 2)
            {
                result[columnName] = value[1..^1].Replace("''", "'");
            }
            else if (long.TryParse(value, out var longVal))
            {
                result[columnName] = longVal;
            }
            else if (double.TryParse(value, System.Globalization.CultureInfo.InvariantCulture, out var doubleVal))
            {
                result[columnName] = doubleVal;
            }
            else if (bool.TryParse(value, out var boolVal))
            {
                result[columnName] = boolVal;
            }
            else
            {
                result[columnName] = value;
            }
        }

        return result;
    }

    private static int FindNextColumn(string str, int startIndex)
    {
        // Look for a pattern like " colname[" which indicates the start of the next column
        for (var i = startIndex; i < str.Length - 1; i++)
        {
            if (str[i] == ' ')
            {
                // Check if the rest looks like "colname[type]"
                var bracketPos = str.IndexOf('[', i + 1);
                if (bracketPos > i + 1)
                {
                    // Verify there's no space between i+1 and bracketPos (it's a column name)
                    var segment = str[(i + 1)..bracketPos];
                    if (!segment.Contains(' '))
                    {
                        return i + 1;
                    }
                }
            }
        }

        return -1;
    }

    private static long ParseLsn(string lsnString)
    {
        // LSN format: "16/B374D848" -- two hex segments separated by '/'
        var parts = lsnString.Split('/');
        if (parts.Length == 2 &&
            long.TryParse(parts[0], System.Globalization.NumberStyles.HexNumber, null, out var high) &&
            long.TryParse(parts[1], System.Globalization.NumberStyles.HexNumber, null, out var low))
        {
            return (high << 32) | low;
        }

        return 0;
    }

    private static string EscapeIdentifier(string identifier)
    {
        // Prevent SQL injection in identifiers
        return identifier.Replace("'", "''").Replace("\"", "\"\"");
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _logger.LogDebug("PostgresCdcSource disposed");
        await ValueTask.CompletedTask;
    }
}
