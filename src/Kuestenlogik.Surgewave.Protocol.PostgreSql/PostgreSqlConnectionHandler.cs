using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Protocol.PostgreSql.Protocol;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.PostgreSql;

/// <summary>
/// Handles a single PostgreSQL client connection. Manages the startup handshake,
/// authentication, and dispatches query messages to the <see cref="QueryExecutor"/>.
/// Supports both the simple query protocol and the extended query protocol.
/// </summary>
internal sealed class PostgreSqlConnectionHandler
{
    private static int _processIdCounter;

    private readonly Stream _stream;
    private readonly PgReader _reader;
    private readonly PgWriter _writer;
    private readonly QueryExecutor _queryExecutor;
    private readonly PostgreSqlConfig _config;
    private readonly ILogger _logger;

    private readonly int _processId;
    private readonly int _secretKey;
    private readonly Dictionary<string, string> _clientParameters = new(StringComparer.OrdinalIgnoreCase);

    // Extended query state: named prepared statements and portals
    private readonly ConcurrentDictionary<string, PreparedStatement> _preparedStatements = new();
    private readonly ConcurrentDictionary<string, PreparedStatement> _portals = new();

    public PostgreSqlConnectionHandler(
        Stream stream,
        QueryExecutor queryExecutor,
        PostgreSqlConfig config,
        ILogger logger)
    {
        _stream = stream;
        _reader = new PgReader(stream);
        _writer = new PgWriter(stream);
        _queryExecutor = queryExecutor;
        _config = config;
        _logger = logger;

        _processId = Interlocked.Increment(ref _processIdCounter);
        _secretKey = Random.Shared.Next();
    }

    /// <summary>
    /// Runs the full connection lifecycle: startup, authentication, query loop.
    /// </summary>
    public async Task HandleAsync(CancellationToken ct)
    {
        if (!await HandleStartupAsync(ct).ConfigureAwait(false))
            return;

        await RunQueryLoopAsync(ct).ConfigureAwait(false);
    }

    private async Task<bool> HandleStartupAsync(CancellationToken ct)
    {
        var startup = await _reader.ReadStartupMessageAsync(ct).ConfigureAwait(false);
        if (startup is null)
            return false;

        var (protocolVersion, parameters) = startup.Value;

        // Handle SSL request — respond with 'N' (not supported) and read actual startup
        if (protocolVersion == 80877103) // SSL request
        {
            await _stream.WriteAsync(new byte[] { (byte)'N' }, ct).ConfigureAwait(false);
            await _stream.FlushAsync(ct).ConfigureAwait(false);

            startup = await _reader.ReadStartupMessageAsync(ct).ConfigureAwait(false);
            if (startup is null)
                return false;
            (protocolVersion, parameters) = startup.Value;
        }

        // Handle cancel request — we don't support cancellation, just close
        if (protocolVersion == 80877102)
            return false;

        // Expect protocol version 3.0
        if (protocolVersion != 196608)
        {
            await _writer.WriteErrorResponseAsync(
                "FATAL", "0A000", $"Unsupported protocol version: {protocolVersion}", ct).ConfigureAwait(false);
            await _writer.FlushAsync(ct).ConfigureAwait(false);
            return false;
        }

        foreach (var (key, value) in parameters)
            _clientParameters[key] = value;

        var user = parameters.GetValueOrDefault("user", "surgewave");
        _logger.LogDebug("PostgreSQL client startup: user={User}, database={Database}",
            user, parameters.GetValueOrDefault("database", "surgewave"));

        // Authentication
        if (_config.RequirePassword && !string.IsNullOrEmpty(_config.Password))
        {
            await _writer.WriteAuthenticationCleartextPasswordAsync(ct).ConfigureAwait(false);
            await _writer.FlushAsync(ct).ConfigureAwait(false);

            var passwordMsg = await _reader.ReadMessageAsync(ct).ConfigureAwait(false);
            if (passwordMsg is null)
                return false;

            if (passwordMsg.Value.Type != PgFrontendMessage.Password)
            {
                await _writer.WriteErrorResponseAsync(
                    "FATAL", "28P01", "Expected password message", ct).ConfigureAwait(false);
                await _writer.FlushAsync(ct).ConfigureAwait(false);
                return false;
            }

            var password = PgReader.ReadPasswordFromPayload(passwordMsg.Value.Payload);
            if (password != _config.Password)
            {
                await _writer.WriteErrorResponseAsync(
                    "FATAL", "28P01", $"Password authentication failed for user \"{user}\"", ct)
                    .ConfigureAwait(false);
                await _writer.FlushAsync(ct).ConfigureAwait(false);
                return false;
            }
        }

        // AuthenticationOk
        await _writer.WriteAuthenticationOkAsync(ct).ConfigureAwait(false);

        // Send parameter status messages
        await _writer.WriteParameterStatusAsync("server_version", _config.ServerVersion, ct).ConfigureAwait(false);
        await _writer.WriteParameterStatusAsync("server_encoding", "UTF8", ct).ConfigureAwait(false);
        await _writer.WriteParameterStatusAsync("client_encoding", "UTF8", ct).ConfigureAwait(false);
        await _writer.WriteParameterStatusAsync("DateStyle", "ISO, MDY", ct).ConfigureAwait(false);
        await _writer.WriteParameterStatusAsync("TimeZone", "UTC", ct).ConfigureAwait(false);
        await _writer.WriteParameterStatusAsync("integer_datetimes", "on", ct).ConfigureAwait(false);
        await _writer.WriteParameterStatusAsync("standard_conforming_strings", "on", ct).ConfigureAwait(false);
        await _writer.WriteParameterStatusAsync("is_superuser", "on", ct).ConfigureAwait(false);
        await _writer.WriteParameterStatusAsync("session_authorization", user, ct).ConfigureAwait(false);
        await _writer.WriteParameterStatusAsync("application_name",
            parameters.GetValueOrDefault("application_name", ""), ct).ConfigureAwait(false);

        // Backend key data
        await _writer.WriteBackendKeyDataAsync(_processId, _secretKey, ct).ConfigureAwait(false);

        // Ready for query
        await _writer.WriteReadyForQueryAsync(PgTransactionStatus.Idle, ct).ConfigureAwait(false);
        await _writer.FlushAsync(ct).ConfigureAwait(false);

        return true;
    }

    private async Task RunQueryLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var msg = await _reader.ReadMessageAsync(ct).ConfigureAwait(false);
            if (msg is null)
                break;

            var (type, payload) = msg.Value;

            switch (type)
            {
                case PgFrontendMessage.Query:
                    await HandleSimpleQueryAsync(payload, ct).ConfigureAwait(false);
                    break;

                case PgFrontendMessage.Parse:
                    await HandleParseAsync(payload, ct).ConfigureAwait(false);
                    break;

                case PgFrontendMessage.Bind:
                    await HandleBindAsync(payload, ct).ConfigureAwait(false);
                    break;

                case PgFrontendMessage.Describe:
                    await HandleDescribeAsync(payload, ct).ConfigureAwait(false);
                    break;

                case PgFrontendMessage.Execute:
                    await HandleExecuteAsync(payload, ct).ConfigureAwait(false);
                    break;

                case PgFrontendMessage.Sync:
                    await _writer.WriteReadyForQueryAsync(PgTransactionStatus.Idle, ct).ConfigureAwait(false);
                    await _writer.FlushAsync(ct).ConfigureAwait(false);
                    break;

                case PgFrontendMessage.Flush:
                    await _writer.FlushAsync(ct).ConfigureAwait(false);
                    break;

                case PgFrontendMessage.Close:
                    await HandleCloseAsync(payload, ct).ConfigureAwait(false);
                    break;

                case PgFrontendMessage.Terminate:
                    _logger.LogDebug("PostgreSQL client terminated (pid={ProcessId})", _processId);
                    return;

                default:
                    _logger.LogDebug("PostgreSQL unknown message type: 0x{Type:X2}", type);
                    break;
            }
        }
    }

    private async Task HandleSimpleQueryAsync(byte[] payload, CancellationToken ct)
    {
        var sql = PgReader.ReadQueryFromPayload(payload);
        _logger.LogDebug("PostgreSQL query (pid={ProcessId}): {Sql}", _processId, sql);

        // Handle multiple statements separated by semicolons (psql sends these)
        var statements = SplitStatements(sql);
        foreach (var stmt in statements)
        {
            if (string.IsNullOrWhiteSpace(stmt))
                continue;

            var result = _queryExecutor.Execute(stmt);
            await SendQueryResultAsync(result, ct).ConfigureAwait(false);
        }

        await _writer.WriteReadyForQueryAsync(PgTransactionStatus.Idle, ct).ConfigureAwait(false);
        await _writer.FlushAsync(ct).ConfigureAwait(false);
    }

    private async Task HandleParseAsync(byte[] payload, CancellationToken ct)
    {
        var (name, query, _) = PgReader.ReadParsePayload(payload);
        var key = string.IsNullOrEmpty(name) ? "" : name;

        _preparedStatements[key] = new PreparedStatement(query);
        await _writer.WriteParseCompleteAsync(ct).ConfigureAwait(false);
    }

    private async Task HandleBindAsync(byte[] payload, CancellationToken ct)
    {
        var (portal, statement) = PgReader.ReadBindPayload(payload);
        var portalKey = string.IsNullOrEmpty(portal) ? "" : portal;
        var stmtKey = string.IsNullOrEmpty(statement) ? "" : statement;

        if (_preparedStatements.TryGetValue(stmtKey, out var ps))
            _portals[portalKey] = ps;

        await _writer.WriteBindCompleteAsync(ct).ConfigureAwait(false);
    }

    private async Task HandleDescribeAsync(byte[] payload, CancellationToken ct)
    {
        var (descType, name) = PgReader.ReadDescribePayload(payload);
        var key = string.IsNullOrEmpty(name) ? "" : name;

        PreparedStatement? ps = null;
        if (descType == (byte)'S')
            _preparedStatements.TryGetValue(key, out ps);
        else if (descType == (byte)'P')
            _portals.TryGetValue(key, out ps);

        if (ps is not null)
        {
            // Execute the query to discover column types, cache the result
            ps.CachedResult ??= _queryExecutor.Execute(ps.Sql);
            var result = ps.CachedResult;

            await _writer.WriteParameterDescriptionAsync(ct).ConfigureAwait(false);

            if (result.IsError || result.Columns.Count == 0)
            {
                await _writer.WriteNoDataAsync(ct).ConfigureAwait(false);
            }
            else
            {
                await _writer.WriteRowDescriptionAsync(result.Columns, ct).ConfigureAwait(false);
            }
        }
        else
        {
            await _writer.WriteParameterDescriptionAsync(ct).ConfigureAwait(false);
            await _writer.WriteNoDataAsync(ct).ConfigureAwait(false);
        }
    }

    private async Task HandleExecuteAsync(byte[] payload, CancellationToken ct)
    {
        var (portal, _) = PgReader.ReadExecutePayload(payload);
        var portalKey = string.IsNullOrEmpty(portal) ? "" : portal;

        if (_portals.TryGetValue(portalKey, out var ps))
        {
            // Use cached result from Describe, or execute now
            var result = ps.CachedResult ?? _queryExecutor.Execute(ps.Sql);
            ps.CachedResult = null; // Clear cache after use

            if (result.IsError)
            {
                var err = result.ErrorInfo!;
                await _writer.WriteErrorResponseAsync(err.Severity, err.Code, err.Message, ct)
                    .ConfigureAwait(false);
            }
            else
            {
                // Don't send RowDescription in extended protocol Execute
                // (it's already sent in Describe)
                foreach (var row in result.Rows)
                    await _writer.WriteDataRowAsync(row, ct).ConfigureAwait(false);

                await _writer.WriteCommandCompleteAsync(result.CommandTag, ct).ConfigureAwait(false);
            }
        }
        else
        {
            await _writer.WriteCommandCompleteAsync("SELECT 0", ct).ConfigureAwait(false);
        }
    }

    private async Task HandleCloseAsync(byte[] payload, CancellationToken ct)
    {
        if (payload.Length > 0)
        {
            var closeType = payload[0];
            var offset = 1;
            var name = ReadCStringFromPayload(payload, ref offset);
            var key = string.IsNullOrEmpty(name) ? "" : name;

            if (closeType == (byte)'S')
                _preparedStatements.TryRemove(key, out _);
            else if (closeType == (byte)'P')
                _portals.TryRemove(key, out _);
        }

        await _writer.WriteCloseCompleteAsync(ct).ConfigureAwait(false);
    }

    private async Task SendQueryResultAsync(QueryResult result, CancellationToken ct)
    {
        if (result.IsError)
        {
            var err = result.ErrorInfo!;
            await _writer.WriteErrorResponseAsync(err.Severity, err.Code, err.Message, ct)
                .ConfigureAwait(false);
            return;
        }

        // Commands with no result set (SET, BEGIN, etc.)
        if (result.Columns.Count == 0 && result.Rows.Count == 0)
        {
            await _writer.WriteCommandCompleteAsync(result.CommandTag, ct).ConfigureAwait(false);
            return;
        }

        // Result set with rows
        await _writer.WriteRowDescriptionAsync(result.Columns, ct).ConfigureAwait(false);

        foreach (var row in result.Rows)
            await _writer.WriteDataRowAsync(row, ct).ConfigureAwait(false);

        await _writer.WriteCommandCompleteAsync(result.CommandTag, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Splits a SQL string into individual statements by semicolons,
    /// respecting single-quoted string literals and double-quoted identifiers.
    /// </summary>
    private static List<string> SplitStatements(string sql)
    {
        var statements = new List<string>();
        var start = 0;
        var inSingleQuote = false;
        var inDoubleQuote = false;

        for (var i = 0; i < sql.Length; i++)
        {
            var c = sql[i];
            switch (c)
            {
                case '\'' when !inDoubleQuote:
                    inSingleQuote = !inSingleQuote;
                    break;
                case '"' when !inSingleQuote:
                    inDoubleQuote = !inDoubleQuote;
                    break;
                case ';' when !inSingleQuote && !inDoubleQuote:
                    var stmt = sql[start..i].Trim();
                    if (stmt.Length > 0)
                        statements.Add(stmt);
                    start = i + 1;
                    break;
            }
        }

        var last = sql[start..].Trim();
        if (last.Length > 0)
            statements.Add(last);

        return statements;
    }

    private static string ReadCStringFromPayload(byte[] buf, ref int offset)
    {
        var start = offset;
        while (offset < buf.Length && buf[offset] != 0)
            offset++;
        var str = System.Text.Encoding.UTF8.GetString(buf, start, offset - start);
        if (offset < buf.Length)
            offset++;
        return str;
    }

    /// <summary>
    /// Holds a prepared statement's SQL and optionally cached result.
    /// </summary>
    private sealed class PreparedStatement(string sql)
    {
        public string Sql { get; } = sql;
        public QueryResult? CachedResult { get; set; }
    }
}
