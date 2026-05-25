using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Control.Models;
using Kuestenlogik.Surgewave.Control.Models.Assistant;

namespace Kuestenlogik.Surgewave.Control.Services.Assistant;

/// <summary>
/// Pattern-based natural-language to SQL translator with optional LLM fallback.
/// Supports English and German question patterns. When pattern matching fails
/// and an LLM client is configured, delegates to the LLM for SQL generation.
/// </summary>
public sealed partial class NlToSqlTranslator : INlToSqlTranslator
{
    private readonly ISurgewaveApiClient _apiClient;
    private readonly ILlmClient _llmClient;
    private readonly ISchemaRegistryClient _schemaClient;
    private readonly AssistantSettings _settings;
    private readonly ILogger<NlToSqlTranslator> _logger;

    public NlToSqlTranslator(
        ISurgewaveApiClient apiClient,
        ILlmClient llmClient,
        ISchemaRegistryClient schemaClient,
        AssistantSettings settings,
        ILogger<NlToSqlTranslator> logger)
    {
        _apiClient = apiClient;
        _llmClient = llmClient;
        _schemaClient = schemaClient;
        _settings = settings;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<NlSqlResult> TranslateAsync(string question, CancellationToken ct = default)
    {
        var normalized = question.Trim().ToLowerInvariant();

        // Fetch available topics for validation
        HashSet<string> knownTopics;
        try
        {
            var topics = await _apiClient.ListTopicsAsync(ct: ct);
            knownTopics = new HashSet<string>(topics.Select(t => t.Name), StringComparer.OrdinalIgnoreCase);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to fetch topic list for NL-to-SQL validation");
            knownTopics = [];
        }

        // Try each pattern in order of specificity
        var result = TryGroupByPattern(normalized, knownTopics)
                  ?? TryTopNPattern(normalized, knownTopics)
                  ?? TrySumPattern(normalized, knownTopics)
                  ?? TryMinMaxPattern(normalized, knownTopics)
                  ?? TryAveragePattern(normalized, knownTopics)
                  ?? TryDistinctPattern(normalized, knownTopics)
                  ?? TryBetweenDatesPattern(normalized, knownTopics)
                  ?? TryCountPattern(normalized, knownTopics)
                  ?? TryCountGermanPattern(normalized, knownTopics)
                  ?? TryLatestNPattern(normalized, knownTopics)
                  ?? TryOldestNewestPattern(normalized, knownTopics)
                  ?? TryTimeRangePattern(normalized, knownTopics)
                  ?? TryWhereNullPattern(normalized, knownTopics)
                  ?? TryWherePattern(normalized, knownTopics)
                  ?? TryGermanShowPattern(normalized, knownTopics)
                  ?? TryGermanAggregatePattern(normalized, knownTopics)
                  ?? TryShowMessagesPattern(normalized, knownTopics);

        if (result is not null)
            return result;

        // Partial match: if we can identify a topic, build a basic SELECT
        var topic = ExtractTopic(normalized, knownTopics);
        if (topic is not null)
        {
            var patternFallback = new NlSqlResult
            {
                OriginalQuestion = question,
                GeneratedSql = $"SELECT * FROM {topic} LIMIT 10",
                Confidence = 0.3,
                Source = NlSqlSource.Fallback
            };

            // If confidence is low and LLM is available, try LLM
            if (_settings.LlmEnabled)
            {
                var llmResult = await TryLlmTranslationAsync(question, knownTopics, ct);
                if (llmResult is not null && llmResult.Confidence > patternFallback.Confidence)
                    return llmResult;
            }

            return patternFallback;
        }

        // No pattern matched at all — try LLM as last resort
        if (_settings.LlmEnabled)
        {
            var llmResult = await TryLlmTranslationAsync(question, knownTopics, ct);
            if (llmResult is not null)
                return llmResult;
        }

        return new NlSqlResult
        {
            OriginalQuestion = question,
            GeneratedSql = "",
            Confidence = 0.0,
            Source = NlSqlSource.Fallback,
            Error = "Could not understand the question. Try phrases like 'show messages from {topic}', " +
                    "'count messages in {topic}', 'latest 5 from {topic}', 'top 10 status from orders', " +
                    "or 'sum of amount from orders'."
        };
    }

    // ========== PATTERN MATCHERS ==========

    private static NlSqlResult? TryShowMessagesPattern(string input, HashSet<string> knownTopics)
    {
        // "show messages from {topic}" / "show me messages from {topic}" / "get messages from {topic}"
        var match = ShowMessagesRegex().Match(input);
        if (!match.Success) return null;

        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT * FROM {topic} LIMIT 10",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryCountPattern(string input, HashSet<string> knownTopics)
    {
        // "count messages in {topic}" / "how many messages in {topic}"
        var match = CountMessagesRegex().Match(input);
        if (!match.Success) return null;

        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT COUNT(*) FROM {topic}",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryCountGermanPattern(string input, HashSet<string> knownTopics)
    {
        // "wie viele nachrichten in {topic}" / "anzahl nachrichten in {topic}" / "wie viele in {topic}"
        var match = CountGermanRegex().Match(input);
        if (!match.Success) return null;

        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT COUNT(*) FROM {topic}",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryLatestNPattern(string input, HashSet<string> knownTopics)
    {
        // "latest N from {topic}" / "last N from {topic}" / "newest N from {topic}"
        var match = LatestNRegex().Match(input);
        if (!match.Success) return null;

        var n = int.TryParse(match.Groups["n"].Value, out var count) ? count : 10;
        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT * FROM {topic} ORDER BY _timestamp DESC LIMIT {n}",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryOldestNewestPattern(string input, HashSet<string> knownTopics)
    {
        // "oldest message from {topic}" / "newest message from {topic}" / "first message from {topic}"
        // German: "aelteste nachricht aus {topic}" / "neueste nachricht aus {topic}"
        var match = OldestNewestRegex().Match(input);
        if (!match.Success) return null;

        var direction = match.Groups["dir"].Value.Trim().ToLowerInvariant();
        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        var isOldest = direction is "oldest" or "first" or "earliest"
                       or "\u00e4lteste" or "aelteste" or "erste" or "ersten";
        var order = isOldest ? "ASC" : "DESC";

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT * FROM {topic} ORDER BY _timestamp {order} LIMIT 1",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryWherePattern(string input, HashSet<string> knownTopics)
    {
        // "messages where {field} > {value}" / "messages from {topic} where {field} = {value}"
        // Also: "messages with {field} = {value}"
        var match = WhereClauseRegex().Match(input);
        if (!match.Success) return null;

        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);
        var field = match.Groups["field"].Value.Trim();
        var op = match.Groups["op"].Value.Trim();
        var value = match.Groups["value"].Value.Trim();

        // Try to determine if value is numeric or string
        var sqlValue = double.TryParse(value, System.Globalization.CultureInfo.InvariantCulture, out _)
            ? value
            : $"'{value}'";

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT * FROM {topic} WHERE {field} {op} {sqlValue}",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryWhereNullPattern(string input, HashSet<string> knownTopics)
    {
        // "messages without {field}" / "messages from {topic} without {field}"
        // "messages where {field} is null" / "messages missing {field}"
        var match = WhereNullRegex().Match(input);
        if (!match.Success) return null;

        var topic = match.Groups["topic"].Success
            ? ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics)
            : ExtractTopic(input, knownTopics) ?? "topic";
        var field = match.Groups["field"].Value.Trim();

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT * FROM {topic} WHERE {field} IS NULL",
            Confidence = 0.9,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryAveragePattern(string input, HashSet<string> knownTopics)
    {
        // "average {field} from {topic}" / "avg {field} from {topic}" / "mean {field} from {topic}"
        // German: "durchschnitt {field} aus {topic}"
        var match = AverageRegex().Match(input);
        if (!match.Success) return null;

        var field = match.Groups["field"].Value.Trim();
        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT AVG({field}) FROM {topic}",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TrySumPattern(string input, HashSet<string> knownTopics)
    {
        // "sum of {field} from {topic}" / "total {field} from {topic}"
        // German: "summe von {field} aus {topic}"
        var match = SumRegex().Match(input);
        if (!match.Success) return null;

        var field = match.Groups["field"].Value.Trim();
        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT SUM({field}) FROM {topic}",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryMinMaxPattern(string input, HashSet<string> knownTopics)
    {
        // "min of {field} from {topic}" / "max of {field} from {topic}"
        // "minimum {field} from {topic}" / "maximum {field} from {topic}"
        var match = MinMaxRegex().Match(input);
        if (!match.Success) return null;

        var func = match.Groups["func"].Value.Trim().ToLowerInvariant();
        var field = match.Groups["field"].Value.Trim();
        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        var sqlFunc = func.StartsWith("min", StringComparison.Ordinal) ? "MIN" : "MAX";

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT {sqlFunc}({field}) FROM {topic}",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryDistinctPattern(string input, HashSet<string> knownTopics)
    {
        // "distinct {field} from {topic}" / "unique {field} from {topic}"
        var match = DistinctRegex().Match(input);
        if (!match.Success) return null;

        var field = match.Groups["field"].Value.Trim();
        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT DISTINCT {field} FROM {topic}",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryGroupByPattern(string input, HashSet<string> knownTopics)
    {
        // "group by {field} from {topic}" / "group {topic} by {field}"
        // "count by {field} from {topic}" / "count per {field} from {topic}"
        var match = GroupByRegex().Match(input);
        if (!match.Success) return null;

        var field = match.Groups["field"].Value.Trim();
        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT {field}, COUNT(*) FROM {topic} GROUP BY {field}",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryTopNPattern(string input, HashSet<string> knownTopics)
    {
        // "top N {field} from {topic}" / "top N values of {field} from {topic}"
        var match = TopNRegex().Match(input);
        if (!match.Success) return null;

        var n = int.TryParse(match.Groups["n"].Value, out var count) ? count : 10;
        var field = match.Groups["field"].Value.Trim();
        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT {field}, COUNT(*) FROM {topic} GROUP BY {field} ORDER BY COUNT(*) DESC LIMIT {n}",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryBetweenDatesPattern(string input, HashSet<string> knownTopics)
    {
        // "messages from {topic} between {date1} and {date2}"
        var match = BetweenDatesRegex().Match(input);
        if (!match.Success) return null;

        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);
        var date1 = match.Groups["date1"].Value.Trim();
        var date2 = match.Groups["date2"].Value.Trim();

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT * FROM {topic} WHERE _timestamp BETWEEN '{date1}' AND '{date2}'",
            Confidence = 0.9,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryTimeRangePattern(string input, HashSet<string> knownTopics)
    {
        // "messages in last N minutes from {topic}" / "messages from {topic} in last N minutes"
        var match = TimeRangeRegex().Match(input);
        if (!match.Success)
        {
            match = TimeRangeAltRegex().Match(input);
            if (!match.Success) return null;
        }

        var n = int.TryParse(match.Groups["n"].Value, out var minutes) ? minutes : 5;
        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);
        var unit = match.Groups["unit"].Value.Trim().ToLowerInvariant();

        // Convert unit to minutes
        var totalMinutes = unit switch
        {
            "hour" or "hours" or "stunde" or "stunden" => n * 60,
            "day" or "days" or "tag" or "tage" or "tagen" => n * 1440,
            "second" or "seconds" or "sec" or "secs" or "sekunde" or "sekunden" => Math.Max(1, n / 60),
            _ => n // default: minutes
        };

        var since = DateTimeOffset.UtcNow.AddMinutes(-totalMinutes);
        var isoTimestamp = since.ToString("yyyy-MM-ddTHH:mm:ssZ");

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT * FROM {topic} WHERE _timestamp > '{isoTimestamp}'",
            Confidence = 0.9,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryGermanShowPattern(string input, HashSet<string> knownTopics)
    {
        // "zeige nachrichten aus {topic}" / "zeige mir nachrichten von {topic}" / "zeige {topic}"
        var match = GermanShowRegex().Match(input);
        if (!match.Success) return null;

        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT * FROM {topic} LIMIT 10",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    private static NlSqlResult? TryGermanAggregatePattern(string input, HashSet<string> knownTopics)
    {
        // "neueste N aus {topic}" / "aelteste N aus {topic}"
        var match = GermanLatestNRegex().Match(input);
        if (!match.Success) return null;

        var direction = match.Groups["dir"].Value.Trim().ToLowerInvariant();
        var n = int.TryParse(match.Groups["n"].Value, out var count) ? count : 10;
        var topic = ResolveTopic(match.Groups["topic"].Value.Trim(), knownTopics);

        var isOldest = direction is "\u00e4lteste" or "\u00e4ltesten" or "aelteste" or "aeltesten" or "erste" or "ersten";
        var order = isOldest ? "ASC" : "DESC";

        return new NlSqlResult
        {
            OriginalQuestion = input,
            GeneratedSql = $"SELECT * FROM {topic} ORDER BY _timestamp {order} LIMIT {n}",
            Confidence = 1.0,
            Source = NlSqlSource.Pattern
        };
    }

    // ========== LLM FALLBACK ==========

    private async Task<NlSqlResult?> TryLlmTranslationAsync(
        string question,
        HashSet<string> knownTopics,
        CancellationToken ct)
    {
        try
        {
            var systemPrompt = await BuildLlmSystemPromptAsync(knownTopics, ct);
            var llmResponse = await _llmClient.CompleteAsync(systemPrompt, question, ct);

            if (string.IsNullOrWhiteSpace(llmResponse) || llmResponse.StartsWith("LLM not configured", StringComparison.Ordinal))
                return null;

            var sql = ExtractSqlFromResponse(llmResponse);
            if (string.IsNullOrWhiteSpace(sql))
                return null;

            // Validate: only allow SELECT statements
            var trimmed = sql.TrimStart();
            if (!trimmed.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)
                && !trimmed.StartsWith("WITH", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("LLM generated non-SELECT SQL, rejecting: {Sql}", sql);
                return null;
            }

            // Reject dangerous statements
            if (ContainsDangerousKeyword(trimmed))
            {
                _logger.LogWarning("LLM generated potentially dangerous SQL, rejecting: {Sql}", sql);
                return null;
            }

            // Confidence based on whether we recognize the topic in the generated SQL
            var confidence = 0.7;
            foreach (var topic in knownTopics)
            {
                if (sql.Contains(topic, StringComparison.OrdinalIgnoreCase))
                {
                    confidence = 0.85;
                    break;
                }
            }

            return new NlSqlResult
            {
                OriginalQuestion = question,
                GeneratedSql = sql,
                Confidence = confidence,
                Source = NlSqlSource.Llm
            };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "LLM translation failed for question: {Question}", question);
            return null;
        }
    }

    private async Task<string> BuildLlmSystemPromptAsync(
        HashSet<string> knownTopics,
        CancellationToken ct)
    {
        var topicList = knownTopics.Count > 0
            ? string.Join(", ", knownTopics.Take(50))
            : "(no topics available)";

        // Try to fetch schema information for known topics
        var schemaInfo = await GetSchemaInfoAsync(knownTopics, ct);

        return $"""
            You are a SQL query generator for Surgewave, a high-performance message streaming platform.
            Your task is to translate natural-language questions into Surgewave SQL queries.

            AVAILABLE TOPICS: {topicList}

            METADATA COLUMNS (available on all topics):
            - _offset (long): message offset within the partition
            - _partition (int): partition number
            - _timestamp (datetime): when the message was produced
            - _key (string): message key

            {schemaInfo}

            SUPPORTED SQL FEATURES:
            - SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
            - Aggregate functions: COUNT(*), SUM(field), AVG(field), MIN(field), MAX(field)
            - Operators: =, !=, <, >, <=, >=, LIKE, IN, BETWEEN, IS NULL, IS NOT NULL
            - Logical operators: AND, OR, NOT
            - String functions: UPPER(field), LOWER(field), LENGTH(field)
            - DISTINCT keyword

            RULES:
            1. ONLY generate SELECT statements. Never generate INSERT, UPDATE, DELETE, DROP, or CREATE.
            2. Always reference a real topic from the available list when possible.
            3. Use _timestamp for time-based filtering.
            4. Default LIMIT to 50 if the user does not specify one.
            5. JSON message fields become columns automatically (e.g., amount, status, customer).
            6. Return ONLY the SQL query, no explanations. If you must use a code block, use ```sql ... ```.
            7. Understand both English and German questions.
            """;
    }

    private async Task<string> GetSchemaInfoAsync(HashSet<string> knownTopics, CancellationToken ct)
    {
        try
        {
            var summaries = await _schemaClient.GetInferredSchemasAsync(ct);
            if (summaries.Count == 0)
                return "TOPIC SCHEMAS: No inferred schemas available. JSON message fields become columns automatically.";

            var lines = new List<string> { "KNOWN TOPIC SCHEMAS:" };
            foreach (var summary in summaries.Take(10))
            {
                if (!knownTopics.Contains(summary.Topic))
                    continue;

                try
                {
                    var schema = await _schemaClient.InferSchemaAsync(summary.Topic, cancellationToken: ct);
                    if (schema?.FieldStats is { Count: > 0 })
                    {
                        var fields = string.Join(", ", schema.FieldStats.Take(20).Select(f => $"{f.Path} ({f.Type})"));
                        lines.Add($"- {summary.Topic}: {fields}");
                    }
                }
                catch
                {
                    // Skip schemas we cannot fetch
                }
            }

            return lines.Count > 1 ? string.Join("\n", lines) : "";
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not fetch schema info for LLM prompt");
            return "";
        }
    }

    private static string ExtractSqlFromResponse(string response)
    {
        // Try to extract from markdown code block first
        var codeBlockMatch = SqlCodeBlockRegex().Match(response);
        if (codeBlockMatch.Success)
            return codeBlockMatch.Groups["sql"].Value.Trim();

        // Try generic code block
        var genericBlock = GenericCodeBlockRegex().Match(response);
        if (genericBlock.Success)
            return genericBlock.Groups["sql"].Value.Trim();

        // If response looks like raw SQL, use it directly
        var trimmed = response.Trim();
        if (trimmed.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("WITH", StringComparison.OrdinalIgnoreCase))
        {
            // Take only the first statement (up to semicolon or end)
            var semiIdx = trimmed.IndexOf(';');
            return semiIdx > 0 ? trimmed[..semiIdx] : trimmed;
        }

        return "";
    }

    private static bool ContainsDangerousKeyword(string sql)
    {
        ReadOnlySpan<string> dangerous =
        [
            "INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "CREATE ",
            "TRUNCATE ", "GRANT ", "REVOKE ", "EXEC ", "EXECUTE "
        ];

        var upper = sql.ToUpperInvariant();
        foreach (var keyword in dangerous)
        {
            if (upper.Contains(keyword, StringComparison.Ordinal))
                return true;
        }
        return false;
    }

    // ========== HELPERS ==========

    private static string ResolveTopic(string candidate, HashSet<string> knownTopics)
    {
        // Clean up common trailing words
        candidate = candidate.TrimEnd('.', '?', '!', ' ');
        candidate = StripTrailingWords().Replace(candidate, "").Trim();

        // Exact match
        if (knownTopics.Contains(candidate))
            return candidate;

        // Case-insensitive match
        var match = knownTopics.FirstOrDefault(t => t.Equals(candidate, StringComparison.OrdinalIgnoreCase));
        if (match is not null)
            return match;

        // Return as-is (user may know a topic we haven't fetched yet)
        return candidate;
    }

    private static string? ExtractTopic(string input, HashSet<string> knownTopics)
    {
        foreach (var topic in knownTopics)
        {
            if (input.Contains(topic, StringComparison.OrdinalIgnoreCase))
                return topic;
        }
        return null;
    }

    // ========== REGEX PATTERNS ==========

    [GeneratedRegex(@"(?:show|get|display|list)\s+(?:me\s+)?messages\s+(?:from|in)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex ShowMessagesRegex();

    [GeneratedRegex(@"(?:count|how many)\s+messages\s+(?:in|from|for)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex CountMessagesRegex();

    [GeneratedRegex(@"(?:wie\s+viele|anzahl(?:\s+der)?)\s+(?:nachrichten\s+)?(?:in|aus|von|f\u00fcr|fuer)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex CountGermanRegex();

    [GeneratedRegex(@"(?:latest|last|newest|recent)\s+(?<n>\d+)\s+(?:messages?\s+)?(?:from|in)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex LatestNRegex();

    [GeneratedRegex(@"(?<dir>oldest|newest|first|last|earliest|latest|\u00e4lteste|aelteste|neueste|erste|ersten)\s+(?:message|nachricht|eintrag)?\s*(?:from|in|aus|von)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex OldestNewestRegex();

    [GeneratedRegex(@"messages?\s+(?:from\s+(?<topic>\S+)\s+)?(?:where|with)\s+(?<field>\w+)\s*(?<op>[><=!]+)\s*(?<value>\S+)", RegexOptions.IgnoreCase)]
    private static partial Regex WhereClauseRegex();

    [GeneratedRegex(@"messages?\s+(?:from\s+(?<topic>\S+)\s+)?(?:without|missing)\s+(?<field>\w+)", RegexOptions.IgnoreCase)]
    private static partial Regex WhereNullRegex();

    [GeneratedRegex(@"(?:average|avg|mean|durchschnitt(?:\s+von)?)\s+(?<field>\w+)\s+(?:from|in|of|aus|von)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex AverageRegex();

    [GeneratedRegex(@"(?:sum|total|summe)(?:\s+(?:of|von))?\s+(?<field>\w+)\s+(?:from|in|aus|von)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex SumRegex();

    [GeneratedRegex(@"(?<func>min|max|minimum|maximum)(?:\s+(?:of|von))?\s+(?<field>\w+)\s+(?:from|in|aus|von)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex MinMaxRegex();

    [GeneratedRegex(@"(?:distinct|unique|eindeutige?)\s+(?<field>\w+)\s+(?:from|in|aus|von)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex DistinctRegex();

    [GeneratedRegex(@"(?:group|count)\s+(?:by|per)\s+(?<field>\w+)\s+(?:from|in|aus|von)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex GroupByRegex();

    [GeneratedRegex(@"top\s+(?<n>\d+)\s+(?:values?\s+(?:of|for)\s+)?(?<field>\w+)\s+(?:from|in|aus|von)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex TopNRegex();

    [GeneratedRegex(@"messages?\s+(?:from\s+(?<topic>\S+)\s+)?between\s+(?<date1>\S+)\s+and\s+(?<date2>\S+)", RegexOptions.IgnoreCase)]
    private static partial Regex BetweenDatesRegex();

    [GeneratedRegex(@"messages?\s+(?:from\s+(?<topic>\S+)\s+)?in\s+(?:the\s+)?last\s+(?<n>\d+)\s+(?<unit>minutes?|mins?|hours?|days?|seconds?|secs?|stunden?|tage[n]?|minuten?|sekunden?)", RegexOptions.IgnoreCase)]
    private static partial Regex TimeRangeRegex();

    [GeneratedRegex(@"messages?\s+in\s+(?:the\s+)?last\s+(?<n>\d+)\s+(?<unit>minutes?|mins?|hours?|days?|seconds?|secs?|stunden?|tage[n]?|minuten?|sekunden?)\s+(?:from|in)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex TimeRangeAltRegex();

    [GeneratedRegex(@"(?:zeige|zeig)\s+(?:mir\s+)?(?:nachrichten\s+)?(?:aus|von|in)?\s*(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex GermanShowRegex();

    [GeneratedRegex(@"(?<dir>neueste[n]?|\u00e4lteste[n]?|aelteste[n]?|erste[n]?|letzte[n]?)\s+(?<n>\d+)\s+(?:nachrichten\s+)?(?:aus|von|in)\s+(?<topic>.+)", RegexOptions.IgnoreCase)]
    private static partial Regex GermanLatestNRegex();

    [GeneratedRegex(@"\s+(?:table|topic|please|now|right now|bitte|jetzt)$", RegexOptions.IgnoreCase)]
    private static partial Regex StripTrailingWords();

    [GeneratedRegex(@"```sql\s*\n?(?<sql>[\s\S]+?)\n?\s*```", RegexOptions.IgnoreCase)]
    private static partial Regex SqlCodeBlockRegex();

    [GeneratedRegex(@"```\s*\n?(?<sql>[\s\S]+?)\n?\s*```")]
    private static partial Regex GenericCodeBlockRegex();
}
