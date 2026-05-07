using System.Text;
using Kuestenlogik.Surgewave.Control.Models.Assistant;

namespace Kuestenlogik.Surgewave.Control.Services.Assistant;

/// <summary>
/// Orchestrates assistant features by detecting intent and delegating to specialized services.
/// Manages conversation history within a scoped Blazor circuit.
/// </summary>
public sealed class AssistantService : IAssistantService
{
    private readonly IMetricsAnalyzer _metricsAnalyzer;
    private readonly ITuningAdvisor _tuningAdvisor;
    private readonly INlToSqlTranslator _nlToSql;
    private readonly ILlmClient _llmClient;
    private readonly IMetricsClient _metricsClient;
    private readonly AssistantSettings _settings;
    private readonly ILogger<AssistantService> _logger;

    private readonly List<AssistantMessage> _history = [];

    public AssistantService(
        IMetricsAnalyzer metricsAnalyzer,
        ITuningAdvisor tuningAdvisor,
        INlToSqlTranslator nlToSql,
        ILlmClient llmClient,
        IMetricsClient metricsClient,
        AssistantSettings settings,
        ILogger<AssistantService> logger)
    {
        _metricsAnalyzer = metricsAnalyzer;
        _tuningAdvisor = tuningAdvisor;
        _nlToSql = nlToSql;
        _llmClient = llmClient;
        _metricsClient = metricsClient;
        _settings = settings;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<AssistantMessage> AskAsync(string question, CancellationToken ct = default)
    {
        // Record user message
        _history.Add(new AssistantMessage { Role = "user", Content = question });

        try
        {
            var intent = DetectIntent(question);
            _logger.LogDebug("Assistant intent for '{Question}': {Intent}", question, intent);

            var response = intent switch
            {
                Intent.HealthCheck => await HandleHealthCheckAsync(ct),
                Intent.TuningAdvice => await HandleTuningAdviceAsync(ct),
                Intent.SqlQuery => await HandleSqlQueryAsync(question, ct),
                Intent.ExplainAnomaly => await HandleExplainAnomalyAsync(question, ct),
                Intent.IntentConfig => HandleIntentConfig(question),
                _ => HandleGeneralHelp(question)
            };

            _history.Add(response);
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Assistant failed to process question: {Question}", question);

            var errorResponse = new AssistantMessage
            {
                Role = "assistant",
                Content = $"Sorry, I encountered an error while processing your request: {ex.Message}"
            };
            _history.Add(errorResponse);
            return errorResponse;
        }
    }

    private static Intent DetectIntent(string question)
    {
        var lower = question.ToLowerInvariant();

        // Intent-based topic creation (check first since it's most specific)
        if (ContainsAny(lower, "create a topic for", "create topic for", "i need a topic for",
                         "set up a topic for", "configure a topic for", "new topic for",
                         "erstelle einen topic für", "erstelle topic für", "erstelle ein topic für",
                         "ich brauche einen topic für", "topic anlegen für", "topic einrichten für"))
        {
            return Intent.IntentConfig;
        }

        // SQL / query intent (check first since it's most specific)
        if (ContainsAny(lower, "show", "count", "select", "query", "messages from", "latest", "newest",
                         "how many messages", "average", "avg", "sum of", "min of", "max of", "distinct",
                         "group by", "top ", "oldest", "between", "messages where", "messages with",
                         "zeige", "wie viele", "anzahl", "neueste", "\u00e4lteste", "aelteste",
                         "durchschnitt", "summe", "nachrichten"))
        {
            return Intent.SqlQuery;
        }

        // Health / status check
        if (ContainsAny(lower, "health", "status", "check", "healthy", "alive", "up?", "running",
                         "gesundheit", "zustand"))
        {
            return Intent.HealthCheck;
        }

        // Tuning / optimization
        if (ContainsAny(lower, "tune", "tuning", "optimize", "improve", "recommend", "recommendation",
                         "performance", "faster", "slow", "speed up",
                         "optimieren", "empfehlung", "schneller", "langsam"))
        {
            return Intent.TuningAdvice;
        }

        // Explain anomaly / error
        if (ContainsAny(lower, "explain", "why", "what happened", "what's wrong", "root cause", "investigate",
                         "erkl\u00e4re", "warum", "was ist", "ursache"))
        {
            return Intent.ExplainAnomaly;
        }

        return Intent.General;
    }

    private static bool ContainsAny(string text, params string[] keywords)
    {
        foreach (var keyword in keywords)
        {
            if (text.Contains(keyword, StringComparison.Ordinal))
                return true;
        }
        return false;
    }

    private async Task<AssistantMessage> HandleHealthCheckAsync(CancellationToken ct)
    {
        var snapshot = await _metricsClient.GetMetricsAsync(ct);
        var anomalies = await _metricsAnalyzer.AnalyzeAsync(snapshot, _settings.AnomalySensitivity);

        var sb = new StringBuilder();

        if (anomalies.Count == 0)
        {
            sb.AppendLine("**Cluster Health: Healthy**");
            sb.AppendLine();
            sb.AppendLine("No anomalies detected. Here's the current snapshot:");
        }
        else
        {
            var criticalCount = anomalies.Count(a => a.Severity == "Critical");
            var warningCount = anomalies.Count(a => a.Severity == "Warning");

            sb.AppendLine(criticalCount > 0
                ? "**Cluster Health: Degraded**"
                : "**Cluster Health: Warning**");
            sb.AppendLine();
            sb.AppendLine($"Detected **{anomalies.Count}** anomalies ({criticalCount} critical, {warningCount} warning):");
            sb.AppendLine();

            foreach (var anomaly in anomalies)
            {
                var icon = anomaly.Severity switch
                {
                    "Critical" => "[CRITICAL]",
                    "Warning" => "[WARNING]",
                    _ => "[INFO]"
                };
                sb.AppendLine($"- {icon} **{anomaly.Type}** on `{anomaly.Resource}`: {anomaly.Description}");
            }
        }

        sb.AppendLine();
        sb.AppendLine($"- Topics: {snapshot.TopicCount} | Partitions: {snapshot.PartitionCount}");
        sb.AppendLine($"- Active connections: {snapshot.ActiveConnections:N0}");
        sb.AppendLine($"- Consumer groups: {snapshot.ActiveConsumerGroups} | Max lag: {snapshot.MaxConsumerLag:N0}");
        sb.AppendLine($"- Produce latency P50/P99: {snapshot.ProduceLatencyP50:F1}ms / {snapshot.ProduceLatencyP99:F1}ms");
        sb.AppendLine($"- Errors: {snapshot.ErrorsTotal:N0} | Requests: {snapshot.RequestsTotal:N0}");

        return new AssistantMessage
        {
            Role = "assistant",
            Content = sb.ToString(),
            Anomalies = anomalies
        };
    }

    private async Task<AssistantMessage> HandleTuningAdviceAsync(CancellationToken ct)
    {
        var snapshot = await _metricsClient.GetMetricsAsync(ct);
        var anomalies = await _metricsAnalyzer.AnalyzeAsync(snapshot, _settings.AnomalySensitivity);
        var recommendations = _tuningAdvisor.GetRecommendations(snapshot, anomalies);

        var sb = new StringBuilder();

        if (recommendations.Count == 0)
        {
            sb.AppendLine("**No tuning recommendations at this time.**");
            sb.AppendLine();
            sb.AppendLine("The cluster appears to be well-configured for the current workload.");
        }
        else
        {
            sb.AppendLine($"**{recommendations.Count} Tuning Recommendations**");
            sb.AppendLine();

            foreach (var rec in recommendations.OrderByDescending(r => r.Impact switch
            {
                "High" => 3,
                "Medium" => 2,
                _ => 1
            }))
            {
                sb.AppendLine($"### [{rec.Impact}] {rec.Title}");
                sb.AppendLine(rec.Description);

                if (rec.ConfigKey is not null)
                {
                    sb.Append($"- Config: `{rec.ConfigKey}`");
                    if (rec.CurrentValue is not null)
                        sb.Append($" (current: {rec.CurrentValue})");
                    if (rec.SuggestedValue is not null)
                        sb.Append($" -> suggested: **{rec.SuggestedValue}**");
                    sb.AppendLine();
                }

                sb.AppendLine();
            }
        }

        return new AssistantMessage
        {
            Role = "assistant",
            Content = sb.ToString(),
            Anomalies = anomalies,
            Recommendations = recommendations
        };
    }

    private async Task<AssistantMessage> HandleSqlQueryAsync(string question, CancellationToken ct)
    {
        if (!_settings.NlToSqlEnabled)
        {
            return new AssistantMessage
            {
                Role = "assistant",
                Content = "Natural-language to SQL translation is disabled in settings."
            };
        }

        var result = await _nlToSql.TranslateAsync(question, ct);

        var sb = new StringBuilder();

        if (result.Confidence <= 0 || !string.IsNullOrEmpty(result.Error))
        {
            sb.AppendLine("I couldn't translate that into a SQL query.");
            if (result.Error is not null)
                sb.AppendLine(result.Error);
        }
        else
        {
            sb.AppendLine("**Generated SQL:**");
            sb.AppendLine($"```sql");
            sb.AppendLine(result.GeneratedSql);
            sb.AppendLine($"```");
            sb.AppendLine();

            if (result.Confidence < 0.8)
            {
                sb.AppendLine($"*Confidence: {result.Confidence:P0} — please verify before executing.*");
            }
            else
            {
                sb.AppendLine("You can copy this query to the SQL editor to execute it.");
            }
        }

        return new AssistantMessage
        {
            Role = "assistant",
            Content = sb.ToString(),
            GeneratedSql = result.Confidence > 0 ? result.GeneratedSql : null
        };
    }

    private async Task<AssistantMessage> HandleExplainAnomalyAsync(string question, CancellationToken ct)
    {
        var snapshot = await _metricsClient.GetMetricsAsync(ct);
        var anomalies = await _metricsAnalyzer.AnalyzeAsync(snapshot, _settings.AnomalySensitivity);

        var sb = new StringBuilder();

        if (anomalies.Count == 0)
        {
            sb.AppendLine("**No current anomalies to explain.**");
            sb.AppendLine();
            sb.AppendLine("All metrics are within normal ranges. If you're investigating a past issue, " +
                          "check the alerts dashboard for historical events.");
        }
        else
        {
            sb.AppendLine($"**Anomaly Analysis ({anomalies.Count} detected)**");
            sb.AppendLine();

            foreach (var anomaly in anomalies)
            {
                sb.AppendLine($"#### {anomaly.Type} — {anomaly.Severity}");
                sb.AppendLine($"**Resource:** `{anomaly.Resource}`");
                sb.AppendLine($"**What:** {anomaly.Description}");
                sb.AppendLine($"**Deviation:** {anomaly.DeviationPercent:F1}% from baseline");
                sb.AppendLine();

                // Add contextual explanation
                var explanation = anomaly.Type switch
                {
                    "ThroughputDrop" =>
                        "**Possible causes:** Producer failures, network issues, broker overload, or topic deletion. " +
                        "Check producer logs and broker connectivity.",
                    "LatencySpike" =>
                        "**Possible causes:** Disk I/O saturation, garbage collection pauses, network congestion, " +
                        "or insufficient partition count for the workload.",
                    "ErrorRateHigh" =>
                        "**Possible causes:** Broker unavailability, authorization failures, invalid message format, " +
                        "or exceeding quota limits.",
                    "ConsumerLagGrowing" =>
                        "**Possible causes:** Consumer processing too slowly, too few consumer instances, " +
                        "consumer rebalancing, or downstream service bottleneck.",
                    "ConnectionSaturation" =>
                        "**Possible causes:** Connection leak in clients, too many idle connections, or " +
                        "insufficient broker resources to handle the connection count.",
                    _ => "Review the affected resource and recent changes for root cause."
                };
                sb.AppendLine(explanation);
                sb.AppendLine();
            }

            // If LLM is available, offer deeper analysis
            if (_settings.LlmEnabled)
            {
                sb.AppendLine("*Tip: I can provide deeper analysis with LLM — ask me to \"analyze\" a specific anomaly.*");
            }
        }

        return new AssistantMessage
        {
            Role = "assistant",
            Content = sb.ToString(),
            Anomalies = anomalies
        };
    }

    private static AssistantMessage HandleIntentConfig(string question)
    {
        // Extract the intent description from the question
        var lower = question.ToLowerInvariant();
        var description = question;

        // Strip common prefixes to extract the actual intent
        string[] prefixes =
        [
            "create a topic for ", "create topic for ", "i need a topic for ",
            "set up a topic for ", "configure a topic for ", "new topic for ",
            "erstelle einen topic für ", "erstelle topic für ", "erstelle ein topic für ",
            "ich brauche einen topic für ", "topic anlegen für ", "topic einrichten für "
        ];

        foreach (var prefix in prefixes)
        {
            if (lower.StartsWith(prefix, StringComparison.Ordinal))
            {
                description = question[prefix.Length..].Trim();
                break;
            }
        }

        // Detect likely intent categories from keywords
        var detectedIntents = new List<string>();
        if (ContainsAny(lower, "high-availability", "ha", "reliable", "ausfallsicher", "hochverfügbar"))
            detectedIntents.Add("High Availability (replication 3, min ISR 2, all acks)");
        if (ContainsAny(lower, "gdpr", "dsgvo", "compliance", "datenschutz", "pii", "privacy", "personenbezogen"))
            detectedIntents.Add("GDPR Compliance (30-day TTL, DLQ enabled)");
        if (ContainsAny(lower, "iot", "sensor", "edge", "device", "telemetry", "telemetrie"))
            detectedIntents.Add("IoT/Edge (LZ4 compression, 7-day TTL)");
        if (ContainsAny(lower, "payment", "financial", "bank", "transaction", "order", "zahlung"))
            detectedIntents.Add("Financial (HA + deduplication + all acks)");
        if (ContainsAny(lower, "low-latency", "realtime", "echtzeit", "instant"))
            detectedIntents.Add("Low Latency (single partition, ack=1, no linger)");
        if (ContainsAny(lower, "high-throughput", "bulk", "batch", "fast", "schnell"))
            detectedIntents.Add("High Throughput (12 partitions, LZ4 compression, batching)");
        if (ContainsAny(lower, "event-sourcing", "event-store", "audit", "immutable", "ledger"))
            detectedIntents.Add("Event Sourcing (infinite retention, append-only)");
        if (ContainsAny(lower, "temporary", "temp", "ephemeral", "test", "debug"))
            detectedIntents.Add("Temporary (1h retention, no replication)");
        if (ContainsAny(lower, "analytics", "data-lake", "reporting", "warehouse"))
            detectedIntents.Add("Analytics (compacted, infinite retention)");
        if (ContainsAny(lower, "logging", "protokoll", "syslog", "application-log"))
            detectedIntents.Add("Logging (6 partitions, Zstd compression, 7-day retention)");

        var sb = new StringBuilder();
        sb.AppendLine("**Intent-Based Topic Configuration**");
        sb.AppendLine();

        if (detectedIntents.Count > 0)
        {
            sb.AppendLine($"I detected the following intents from \"{description}\":");
            sb.AppendLine();
            foreach (var intent in detectedIntents)
            {
                sb.AppendLine($"- {intent}");
            }
            sb.AppendLine();
            sb.AppendLine("These rules will be combined to generate the optimal configuration.");
        }
        else
        {
            sb.AppendLine("I couldn't detect a specific use case from your description. " +
                          "Try keywords like: **high-availability**, **GDPR**, **IoT**, **payment**, **low-latency**, **analytics**, **logging**.");
        }

        sb.AppendLine();
        sb.AppendLine("**Next steps:**");
        sb.AppendLine("- Open the **[Intent Topic Creator](/topics/intent)** for a visual preview and one-click creation");
        sb.AppendLine("- Or use the REST API: `POST /api/intent/resolve` to preview, `POST /api/intent/create` to create");

        return new AssistantMessage
        {
            Role = "assistant",
            Content = sb.ToString()
        };
    }

    private static AssistantMessage HandleGeneralHelp(string question)
    {
        var sb = new StringBuilder();
        sb.AppendLine("**Surgewave Assistant** — I can help you with:");
        sb.AppendLine();
        sb.AppendLine("- **Health Check** — \"How is my cluster doing?\" / \"Check health\"");
        sb.AppendLine("- **Tuning** — \"How can I improve performance?\" / \"Give me recommendations\"");
        sb.AppendLine("- **Query Data** — \"Show messages from my-topic\" / \"Count messages in orders\"");
        sb.AppendLine("- **Explain Issues** — \"Why is latency high?\" / \"Explain the current anomalies\"");
        sb.AppendLine("- **Create Topics** — \"Create a topic for payment processing\" / \"Erstelle einen Topic für IoT Sensoren\"");
        sb.AppendLine();
        sb.AppendLine("Try asking one of these, or describe what you need help with.");

        return new AssistantMessage
        {
            Role = "assistant",
            Content = sb.ToString()
        };
    }

    private enum Intent
    {
        HealthCheck,
        TuningAdvice,
        SqlQuery,
        ExplainAnomaly,
        IntentConfig,
        General
    }
}
