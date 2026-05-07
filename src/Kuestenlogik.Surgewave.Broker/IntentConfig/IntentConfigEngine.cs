using System.Text;

namespace Kuestenlogik.Surgewave.Broker.IntentConfig;

/// <summary>
/// Pattern-based intent configuration engine.
/// Resolves free-form descriptions or keywords into concrete topic/broker configuration.
/// Works entirely without LLM — uses keyword matching and context-aware rules.
/// Supports both English and German keywords.
/// </summary>
public sealed class IntentConfigEngine
{
    private readonly List<IntentRule> _rules;

    /// <summary>
    /// Creates a new IntentConfigEngine with the default built-in rules.
    /// </summary>
    public IntentConfigEngine()
    {
        _rules = GetDefaultRules();
    }

    /// <summary>
    /// Resolve an intent description to a concrete topic configuration.
    /// Multiple rules can match and their configurations are stacked/merged.
    /// Context-aware adjustments (device count, environment, etc.) are applied on top.
    /// </summary>
    /// <param name="intent">The configuration intent to resolve.</param>
    /// <returns>A resolved configuration with applied rules, confidence, and explanations.</returns>
    public IntentResult Resolve(ConfigIntent intent)
    {
        var normalized = intent.Description.Trim().ToLowerInvariant();

        // Find matching rules by keyword
        var matchedRules = new List<IntentRule>();
        foreach (var rule in _rules)
        {
            foreach (var keyword in rule.Keywords)
            {
                if (normalized.Contains(keyword, StringComparison.OrdinalIgnoreCase))
                {
                    matchedRules.Add(rule);
                    break;
                }
            }
        }

        // Calculate confidence
        var confidence = matchedRules.Count > 0 ? 1.0 : 0.3;

        // Merge configurations from all matched rules (higher priority wins on conflict)
        var mergedConfig = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var appliedRules = new List<IntentRuleMatch>();
        var partitions = 1;
        var replicationFactor = 1;

        // Sort by priority so higher-priority rules override lower ones
        var orderedRules = matchedRules
            .OrderBy(r => r.Priority)
            .ToList();

        foreach (var rule in orderedRules)
        {
            foreach (var kvp in rule.Config)
            {
                mergedConfig[kvp.Key] = kvp.Value;
                appliedRules.Add(new IntentRuleMatch(rule.Name, rule.Description, kvp.Key, kvp.Value));
            }

            if (rule.Partitions.HasValue && rule.Partitions.Value > partitions)
            {
                partitions = rule.Partitions.Value;
                appliedRules.Add(new IntentRuleMatch(rule.Name, rule.Description, "partitions", partitions.ToString()));
            }

            if (rule.ReplicationFactor.HasValue && rule.ReplicationFactor.Value > replicationFactor)
            {
                replicationFactor = rule.ReplicationFactor.Value;
                appliedRules.Add(new IntentRuleMatch(rule.Name, rule.Description, "replication.factor", replicationFactor.ToString()));
            }
        }

        // Apply context-based adjustments
        var warnings = new List<string>();
        ApplyContextRules(intent.Context, mergedConfig, ref partitions, ref replicationFactor, appliedRules, warnings);

        // Cap replication factor to broker count if known
        if (intent.Context.BrokerCount.HasValue && replicationFactor > intent.Context.BrokerCount.Value)
        {
            warnings.Add($"Replication factor {replicationFactor} exceeds broker count {intent.Context.BrokerCount.Value}. " +
                          $"Capped to {intent.Context.BrokerCount.Value}.");
            replicationFactor = intent.Context.BrokerCount.Value;
        }

        // Determine topic name
        var topicName = intent.TopicName ?? GenerateTopicName(normalized, matchedRules);

        // Build explanation
        var explanation = BuildExplanation(matchedRules, appliedRules, intent.Context);

        return new IntentResult
        {
            TopicName = topicName,
            TopicConfig = mergedConfig,
            Partitions = partitions,
            ReplicationFactor = replicationFactor,
            Explanation = explanation,
            AppliedRules = appliedRules,
            Confidence = confidence,
            Warnings = warnings
        };
    }

    /// <summary>
    /// Get all available intent keywords across all built-in rules.
    /// </summary>
    public IReadOnlyList<string> GetAvailableKeywords()
    {
        return _rules
            .SelectMany(r => r.Keywords)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(k => k, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    /// <summary>
    /// Get all built-in rules.
    /// </summary>
    public IReadOnlyList<IntentRule> GetRules() => _rules.AsReadOnly();

    private static void ApplyContextRules(
        IntentContext context,
        Dictionary<string, string> config,
        ref int partitions,
        ref int replicationFactor,
        List<IntentRuleMatch> appliedRules,
        List<string> warnings)
    {
        // High device count → increase partitions
        if (context.ExpectedDeviceCount is > 100)
        {
            var suggestedPartitions = context.ExpectedDeviceCount.Value switch
            {
                > 10000 => 24,
                > 1000 => 12,
                > 100 => 6,
                _ => partitions
            };

            if (suggestedPartitions > partitions)
            {
                partitions = suggestedPartitions;
                appliedRules.Add(new IntentRuleMatch("context-device-count",
                    $"Adjusted partitions for {context.ExpectedDeviceCount} devices",
                    "partitions", partitions.ToString()));
            }
        }

        // High message rate → high-throughput config
        if (context.ExpectedMessagesPerSec is > 10000)
        {
            if (!config.ContainsKey("compression.type"))
            {
                config["compression.type"] = "lz4";
                appliedRules.Add(new IntentRuleMatch("context-high-throughput",
                    $"LZ4 compression for {context.ExpectedMessagesPerSec} msg/s",
                    "compression.type", "lz4"));
            }

            if (!config.ContainsKey("batch.size"))
            {
                config["batch.size"] = "65536";
                appliedRules.Add(new IntentRuleMatch("context-high-throughput",
                    $"Increased batch size for {context.ExpectedMessagesPerSec} msg/s",
                    "batch.size", "65536"));
            }

            if (partitions < 6)
            {
                partitions = context.ExpectedMessagesPerSec.Value switch
                {
                    > 100000 => 24,
                    > 50000 => 12,
                    _ => 6
                };
                appliedRules.Add(new IntentRuleMatch("context-high-throughput",
                    $"Increased partitions for {context.ExpectedMessagesPerSec} msg/s",
                    "partitions", partitions.ToString()));
            }
        }

        // Large messages → adjust segment size
        if (context.ExpectedMessageSizeBytes is > 100000)
        {
            config["max.message.bytes"] = context.ExpectedMessageSizeBytes.Value.ToString();
            appliedRules.Add(new IntentRuleMatch("context-large-messages",
                $"Set max message size to {context.ExpectedMessageSizeBytes} bytes",
                "max.message.bytes", context.ExpectedMessageSizeBytes.Value.ToString()));
        }

        // Production environment → enforce HA
        if (string.Equals(context.Environment, "production", StringComparison.OrdinalIgnoreCase))
        {
            if (replicationFactor < 3)
            {
                replicationFactor = 3;
                appliedRules.Add(new IntentRuleMatch("context-production",
                    "Production environment: replication factor set to 3",
                    "replication.factor", "3"));
            }

            if (!config.ContainsKey("min.insync.replicas"))
            {
                config["min.insync.replicas"] = "2";
                appliedRules.Add(new IntentRuleMatch("context-production",
                    "Production environment: min ISR set to 2",
                    "min.insync.replicas", "2"));
            }

            if (!config.ContainsKey("acks"))
            {
                config["acks"] = "all";
                appliedRules.Add(new IntentRuleMatch("context-production",
                    "Production environment: all acks required",
                    "acks", "all"));
            }
        }

        // Dev environment → relax settings
        if (string.Equals(context.Environment, "dev", StringComparison.OrdinalIgnoreCase))
        {
            if (replicationFactor > 1)
            {
                warnings.Add($"Development environment: replication factor {replicationFactor} may be unnecessary. Consider using 1 for dev.");
            }
        }

        // PII data classification → GDPR rules
        if (string.Equals(context.DataClassification, "pii", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(context.DataClassification, "confidential", StringComparison.OrdinalIgnoreCase))
        {
            if (!config.ContainsKey("surgewave.ttl.enabled"))
            {
                config["surgewave.ttl.enabled"] = "true";
                config["surgewave.ttl.default-ms"] = "2592000000"; // 30 days
                appliedRules.Add(new IntentRuleMatch("context-pii",
                    $"Data classification '{context.DataClassification}': 30-day TTL enabled",
                    "surgewave.ttl.default-ms", "2592000000"));
            }

            if (!config.ContainsKey("surgewave.dlq.enabled"))
            {
                config["surgewave.dlq.enabled"] = "true";
                appliedRules.Add(new IntentRuleMatch("context-pii",
                    $"Data classification '{context.DataClassification}': DLQ enabled",
                    "surgewave.dlq.enabled", "true"));
            }
        }
    }

    private static string GenerateTopicName(string description, List<IntentRule> matchedRules)
    {
        // If we matched a single rule, use its name as a hint
        if (matchedRules.Count == 1)
            return matchedRules[0].Name + "-topic";

        // Extract meaningful words from description
        var words = description.Split([' ', '-', '_', ',', '.'], StringSplitOptions.RemoveEmptyEntries)
            .Where(w => w.Length > 2)
            .Take(3);

        var name = string.Join("-", words);
        return string.IsNullOrWhiteSpace(name) ? "new-topic" : name + "-topic";
    }

    private static string BuildExplanation(
        List<IntentRule> matchedRules,
        List<IntentRuleMatch> appliedRules,
        IntentContext context)
    {
        var sb = new StringBuilder();

        if (matchedRules.Count == 0)
        {
            sb.Append("No specific intent keywords matched. Default configuration applied.");
        }
        else
        {
            sb.Append("Matched rules: ");
            sb.Append(string.Join(", ", matchedRules.Select(r => r.Name)));
            sb.Append(". ");

            foreach (var rule in matchedRules)
            {
                if (!string.IsNullOrEmpty(rule.Description))
                {
                    sb.Append(rule.Description);
                    sb.Append(". ");
                }
            }
        }

        // Note context-based adjustments
        var contextRules = appliedRules.Where(r => r.RuleName.StartsWith("context-", StringComparison.Ordinal)).ToList();
        if (contextRules.Count > 0)
        {
            sb.Append("Context adjustments: ");
            sb.Append(string.Join("; ", contextRules.Select(r => r.Description)));
            sb.Append('.');
        }

        return sb.ToString().Trim();
    }

    private static List<IntentRule> GetDefaultRules() =>
    [
        // High Availability
        new IntentRule
        {
            Name = "high-availability",
            Keywords = ["high-availability", "ha", "hochverfügbar", "ausfallsicher", "reliable", "zuverlässig"],
            ReplicationFactor = 3,
            Config = new Dictionary<string, string>
            {
                ["min.insync.replicas"] = "2",
                ["acks"] = "all"
            },
            Description = "Replication factor 3, min ISR 2, all acks",
            Priority = IntentRulePriority.High
        },

        // High Throughput
        new IntentRule
        {
            Name = "high-throughput",
            Keywords = ["high-throughput", "hoher-durchsatz", "fast", "schnell", "bulk", "batch"],
            Partitions = 12,
            Config = new Dictionary<string, string>
            {
                ["compression.type"] = "lz4",
                ["batch.size"] = "65536",
                ["linger.ms"] = "5"
            },
            Description = "12 partitions, LZ4 compression, batching"
        },

        // Low Latency
        new IntentRule
        {
            Name = "low-latency",
            Keywords = ["low-latency", "niedrige-latenz", "realtime", "echtzeit", "instant"],
            Partitions = 1,
            Config = new Dictionary<string, string>
            {
                ["acks"] = "1",
                ["linger.ms"] = "0"
            },
            Description = "Single partition, ack=1, no linger"
        },

        // GDPR / Compliance
        new IntentRule
        {
            Name = "gdpr-compliance",
            Keywords = ["gdpr", "dsgvo", "compliance", "datenschutz", "pii", "privacy", "personenbezogen"],
            Config = new Dictionary<string, string>
            {
                ["surgewave.ttl.enabled"] = "true",
                ["surgewave.ttl.default-ms"] = "2592000000",
                ["surgewave.dlq.enabled"] = "true"
            },
            Description = "30-day TTL, DLQ enabled, PII guardrails recommended",
            Priority = IntentRulePriority.Critical
        },

        // IoT / Edge
        new IntentRule
        {
            Name = "iot-edge",
            Keywords = ["iot", "sensor", "edge", "device", "gerät", "telemetrie", "telemetry"],
            Config = new Dictionary<string, string>
            {
                ["compression.type"] = "lz4",
                ["surgewave.ttl.enabled"] = "true",
                ["surgewave.ttl.default-ms"] = "604800000"
            },
            Description = "LZ4 compression, 7-day TTL"
        },

        // Analytics / Data Lake
        new IntentRule
        {
            Name = "analytics",
            Keywords = ["analytics", "analyse", "data-lake", "reporting", "warehouse", "olap"],
            Config = new Dictionary<string, string>
            {
                ["cleanup.policy"] = "compact",
                ["retention.ms"] = "-1"
            },
            Description = "Compacted, infinite retention"
        },

        // Temporary / Ephemeral
        new IntentRule
        {
            Name = "temporary",
            Keywords = ["temporary", "temp", "temporär", "ephemeral", "kurzlebig", "test", "debug"],
            ReplicationFactor = 1,
            Config = new Dictionary<string, string>
            {
                ["retention.ms"] = "3600000",
                ["cleanup.policy"] = "delete"
            },
            Description = "1h retention, no replication",
            Priority = IntentRulePriority.Low
        },

        // Event Sourcing
        new IntentRule
        {
            Name = "event-sourcing",
            Keywords = ["event-sourcing", "event-store", "immutable", "audit", "log", "ledger"],
            Config = new Dictionary<string, string>
            {
                ["cleanup.policy"] = "delete",
                ["retention.ms"] = "-1"
            },
            Description = "Infinite retention, append-only"
        },

        // Financial / Payment
        new IntentRule
        {
            Name = "financial",
            Keywords = ["financial", "payment", "zahlung", "bank", "transaction", "transaktion", "order", "bestellung"],
            ReplicationFactor = 3,
            Config = new Dictionary<string, string>
            {
                ["min.insync.replicas"] = "2",
                ["acks"] = "all",
                ["surgewave.dedup.enabled"] = "true"
            },
            Description = "HA + deduplication + all acks",
            Priority = IntentRulePriority.High
        },

        // Chat / Messaging
        new IntentRule
        {
            Name = "chat-messaging",
            Keywords = ["chat", "messaging", "nachrichten", "conversation", "dialog"],
            Config = new Dictionary<string, string>
            {
                ["surgewave.ttl.enabled"] = "true",
                ["surgewave.ttl.default-ms"] = "2592000000"
            },
            Description = "30-day TTL for conversations"
        },

        // Logs
        new IntentRule
        {
            Name = "logging",
            Keywords = ["logging", "protokoll", "syslog", "application-log"],
            Partitions = 6,
            Config = new Dictionary<string, string>
            {
                ["compression.type"] = "zstd",
                ["retention.ms"] = "604800000",
                ["cleanup.policy"] = "delete"
            },
            Description = "6 partitions, Zstd compression, 7-day retention"
        },

        // Machine Learning
        new IntentRule
        {
            Name = "machine-learning",
            Keywords = ["ml", "machine-learning", "ai", "prediction", "scoring", "model", "training"],
            Config = new Dictionary<string, string>
            {
                ["compression.type"] = "lz4"
            },
            Description = "LZ4 compression for ML data"
        },

        // Metrics / Monitoring
        new IntentRule
        {
            Name = "metrics",
            Keywords = ["metrics", "monitoring", "metriken", "observability", "tracing"],
            Partitions = 6,
            Config = new Dictionary<string, string>
            {
                ["compression.type"] = "lz4",
                ["retention.ms"] = "259200000",
                ["cleanup.policy"] = "delete"
            },
            Description = "6 partitions, LZ4 compression, 3-day retention"
        },

        // CDC / Change Data Capture
        new IntentRule
        {
            Name = "cdc",
            Keywords = ["cdc", "change-data-capture", "debezium", "database-sync", "replication"],
            Config = new Dictionary<string, string>
            {
                ["cleanup.policy"] = "compact",
                ["retention.ms"] = "-1"
            },
            Description = "Compacted for CDC, infinite retention"
        },

        // Notification / Alerts
        new IntentRule
        {
            Name = "notification",
            Keywords = ["notification", "alert", "benachrichtigung", "alarm", "webhook", "push"],
            Config = new Dictionary<string, string>
            {
                ["acks"] = "1",
                ["linger.ms"] = "0",
                ["surgewave.ttl.enabled"] = "true",
                ["surgewave.ttl.default-ms"] = "86400000"
            },
            Description = "Fast delivery, 1-day TTL"
        },

        // Queue / Work Queue
        new IntentRule
        {
            Name = "work-queue",
            Keywords = ["queue", "work-queue", "warteschlange", "job", "task", "worker"],
            Config = new Dictionary<string, string>
            {
                ["acks"] = "all",
                ["surgewave.dlq.enabled"] = "true"
            },
            Description = "All acks, DLQ enabled for failed jobs"
        }
    ];
}
