using Kuestenlogik.Surgewave.Broker.IntentConfig;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for the Intent-Based Configuration engine.
/// Verifies keyword matching, rule stacking, context-aware adjustments,
/// and German keyword support.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class IntentConfigTests
{
    private readonly IntentConfigEngine _engine = new();

    [Fact]
    public void Resolve_HighAvailability()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "high-availability" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal(3, result.ReplicationFactor);
        Assert.Equal("2", result.TopicConfig["min.insync.replicas"]);
        Assert.Equal("all", result.TopicConfig["acks"]);
        Assert.True(result.AppliedRules.Count > 0);
    }

    [Fact]
    public void Resolve_LowLatency()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "low-latency realtime processing" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal(1, result.Partitions);
        Assert.Equal("1", result.TopicConfig["acks"]);
        Assert.Equal("0", result.TopicConfig["linger.ms"]);
    }

    [Fact]
    public void Resolve_GdprCompliance()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "GDPR compliance for user data" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal("true", result.TopicConfig["surgewave.ttl.enabled"]);
        Assert.Equal("2592000000", result.TopicConfig["surgewave.ttl.default-ms"]); // 30 days
        Assert.Equal("true", result.TopicConfig["surgewave.dlq.enabled"]);
    }

    [Fact]
    public void Resolve_IoT()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "IoT sensor data collection" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal("lz4", result.TopicConfig["compression.type"]);
        Assert.Equal("true", result.TopicConfig["surgewave.ttl.enabled"]);
        Assert.Equal("604800000", result.TopicConfig["surgewave.ttl.default-ms"]); // 7 days
    }

    [Fact]
    public void Resolve_Temporary()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "temporary test topic" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal(1, result.ReplicationFactor);
        Assert.Equal("3600000", result.TopicConfig["retention.ms"]); // 1 hour
        Assert.Equal("delete", result.TopicConfig["cleanup.policy"]);
    }

    [Fact]
    public void Resolve_MultipleKeywords_CombinesRules()
    {
        // Arrange - both HA and GDPR keywords
        var intent = new ConfigIntent { Description = "high-availability GDPR compliant payment system" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        // HA rule: replication 3, min ISR 2, acks=all
        Assert.Equal(3, result.ReplicationFactor);
        Assert.Equal("2", result.TopicConfig["min.insync.replicas"]);
        Assert.Equal("all", result.TopicConfig["acks"]);
        // GDPR rule: TTL, DLQ
        Assert.Equal("true", result.TopicConfig["surgewave.ttl.enabled"]);
        Assert.Equal("true", result.TopicConfig["surgewave.dlq.enabled"]);
        // Financial rule: dedup
        Assert.Equal("true", result.TopicConfig["surgewave.dedup.enabled"]);
        // Multiple rules applied
        Assert.True(result.AppliedRules.Count > 3);
    }

    [Fact]
    public void Resolve_ContextDeviceCount_AdjustsPartitions()
    {
        // Arrange
        var intent = new ConfigIntent
        {
            Description = "IoT sensor data",
            Context = new IntentContext { ExpectedDeviceCount = 500 }
        };

        // Act
        var result = _engine.Resolve(intent);

        // Assert - 500 devices should trigger 6 partitions
        Assert.True(result.Partitions >= 6);
        Assert.Contains(result.AppliedRules, r => r.RuleName == "context-device-count");
    }

    [Fact]
    public void Resolve_ContextDeviceCount_Large_AdjustsPartitions()
    {
        // Arrange
        var intent = new ConfigIntent
        {
            Description = "IoT sensor data",
            Context = new IntentContext { ExpectedDeviceCount = 5000 }
        };

        // Act
        var result = _engine.Resolve(intent);

        // Assert - 5000 devices should trigger 12 partitions
        Assert.True(result.Partitions >= 12);
    }

    [Fact]
    public void Resolve_ProductionEnvironment_IncreasesReplication()
    {
        // Arrange
        var intent = new ConfigIntent
        {
            Description = "simple logging topic",
            Context = new IntentContext { Environment = "production" }
        };

        // Act
        var result = _engine.Resolve(intent);

        // Assert - production environment enforces HA settings
        Assert.Equal(3, result.ReplicationFactor);
        Assert.Equal("2", result.TopicConfig["min.insync.replicas"]);
        Assert.Equal("all", result.TopicConfig["acks"]);
        Assert.Contains(result.AppliedRules, r => r.RuleName == "context-production");
    }

    [Fact]
    public void Resolve_UnknownKeywords_LowConfidence()
    {
        // Arrange - no recognizable keywords
        var intent = new ConfigIntent { Description = "something completely unrelated xyz123" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert - low confidence, default config
        Assert.Equal(0.3, result.Confidence);
        Assert.Equal(1, result.Partitions);
        Assert.Equal(1, result.ReplicationFactor);
    }

    [Fact]
    public void GetAvailableKeywords_ReturnsAll()
    {
        // Act
        var keywords = _engine.GetAvailableKeywords();

        // Assert
        Assert.True(keywords.Count > 20);
        Assert.Contains("high-availability", keywords);
        Assert.Contains("gdpr", keywords);
        Assert.Contains("iot", keywords);
        Assert.Contains("low-latency", keywords);
        // German keywords
        Assert.Contains("echtzeit", keywords);
        Assert.Contains("dsgvo", keywords);
        Assert.Contains("hochverfügbar", keywords);
    }

    [Fact]
    public void Resolve_GermanKeywords()
    {
        // Arrange - German keywords for GDPR
        var intent = new ConfigIntent { Description = "Datenschutz-konformer Topic für personenbezogene Daten" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert - GDPR rules should match on German keywords
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal("true", result.TopicConfig["surgewave.ttl.enabled"]);
        Assert.Equal("true", result.TopicConfig["surgewave.dlq.enabled"]);
    }

    [Fact]
    public void Resolve_GermanKeywords_HighAvailability()
    {
        // Arrange - German keywords for HA
        var intent = new ConfigIntent { Description = "ausfallsicherer Topic" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal(3, result.ReplicationFactor);
        Assert.Equal("all", result.TopicConfig["acks"]);
    }

    [Fact]
    public void Resolve_GermanKeywords_Realtime()
    {
        // Arrange - German keyword for low-latency
        var intent = new ConfigIntent { Description = "echtzeit Datenverarbeitung" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal("0", result.TopicConfig["linger.ms"]);
    }

    [Fact]
    public void Resolve_Financial()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "payment processing system" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal(3, result.ReplicationFactor);
        Assert.Equal("all", result.TopicConfig["acks"]);
        Assert.Equal("true", result.TopicConfig["surgewave.dedup.enabled"]);
    }

    [Fact]
    public void Resolve_EventSourcing()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "event-sourcing for order events" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal("-1", result.TopicConfig["retention.ms"]); // infinite
    }

    [Fact]
    public void Resolve_Analytics()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "analytics data-lake integration" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal("compact", result.TopicConfig["cleanup.policy"]);
        Assert.Equal("-1", result.TopicConfig["retention.ms"]);
    }

    [Fact]
    public void Resolve_TopicName_Provided()
    {
        // Arrange
        var intent = new ConfigIntent
        {
            Description = "high-availability",
            TopicName = "my-custom-topic"
        };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal("my-custom-topic", result.TopicName);
    }

    [Fact]
    public void Resolve_TopicName_AutoGenerated()
    {
        // Arrange - no topic name provided
        var intent = new ConfigIntent { Description = "high-availability" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert - auto-generated based on rule name
        Assert.NotNull(result.TopicName);
        Assert.NotEmpty(result.TopicName);
    }

    [Fact]
    public void Resolve_BrokerCount_CapsReplicationFactor()
    {
        // Arrange - HA wants replication 3 but only 2 brokers
        var intent = new ConfigIntent
        {
            Description = "high-availability",
            Context = new IntentContext { BrokerCount = 2 }
        };

        // Act
        var result = _engine.Resolve(intent);

        // Assert - replication capped to 2
        Assert.Equal(2, result.ReplicationFactor);
        Assert.Contains(result.Warnings, w => w.Contains("Capped to 2"));
    }

    [Fact]
    public void Resolve_HighThroughput()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "high-throughput bulk data ingestion" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal(12, result.Partitions);
        Assert.Equal("lz4", result.TopicConfig["compression.type"]);
        Assert.Equal("65536", result.TopicConfig["batch.size"]);
    }

    [Fact]
    public void Resolve_ContextHighMessageRate()
    {
        // Arrange
        var intent = new ConfigIntent
        {
            Description = "data ingestion",
            Context = new IntentContext { ExpectedMessagesPerSec = 50000 }
        };

        // Act
        var result = _engine.Resolve(intent);

        // Assert - high throughput config applied via context
        Assert.True(result.Partitions >= 6);
        Assert.Contains(result.AppliedRules, r => r.RuleName == "context-high-throughput");
    }

    [Fact]
    public void Resolve_PiiDataClassification()
    {
        // Arrange
        var intent = new ConfigIntent
        {
            Description = "user data stream",
            Context = new IntentContext { DataClassification = "pii" }
        };

        // Act
        var result = _engine.Resolve(intent);

        // Assert - GDPR rules applied via context
        Assert.Equal("true", result.TopicConfig["surgewave.ttl.enabled"]);
        Assert.Equal("true", result.TopicConfig["surgewave.dlq.enabled"]);
        Assert.Contains(result.AppliedRules, r => r.RuleName == "context-pii");
    }

    [Fact]
    public void Resolve_ExplanationContainsRuleNames()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "high-availability payment system" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Contains("high-availability", result.Explanation);
        Assert.Contains("financial", result.Explanation);
    }

    [Fact]
    public void GetRules_ReturnsAllBuiltInRules()
    {
        // Act
        var rules = _engine.GetRules();

        // Assert - should have at least 12 built-in rules
        Assert.True(rules.Count >= 12);
        Assert.Contains(rules, r => r.Name == "high-availability");
        Assert.Contains(rules, r => r.Name == "low-latency");
        Assert.Contains(rules, r => r.Name == "gdpr-compliance");
        Assert.Contains(rules, r => r.Name == "iot-edge");
        Assert.Contains(rules, r => r.Name == "temporary");
        Assert.Contains(rules, r => r.Name == "event-sourcing");
        Assert.Contains(rules, r => r.Name == "financial");
        Assert.Contains(rules, r => r.Name == "logging");
        Assert.Contains(rules, r => r.Name == "analytics");
    }

    [Fact]
    public void Resolve_Logging()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "application-log collection" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal(6, result.Partitions);
        Assert.Equal("zstd", result.TopicConfig["compression.type"]);
        Assert.Equal("604800000", result.TopicConfig["retention.ms"]); // 7 days
    }

    [Fact]
    public void Resolve_WorkQueue()
    {
        // Arrange
        var intent = new ConfigIntent { Description = "background job queue for workers" };

        // Act
        var result = _engine.Resolve(intent);

        // Assert
        Assert.Equal(1.0, result.Confidence);
        Assert.Equal("all", result.TopicConfig["acks"]);
        Assert.Equal("true", result.TopicConfig["surgewave.dlq.enabled"]);
    }
}
