using Kuestenlogik.Surgewave.Broker.Audit;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for the audit logging functionality.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class AuditLoggerTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;
    private readonly string _testLogDir;

    public AuditLoggerTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddProvider(new XunitLoggerProvider(output));
            builder.SetMinimumLevel(LogLevel.Debug);
        });
        _testLogDir = Path.Combine(Path.GetTempPath(), $"surgewave-audit-test-{Guid.NewGuid():N}");
    }

    public ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_testLogDir);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _loggerFactory.Dispose();
        if (Directory.Exists(_testLogDir))
        {
            Directory.Delete(_testLogDir, true);
        }
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void AuditEventType_HasAllExpectedValues()
    {
        // Verify all event types are defined
        Assert.True(Enum.IsDefined(AuditEventType.TopicCreated));
        Assert.True(Enum.IsDefined(AuditEventType.TopicDeleted));
        Assert.True(Enum.IsDefined(AuditEventType.AclCreated));
        Assert.True(Enum.IsDefined(AuditEventType.AclDeleted));
        Assert.True(Enum.IsDefined(AuditEventType.AuthenticationSuccess));
        Assert.True(Enum.IsDefined(AuditEventType.AuthenticationFailed));
        Assert.True(Enum.IsDefined(AuditEventType.ConfigChanged));
        Assert.True(Enum.IsDefined(AuditEventType.ConnectorCreated));
        Assert.True(Enum.IsDefined(AuditEventType.SchemaRegistered));
    }

    [Fact]
    public void AuditEvent_HasRequiredProperties()
    {
        // Arrange & Act
        var auditEvent = new AuditEvent
        {
            EventId = "test-event-123",
            EventType = AuditEventType.TopicCreated,
            Principal = "User:admin",
            ClientAddress = "192.168.1.100",
            ClientId = "test-client",
            BrokerId = 0,
            ResourceType = "topic",
            ResourceName = "test-topic",
            Success = true,
            Details = new Dictionary<string, string>
            {
                ["partitions"] = "3",
                ["replicationFactor"] = "1"
            }
        };

        // Assert
        Assert.Equal("test-event-123", auditEvent.EventId);
        Assert.Equal(AuditEventType.TopicCreated, auditEvent.EventType);
        Assert.Equal("User:admin", auditEvent.Principal);
        Assert.Equal("topic", auditEvent.ResourceType);
        Assert.Equal("test-topic", auditEvent.ResourceName);
        Assert.True(auditEvent.Success);
        Assert.True(auditEvent.Timestamp > 0);
        Assert.NotNull(auditEvent.Details);
        Assert.Equal("3", auditEvent.Details["partitions"]);
    }

    [Fact]
    public void AuditEventQuery_HasFilterProperties()
    {
        // Arrange & Act
        var query = new AuditEventQuery
        {
            StartTime = 1000,
            EndTime = 2000,
            EventType = AuditEventType.TopicCreated,
            Principal = "User:admin",
            ResourceType = "topic",
            ResourceName = "test-topic",
            Success = true,
            Limit = 50,
            Offset = 10
        };

        // Assert
        Assert.Equal(1000, query.StartTime);
        Assert.Equal(2000, query.EndTime);
        Assert.Equal(AuditEventType.TopicCreated, query.EventType);
        Assert.Equal("User:admin", query.Principal);
        Assert.Equal(50, query.Limit);
        Assert.Equal(10, query.Offset);
    }

    [Fact]
    public void AuditQueryResult_HasRequiredProperties()
    {
        // Arrange & Act
        var result = new AuditQueryResult
        {
            Events = [new AuditEvent
            {
                EventId = "evt-1",
                EventType = AuditEventType.TopicCreated
            }],
            TotalCount = 100,
            HasMore = true
        };

        // Assert
        Assert.Single(result.Events);
        Assert.Equal(100, result.TotalCount);
        Assert.True(result.HasMore);
    }

    [Fact]
    public async Task AuditLogger_DisabledByDefault()
    {
        // Arrange
        var config = new BrokerConfig
        {
            LogDirectory = _testLogDir,
            Audit = new AuditConfig { Enabled = false }
        };
        var logger = _loggerFactory.CreateLogger<AuditLogger>();

        // Act
        await using var auditLogger = new AuditLogger(config, logger);
        await auditLogger.InitializeAsync();

        // Assert
        Assert.False(auditLogger.IsEnabled);
    }

    [Fact]
    public async Task AuditLogger_EnabledWhenConfigured()
    {
        // Arrange
        var config = new BrokerConfig
        {
            LogDirectory = _testLogDir,
            Audit = new AuditConfig { Enabled = true }
        };
        var logger = _loggerFactory.CreateLogger<AuditLogger>();

        // Act
        await using var auditLogger = new AuditLogger(config, logger);
        await auditLogger.InitializeAsync();

        // Assert
        Assert.True(auditLogger.IsEnabled);
    }

    [Fact]
    public async Task AuditLogger_LogsTopicEvent()
    {
        // Arrange
        var config = new BrokerConfig
        {
            BrokerId = 1,
            LogDirectory = _testLogDir,
            Audit = new AuditConfig { Enabled = true }
        };
        var logger = _loggerFactory.CreateLogger<AuditLogger>();

        await using var auditLogger = new AuditLogger(config, logger);
        await auditLogger.InitializeAsync();

        // Act
        auditLogger.LogTopicEvent(
            AuditEventType.TopicCreated,
            "test-topic",
            "User:admin",
            "192.168.1.100",
            "test-client",
            success: true,
            details: new Dictionary<string, string> { ["partitions"] = "3" });

        // Wait for event to be processed with polling for CI reliability
        AuditQueryResult? result = null;
        for (int attempt = 0; attempt < 10; attempt++)
        {
            await Task.Delay(500);
            result = auditLogger.QueryRecent(new AuditEventQuery { Limit = 10 });
            if (result.Events.Count > 0)
                break;
        }

        // Assert
        Assert.NotNull(result);
        Assert.True(result.Events.Count > 0, "Expected at least one event");
        var evt = result.Events[0];
        Assert.Equal(AuditEventType.TopicCreated, evt.EventType);
        Assert.Equal("test-topic", evt.ResourceName);
        Assert.Equal("User:admin", evt.Principal);
    }

    [Fact]
    public async Task AuditLogger_LogsAuthenticationEvent()
    {
        // Arrange
        var config = new BrokerConfig
        {
            LogDirectory = _testLogDir,
            Audit = new AuditConfig
            {
                Enabled = true,
                LogSuccessfulAuthentication = true
            }
        };
        var logger = _loggerFactory.CreateLogger<AuditLogger>();

        await using var auditLogger = new AuditLogger(config, logger);
        await auditLogger.InitializeAsync();

        // Act
        auditLogger.LogAuthenticationEvent(
            AuditEventType.AuthenticationSuccess,
            "testuser",
            "10.0.0.1",
            "PLAIN",
            success: true);

        // Wait for event to be processed with polling for CI reliability
        AuditQueryResult? result = null;
        for (int attempt = 0; attempt < 10; attempt++)
        {
            await Task.Delay(500);
            result = auditLogger.QueryRecent(new AuditEventQuery
            {
                EventType = AuditEventType.AuthenticationSuccess
            });
            if (result.Events.Count > 0)
                break;
        }

        // Assert
        Assert.NotNull(result);
        Assert.True(result.Events.Count > 0);
        var evt = result.Events[0];
        Assert.Equal("testuser", evt.Principal);
        Assert.Equal("10.0.0.1", evt.ClientAddress);
    }

    [Fact]
    public async Task AuditLogger_LogsAclEvent()
    {
        // Arrange
        var config = new BrokerConfig
        {
            LogDirectory = _testLogDir,
            Audit = new AuditConfig { Enabled = true }
        };
        var logger = _loggerFactory.CreateLogger<AuditLogger>();

        await using var auditLogger = new AuditLogger(config, logger);
        await auditLogger.InitializeAsync();

        // Act
        auditLogger.LogAclEvent(
            AuditEventType.AclCreated,
            "Topic",
            "my-topic",
            "User:admin",
            "192.168.1.1",
            success: true,
            details: new Dictionary<string, string>
            {
                ["principal"] = "User:reader",
                ["operation"] = "Read",
                ["permission"] = "Allow"
            });

        // Wait for event to be processed with polling for CI reliability
        AuditQueryResult? result = null;
        for (int attempt = 0; attempt < 10; attempt++)
        {
            await Task.Delay(500);
            result = auditLogger.QueryRecent(new AuditEventQuery
            {
                EventType = AuditEventType.AclCreated
            });
            if (result.Events.Count > 0)
                break;
        }

        // Assert
        Assert.NotNull(result);
        Assert.True(result.Events.Count > 0);
        var evt = result.Events[0];
        Assert.Equal("Topic", evt.ResourceType);
        Assert.Equal("my-topic", evt.ResourceName);
    }

    [Fact]
    public async Task AuditLogger_ExcludesInternalTopics()
    {
        // Arrange
        var config = new BrokerConfig
        {
            LogDirectory = _testLogDir,
            Audit = new AuditConfig
            {
                Enabled = true,
                ExcludeInternalTopics = true
            }
        };
        var logger = _loggerFactory.CreateLogger<AuditLogger>();

        await using var auditLogger = new AuditLogger(config, logger);
        await auditLogger.InitializeAsync();

        // Act
        auditLogger.LogTopicEvent(
            AuditEventType.TopicCreated,
            "__consumer_offsets",
            null,
            null,
            null);

        auditLogger.LogTopicEvent(
            AuditEventType.TopicCreated,
            "user-topic",
            null,
            null,
            null);

        // Wait for events to be processed with polling for CI reliability
        AuditQueryResult? result = null;
        for (int attempt = 0; attempt < 10; attempt++)
        {
            await Task.Delay(500);
            result = auditLogger.QueryRecent(new AuditEventQuery { Limit = 10 });
            if (result.Events.Any(e => e.ResourceName == "user-topic"))
                break;
        }

        // Assert - internal topic should be excluded
        Assert.NotNull(result);
        Assert.DoesNotContain(result.Events, e => e.ResourceName == "__consumer_offsets");
        Assert.Contains(result.Events, e => e.ResourceName == "user-topic");
    }

    [Fact]
    public async Task AuditLogger_FiltersByEventType()
    {
        // Arrange
        var config = new BrokerConfig
        {
            LogDirectory = _testLogDir,
            Audit = new AuditConfig
            {
                Enabled = true,
                IncludeEventTypes = [AuditEventType.TopicCreated, AuditEventType.TopicDeleted]
            }
        };
        var logger = _loggerFactory.CreateLogger<AuditLogger>();

        await using var auditLogger = new AuditLogger(config, logger);
        await auditLogger.InitializeAsync();

        // Act - log events of different types
        auditLogger.LogTopicEvent(AuditEventType.TopicCreated, "topic1", null, null, null);
        auditLogger.LogAclEvent(AuditEventType.AclCreated, "Topic", "topic1", null, null);

        // Wait for events to be processed with polling for CI reliability
        AuditQueryResult? result = null;
        for (int attempt = 0; attempt < 10; attempt++)
        {
            await Task.Delay(500);
            result = auditLogger.QueryRecent(new AuditEventQuery { Limit = 10 });
            if (result.Events.Count > 0)
                break;
        }

        // Assert - only TopicCreated should be logged (AclCreated is filtered out)
        Assert.NotNull(result);
        Assert.All(result.Events, e => Assert.True(
            e.EventType == AuditEventType.TopicCreated ||
            e.EventType == AuditEventType.TopicDeleted));
    }

    [Fact]
    public async Task AuditLogger_QueryRecentWithFilters()
    {
        // Arrange
        var config = new BrokerConfig
        {
            LogDirectory = _testLogDir,
            Audit = new AuditConfig { Enabled = true }
        };
        var logger = _loggerFactory.CreateLogger<AuditLogger>();

        await using var auditLogger = new AuditLogger(config, logger);
        await auditLogger.InitializeAsync();

        // Act - log multiple events
        auditLogger.LogTopicEvent(AuditEventType.TopicCreated, "topic-a", "User:admin", null, null);
        auditLogger.LogTopicEvent(AuditEventType.TopicCreated, "topic-b", "User:dev", null, null);
        auditLogger.LogTopicEvent(AuditEventType.TopicDeleted, "topic-c", "User:admin", null, null);

        // Wait for events to be processed with polling for CI reliability
        AuditQueryResult? allEvents = null;
        for (int attempt = 0; attempt < 10; attempt++)
        {
            await Task.Delay(500);
            allEvents = auditLogger.QueryRecent(new AuditEventQuery { Limit = 10 });
            if (allEvents.Events.Count >= 3)
                break;
        }

        // Assert - filter by principal
        var adminEvents = auditLogger.QueryRecent(new AuditEventQuery
        {
            Principal = "User:admin"
        });
        Assert.Equal(2, adminEvents.Events.Count);

        // Assert - filter by event type
        var deleteEvents = auditLogger.QueryRecent(new AuditEventQuery
        {
            EventType = AuditEventType.TopicDeleted
        });
        Assert.Single(deleteEvents.Events);
    }

    [Fact]
    public void AuditConfig_HasDefaultValues()
    {
        // Arrange & Act
        var config = new AuditConfig();

        // Assert
        Assert.False(config.Enabled);
        Assert.Equal(1, config.Partitions);
        Assert.Equal(1, config.ReplicationFactor);
        Assert.Equal(7 * 24 * 60 * 60 * 1000L, config.RetentionMs);
        Assert.True(config.ExcludeInternalTopics);
        Assert.False(config.LogSuccessfulAuthentication);
        Assert.False(config.LogAuthorizationChecks);
    }

    [Fact]
    public void AuditQueryResultResponse_HasRequiredProperties()
    {
        // Arrange & Act
        var response = new AuditQueryResultResponse
        {
            Events = [new AuditEventResponse
            {
                EventId = "test-id",
                EventType = "TopicCreated",
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                BrokerId = 0,
                ResourceType = "topic",
                ResourceName = "test-topic",
                Success = true
            }],
            TotalCount = 1,
            HasMore = false,
            Message = null
        };

        // Assert
        Assert.Single(response.Events);
        Assert.Equal("test-id", response.Events[0].EventId);
        Assert.Equal("TopicCreated", response.Events[0].EventType);
        Assert.Equal(1, response.TotalCount);
        Assert.False(response.HasMore);
    }

    [Fact]
    public void AuditStatsResponse_HasRequiredProperties()
    {
        // Arrange & Act
        var stats = new AuditStatsResponse
        {
            Enabled = true,
            BrokerId = 1,
            RecentEventCount = 50
        };

        // Assert
        Assert.True(stats.Enabled);
        Assert.Equal(1, stats.BrokerId);
        Assert.Equal(50, stats.RecentEventCount);
    }

    [Fact]
    public void AuditConfigResponse_HasRequiredProperties()
    {
        // Arrange & Act
        var configResponse = new AuditConfigResponse
        {
            Enabled = true,
            Partitions = 3,
            ReplicationFactor = 2,
            RetentionMs = 604800000,
            ExcludeInternalTopics = true,
            LogSuccessfulAuthentication = false,
            LogAuthorizationChecks = true,
            IncludeEventTypes = ["TopicCreated", "TopicDeleted"],
            ExcludeEventTypes = []
        };

        // Assert
        Assert.True(configResponse.Enabled);
        Assert.Equal(3, configResponse.Partitions);
        Assert.Equal(2, configResponse.ReplicationFactor);
        Assert.Equal(604800000, configResponse.RetentionMs);
        Assert.True(configResponse.ExcludeInternalTopics);
        Assert.False(configResponse.LogSuccessfulAuthentication);
        Assert.True(configResponse.LogAuthorizationChecks);
        Assert.Equal(2, configResponse.IncludeEventTypes.Count);
        Assert.Empty(configResponse.ExcludeEventTypes);
    }

    [Fact]
    public void AuditEventResponse_MapsAllFields()
    {
        // Arrange & Act
        var eventResponse = new AuditEventResponse
        {
            EventId = "evt-123",
            EventType = "AclCreated",
            Timestamp = 1704067200000,
            Principal = "User:admin",
            ClientAddress = "192.168.1.100",
            ClientId = "test-client",
            BrokerId = 2,
            ResourceType = "Topic",
            ResourceName = "my-topic",
            Success = true,
            ErrorMessage = null,
            Details = new Dictionary<string, string> { ["operation"] = "Read" },
            CorrelationId = "corr-456"
        };

        // Assert
        Assert.Equal("evt-123", eventResponse.EventId);
        Assert.Equal("AclCreated", eventResponse.EventType);
        Assert.Equal(1704067200000, eventResponse.Timestamp);
        Assert.Equal("User:admin", eventResponse.Principal);
        Assert.Equal("192.168.1.100", eventResponse.ClientAddress);
        Assert.Equal("test-client", eventResponse.ClientId);
        Assert.Equal(2, eventResponse.BrokerId);
        Assert.Equal("Topic", eventResponse.ResourceType);
        Assert.Equal("my-topic", eventResponse.ResourceName);
        Assert.True(eventResponse.Success);
        Assert.Null(eventResponse.ErrorMessage);
        Assert.NotNull(eventResponse.Details);
        Assert.Equal("Read", eventResponse.Details["operation"]);
        Assert.Equal("corr-456", eventResponse.CorrelationId);
    }
}

/// <summary>
/// Logger provider that writes to xunit test output.
/// </summary>
file sealed class XunitLoggerProvider(ITestOutputHelper output) : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new XunitLogger(output, categoryName);
    public void Dispose() { }
}

file sealed class XunitLogger(ITestOutputHelper output, string categoryName) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        output.WriteLine($"[{logLevel}] {categoryName}: {formatter(state, exception)}");
        if (exception != null)
            output.WriteLine(exception.ToString());
    }
}
