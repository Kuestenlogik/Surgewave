using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Connect.Tests.Models;

/// <summary>
/// Tests for PipelineDefinition, PipelineNode, and PipelineConnection models.
/// </summary>
public sealed class PipelineModelTests
{
    [Fact]
    public void PipelineDefinition_AllProperties_AreSet()
    {
        var now = DateTimeOffset.UtcNow;

        var definition = new PipelineDefinition
        {
            Id = "pipeline-1",
            Name = "Test Pipeline",
            Description = "A test pipeline",
            Nodes = [
                new PipelineNode
                {
                    Id = "node-1",
                    ConnectorType = "FileStream",
                    Config = new Dictionary<string, string> { ["path"] = "/data" },
                    X = 100.0,
                    Y = 200.0,
                    Label = "File Source"
                }
            ],
            Connections = [
                new PipelineConnection
                {
                    Id = "conn-1",
                    SourceNodeId = "node-1",
                    TargetNodeId = "node-2"
                }
            ],
            Status = PipelineStatus.Draft,
            CreatedAt = now,
            Parameters = new Dictionary<string, string> { ["env"] = "test" }
        };

        Assert.Equal("pipeline-1", definition.Id);
        Assert.Equal("Test Pipeline", definition.Name);
        Assert.Equal("A test pipeline", definition.Description);
        Assert.Single(definition.Nodes);
        Assert.Single(definition.Connections);
        Assert.Equal(PipelineStatus.Draft, definition.Status);
        Assert.Equal(now, definition.CreatedAt);
        Assert.Null(definition.UpdatedAt);
        Assert.Null(definition.Error);
        Assert.NotNull(definition.Parameters);
        Assert.Equal("test", definition.Parameters["env"]);
        Assert.Null(definition.Schedule);
    }

    [Fact]
    public void PipelineDefinition_DefaultStatus_IsDraft()
    {
        var definition = new PipelineDefinition
        {
            Id = "default-status",
            Name = "Default",
            Nodes = [],
            Connections = []
        };

        Assert.Equal(PipelineStatus.Draft, definition.Status);
    }

    [Fact]
    public void PipelineDefinition_WithError_SetsErrorField()
    {
        var definition = new PipelineDefinition
        {
            Id = "error-pipeline",
            Name = "Failing Pipeline",
            Nodes = [],
            Connections = [],
            Status = PipelineStatus.Failed,
            Error = "Node 'kafka-source' failed to connect"
        };

        Assert.Equal(PipelineStatus.Failed, definition.Status);
        Assert.Equal("Node 'kafka-source' failed to connect", definition.Error);
    }

    [Fact]
    public void PipelineDefinition_WithSchedule_SetsSchedule()
    {
        var schedule = new ScheduleConfig
        {
            CronExpression = "0 0 * * *",
            Timezone = "Europe/Berlin",
            Enabled = true,
            MaxRunDurationMinutes = 60
        };

        var definition = new PipelineDefinition
        {
            Id = "scheduled",
            Name = "Scheduled Pipeline",
            Nodes = [],
            Connections = [],
            Schedule = schedule
        };

        Assert.NotNull(definition.Schedule);
        Assert.Equal("0 0 * * *", definition.Schedule.CronExpression);
        Assert.Equal("Europe/Berlin", definition.Schedule.Timezone);
        Assert.True(definition.Schedule.Enabled);
        Assert.Equal(60, definition.Schedule.MaxRunDurationMinutes);
    }

    [Fact]
    public void PipelineNode_AllProperties_AreSet()
    {
        var retryPolicy = new RetryPolicy(MaxRetries: 5, BackoffMs: 2000);

        var node = new PipelineNode
        {
            Id = "node-abc",
            ConnectorType = "PostgreSQL",
            Config = new Dictionary<string, string>
            {
                ["connection.url"] = "jdbc:postgresql://localhost/mydb",
                ["table.name"] = "orders"
            },
            X = 150.5,
            Y = 250.3,
            Label = "DB Source",
            RetryPolicy = retryPolicy,
            SubPipelineId = "sub-pipe-1",
            PortMappingsJson = "{\"in\":\"topic-a\"}"
        };

        Assert.Equal("node-abc", node.Id);
        Assert.Equal("PostgreSQL", node.ConnectorType);
        Assert.Equal(2, node.Config.Count);
        Assert.Equal(150.5, node.X);
        Assert.Equal(250.3, node.Y);
        Assert.Equal("DB Source", node.Label);
        Assert.NotNull(node.RetryPolicy);
        Assert.Equal(5, node.RetryPolicy.MaxRetries);
        Assert.Equal("sub-pipe-1", node.SubPipelineId);
        Assert.NotNull(node.PortMappingsJson);
    }

    [Fact]
    public void PipelineNode_OptionalFields_DefaultToNull()
    {
        var node = new PipelineNode
        {
            Id = "minimal-node",
            ConnectorType = "Console",
            Config = new Dictionary<string, string>(),
            X = 0,
            Y = 0
        };

        Assert.Null(node.Label);
        Assert.Null(node.RetryPolicy);
        Assert.Null(node.SubPipelineId);
        Assert.Null(node.PortMappingsJson);
    }

    [Fact]
    public void PipelineConnection_AllProperties_AreSet()
    {
        var connection = new PipelineConnection
        {
            Id = "conn-1",
            SourceNodeId = "source-node",
            TargetNodeId = "target-node",
            InternalTopic = "pipeline-1-conn-1",
            Type = PipelineConnectionType.Error
        };

        Assert.Equal("conn-1", connection.Id);
        Assert.Equal("source-node", connection.SourceNodeId);
        Assert.Equal("target-node", connection.TargetNodeId);
        Assert.Equal("pipeline-1-conn-1", connection.InternalTopic);
        Assert.Equal(PipelineConnectionType.Error, connection.Type);
    }

    [Fact]
    public void PipelineConnection_DefaultType_IsNormal()
    {
        var connection = new PipelineConnection
        {
            Id = "default-conn",
            SourceNodeId = "a",
            TargetNodeId = "b"
        };

        Assert.Equal(PipelineConnectionType.Normal, connection.Type);
        Assert.Null(connection.InternalTopic);
    }
}
