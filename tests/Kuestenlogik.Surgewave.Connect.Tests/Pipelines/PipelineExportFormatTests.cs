namespace Kuestenlogik.Surgewave.Connect.Tests.Pipelines;

using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Pipelines;

public class PipelineExportFormatTests
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static PipelineExportFormat CreateExport()
    {
        return new PipelineExportFormat
        {
            Version = "1.0",
            ExportedAt = DateTimeOffset.UnixEpoch,
            SurgewaveVersion = "1.2.3",
            Pipeline = new PipelineExportData
            {
                Name = "sample",
                Description = "desc",
                Nodes =
                [
                    new PipelineNodeExport
                    {
                        NodeId = "n1",
                        ConnectorType = "Kuestenlogik.Surgewave.Connect.Nodes.Transform.FilterNode",
                        Config = new Dictionary<string, string> { ["condition"] = "$.x == 1" },
                        X = 100,
                        Y = 200,
                        RetryPolicy = new RetryPolicy(MaxRetries: 7),
                    },
                ],
                Connections =
                [
                    new PipelineConnectionExport { SourceNodeId = "n1", TargetNodeId = "n2", Type = PipelineConnectionType.Error },
                ],
                Parameters = new Dictionary<string, string> { ["region"] = "eu" },
                Schedule = new ScheduleConfig { CronExpression = "0 * * * *", Enabled = true },
            },
        };
    }

    [Fact]
    public void PortableFields_SurviveJsonRoundTrip()
    {
        var json = JsonSerializer.Serialize(CreateExport(), Web);
        var restored = JsonSerializer.Deserialize<PipelineExportFormat>(json, Web);

        Assert.NotNull(restored);
        var node = Assert.Single(restored.Pipeline.Nodes);
        Assert.NotNull(node.RetryPolicy);
        Assert.Equal(7, node.RetryPolicy.MaxRetries);
        Assert.NotNull(restored.Pipeline.Parameters);
        Assert.Equal("eu", restored.Pipeline.Parameters["region"]);
        Assert.NotNull(restored.Pipeline.Schedule);
        Assert.Equal("0 * * * *", restored.Pipeline.Schedule.CronExpression);
        Assert.True(restored.Pipeline.Schedule.Enabled);
        Assert.Equal(PipelineConnectionType.Error, Assert.Single(restored.Pipeline.Connections).Type);
    }

    [Fact]
    public void ExportsWithoutPortableFields_StillDeserialize()
    {
        // shape of exports created before the format additions
        const string legacy = """
            {
              "version": "1.0",
              "exportedAt": "2026-01-01T00:00:00+00:00",
              "pipeline": {
                "name": "legacy",
                "nodes": [
                  { "nodeId": "n1", "connectorType": "X", "config": {}, "x": 1, "y": 2 }
                ],
                "connections": [
                  { "sourceNodeId": "n1", "targetNodeId": "n1" }
                ]
              }
            }
            """;

        var restored = JsonSerializer.Deserialize<PipelineExportFormat>(legacy, Web);

        Assert.NotNull(restored);
        Assert.Null(restored.Pipeline.Parameters);
        Assert.Null(restored.Pipeline.Schedule);
        Assert.Null(Assert.Single(restored.Pipeline.Nodes).RetryPolicy);
        Assert.Equal(PipelineConnectionType.Normal, Assert.Single(restored.Pipeline.Connections).Type);
    }
}
