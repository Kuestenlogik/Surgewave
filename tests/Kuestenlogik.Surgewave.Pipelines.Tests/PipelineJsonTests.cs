using System.Text.Json;
using Kuestenlogik.Surgewave.Pipelines.Serialization;

namespace Kuestenlogik.Surgewave.Pipelines.Tests;

public class PipelineJsonTests
{
    private sealed record Order(string Status, double Amount);

    private static BuiltPipeline BuildSample()
    {
        return Pipeline.From<Order>("orders")
            .Named("sample")
            .DescribedAs("sample pipeline")
            .WithParameter("region", "eu")
            .Filter(o => o.Amount > 1000)
            .WithRetry(maxRetries: 2)
            .To("out")
            .Build();
    }

    [Fact]
    public void ToJson_ProducesEditorCompatibleShape()
    {
        var json = BuildSample().ToJson();

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("1.0", root.GetProperty("version").GetString());
        Assert.Equal(DateTimeOffset.UnixEpoch, root.GetProperty("exportedAt").GetDateTimeOffset());

        var pipeline = root.GetProperty("pipeline");
        Assert.Equal("sample", pipeline.GetProperty("name").GetString());
        Assert.Equal("sample pipeline", pipeline.GetProperty("description").GetString());
        Assert.Equal("eu", pipeline.GetProperty("parameters").GetProperty("region").GetString());

        var node = pipeline.GetProperty("nodes")[0];
        Assert.Equal("filter-1", node.GetProperty("nodeId").GetString());
        Assert.Equal(ConnectNodeTypes.Filter, node.GetProperty("connectorType").GetString());

        // config keys keep their connector-defined casing
        var config = node.GetProperty("config");
        Assert.Equal("orders", config.GetProperty("topics").GetString());
        Assert.Equal("$.amount > 1000", config.GetProperty("condition").GetString());
        Assert.Equal("out", config.GetProperty("output.topic").GetString());

        Assert.Equal(2, node.GetProperty("retryPolicy").GetProperty("maxRetries").GetInt32());
    }

    [Fact]
    public void ToJson_IsDeterministic()
    {
        Assert.Equal(BuildSample().ToJson(), BuildSample().ToJson());
    }

    [Fact]
    public void RoundTrip_PreservesStructure()
    {
        var json = BuildSample().ToJson();
        var export = PipelineJson.Read(json);

        Assert.Equal("sample", export.Pipeline.Name);
        var node = Assert.Single(export.Pipeline.Nodes);
        Assert.Equal("filter-1", node.NodeId);
        Assert.Equal("$.amount > 1000", node.Config["condition"]);
        Assert.NotNull(node.RetryPolicy);
        Assert.Equal(2, node.RetryPolicy.MaxRetries);
        Assert.NotNull(export.Pipeline.Parameters);
        Assert.Equal("eu", export.Pipeline.Parameters["region"]);
    }

    [Fact]
    public void Read_RejectsNonExportDocuments()
    {
        Assert.ThrowsAny<JsonException>(() => PipelineJson.Read("[1,2,3]"));
        Assert.ThrowsAny<JsonException>(() => PipelineJson.Read("not json"));
    }

    [Fact]
    public void SaveAndReadFile_RoundTrips()
    {
        var path = Path.Combine(Path.GetTempPath(), $"surgewave-pipeline-{Guid.NewGuid():N}.json");
        try
        {
            BuildSample().Save(path);
            var export = PipelineJson.ReadFile(path);
            Assert.Equal("sample", export.Pipeline.Name);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
