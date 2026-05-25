namespace Kuestenlogik.Surgewave.Connect.Tests.Pipelines;

using Kuestenlogik.Surgewave.Connect.Pipelines;

public class PipelineVariableResolverTests
{
    private static PipelineVariableContext CreateContext(
        string pipelineId = "pipe-1",
        string pipelineName = "Test Pipeline",
        string? nodeId = "node-1",
        Dictionary<string, string>? parameters = null)
    {
        return new PipelineVariableContext
        {
            PipelineId = pipelineId,
            PipelineName = pipelineName,
            NodeId = nodeId,
            Parameters = parameters ?? new Dictionary<string, string>()
        };
    }

    [Fact]
    public void ResolveValue_PipelineId()
    {
        var ctx = CreateContext(pipelineId: "abc123");
        var result = PipelineVariableResolver.ResolveValue("prefix-${pipeline.id}-suffix", ctx);
        Assert.Equal("prefix-abc123-suffix", result);
    }

    [Fact]
    public void ResolveValue_PipelineName()
    {
        var ctx = CreateContext(pipelineName: "My Pipeline");
        var result = PipelineVariableResolver.ResolveValue("${pipeline.name}", ctx);
        Assert.Equal("My Pipeline", result);
    }

    [Fact]
    public void ResolveValue_NodeId()
    {
        var ctx = CreateContext(nodeId: "node-42");
        var result = PipelineVariableResolver.ResolveValue("${node.id}", ctx);
        Assert.Equal("node-42", result);
    }

    [Fact]
    public void ResolveValue_Timestamp_ReturnsIso8601()
    {
        var ctx = CreateContext();
        var result = PipelineVariableResolver.ResolveValue("${timestamp}", ctx);
        Assert.True(DateTimeOffset.TryParse(result, out _));
    }

    [Fact]
    public void ResolveValue_TimestampEpoch_ReturnsNumber()
    {
        var ctx = CreateContext();
        var result = PipelineVariableResolver.ResolveValue("${timestamp.epoch}", ctx);
        Assert.True(long.TryParse(result, out var epoch));
        Assert.True(epoch > 0);
    }

    [Fact]
    public void ResolveValue_Date_ReturnsDateFormat()
    {
        var ctx = CreateContext();
        var result = PipelineVariableResolver.ResolveValue("${date}", ctx);
        Assert.Matches(@"^\d{4}-\d{2}-\d{2}$", result);
    }

    [Fact]
    public void ResolveValue_EnvVariable()
    {
        Environment.SetEnvironmentVariable("Surgewave_TEST_VAR", "hello-world");
        try
        {
            var ctx = CreateContext();
            var result = PipelineVariableResolver.ResolveValue("${env.Surgewave_TEST_VAR}", ctx);
            Assert.Equal("hello-world", result);
        }
        finally
        {
            Environment.SetEnvironmentVariable("Surgewave_TEST_VAR", null);
        }
    }

    [Fact]
    public void ResolveValue_UserParameter()
    {
        var ctx = CreateContext(parameters: new Dictionary<string, string>
        {
            ["target"] = "prod-cluster"
        });
        var result = PipelineVariableResolver.ResolveValue("${param.target}", ctx);
        Assert.Equal("prod-cluster", result);
    }

    [Fact]
    public void ResolveValue_DirectParameterLookup()
    {
        var ctx = CreateContext(parameters: new Dictionary<string, string>
        {
            ["broker"] = "localhost:9092"
        });
        var result = PipelineVariableResolver.ResolveValue("${broker}", ctx);
        Assert.Equal("localhost:9092", result);
    }

    [Fact]
    public void ResolveValue_MixedContent()
    {
        var ctx = CreateContext(pipelineId: "p1", parameters: new Dictionary<string, string>
        {
            ["env"] = "staging"
        });
        var result = PipelineVariableResolver.ResolveValue("dlq-${pipeline.id}-${env}", ctx);
        Assert.Equal("dlq-p1-staging", result);
    }

    [Fact]
    public void ResolveValue_UnresolvedVariable_KeepsOriginal()
    {
        var ctx = CreateContext();
        var result = PipelineVariableResolver.ResolveValue("${unknown.var}", ctx);
        Assert.Equal("${unknown.var}", result);
    }

    [Fact]
    public void Resolve_FullConfig()
    {
        var ctx = CreateContext(pipelineId: "p1", nodeId: "n1", parameters: new Dictionary<string, string>
        {
            ["region"] = "eu-west"
        });

        var config = new Dictionary<string, string>
        {
            ["topic"] = "events-${pipeline.id}",
            ["group.id"] = "${node.id}-consumer",
            ["region"] = "${param.region}",
            ["plain"] = "no-variables-here"
        };

        var resolved = PipelineVariableResolver.Resolve(config, ctx);

        Assert.Equal("events-p1", resolved["topic"]);
        Assert.Equal("n1-consumer", resolved["group.id"]);
        Assert.Equal("eu-west", resolved["region"]);
        Assert.Equal("no-variables-here", resolved["plain"]);
    }

    [Fact]
    public void ResolveValue_EmptyString_ReturnsEmpty()
    {
        var ctx = CreateContext();
        var result = PipelineVariableResolver.ResolveValue("", ctx);
        Assert.Equal("", result);
    }
}
