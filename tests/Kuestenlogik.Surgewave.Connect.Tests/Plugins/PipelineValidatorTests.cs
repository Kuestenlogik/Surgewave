using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Plugins.Pipeline;

namespace Kuestenlogik.Surgewave.Connect.Tests.Plugins;

public class PipelineValidatorTests
{
    [Fact]
    public void EmptyPipeline_ReturnsError()
    {
        var result = PipelineValidator.Validate(
            new Dictionary<string, IPipelineNode>(),
            []);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("no nodes"));
    }

    [Fact]
    public void SingleSourceAndSink_IsValid()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["source"] = new TestSourceNode(),
            ["sink"] = new TestSinkNode()
        };
        var connections = new List<(string, string)> { ("source", "sink") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.True(result.IsValid);
    }

    [Fact]
    public void MissingStartNode_ReturnsError()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["processor"] = new TestProcessorNode(),
            ["sink"] = new TestSinkNode()
        };
        var connections = new List<(string, string)> { ("processor", "sink") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("no start node"));
    }

    [Fact]
    public void MissingEndNode_ReturnsError()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["source"] = new TestSourceNode(),
            ["processor"] = new TestProcessorNode()
        };
        var connections = new List<(string, string)> { ("source", "processor") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("no end node"));
    }

    [Fact]
    public void SourceWithIncoming_ReturnsError()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["source"] = new TestSourceNode(),
            ["sink"] = new TestSinkNode()
        };
        var connections = new List<(string, string)> { ("sink", "source"), ("source", "sink") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("InputPorts=0 but receives"));
    }

    [Fact]
    public void CyclicPipeline_ReturnsError()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["source"] = new TestSourceNode(),
            ["a"] = new TestProcessorNode(),
            ["b"] = new TestProcessorNode(),
            ["sink"] = new TestSinkNode()
        };
        var connections = new List<(string, string)>
        {
            ("source", "a"), ("a", "b"), ("b", "a"), ("b", "sink")
        };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("cycle"));
    }

    [Fact]
    public void UnconnectedProcessor_ReturnsWarnings()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["source"] = new TestSourceNode(),
            ["orphan"] = new TestProcessorNode(),
            ["sink"] = new TestSinkNode()
        };
        var connections = new List<(string, string)> { ("source", "sink") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.True(result.IsValid);
        Assert.True(result.Warnings.Count >= 2); // orphan has no in + no out
    }

    [Fact]
    public void UnknownSourceInConnection_ReturnsError()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["source"] = new TestSourceNode(),
            ["sink"] = new TestSinkNode()
        };
        var connections = new List<(string, string)> { ("source", "sink"), ("ghost", "sink") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("unknown source node 'ghost'"));
    }

    [Fact]
    public void SourceProcessorSink_FullPipeline_IsValid()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["src"] = new TestSourceNode(),
            ["transform"] = new TestProcessorNode(),
            ["dst"] = new TestSinkNode()
        };
        var connections = new List<(string, string)>
        {
            ("src", "transform"), ("transform", "dst")
        };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
    }

    // --- Test node implementations ---

    private sealed class TestSourceNode : ISourceNode
    {
        public string FeatureId => "test.source";
        public string DisplayName => "Test Source";
        public int InputPorts => 0;
        public int OutputPorts => 1;
        public ConfigDef Config => new();
        public string Version => "1.0.0";
    }

    private sealed class TestSinkNode : ISinkNode
    {
        public string FeatureId => "test.sink";
        public string DisplayName => "Test Sink";
        public int InputPorts => 1;
        public int OutputPorts => 0;
        public ConfigDef Config => new();
        public string Version => "1.0.0";
    }

    private sealed class TestProcessorNode : IProcessorNode
    {
        public string FeatureId => "test.processor";
        public string DisplayName => "Test Processor";
        public int InputPorts => 1;
        public int OutputPorts => 1;
        public ConfigDef Config => new();
        public string Version => "1.0.0";
    }
}
