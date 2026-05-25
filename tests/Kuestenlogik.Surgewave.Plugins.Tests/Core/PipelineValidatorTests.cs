using Kuestenlogik.Surgewave.Plugins.Pipeline;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Core;

/// <summary>
/// Tests fuer <see cref="PipelineValidator"/> — Port-Constraints, Cycle-Detection,
/// unknown-node-references und Start/End-Node-Requirement.
/// </summary>
public sealed class PipelineValidatorTests
{
    private sealed class FakeNode : IPipelineNode
    {
        public required string DisplayName { get; init; }
        public string FeatureId => DisplayName;
        public int InputPorts { get; init; }
        public int OutputPorts { get; init; }
        public Kuestenlogik.Surgewave.Plugins.Configuration.ConfigDef Config { get; } = new();
        public string Version => "1.0.0";
    }

    private static FakeNode Source(string name = "src") => new() { DisplayName = name, InputPorts = 0, OutputPorts = 1 };
    private static FakeNode Sink(string name = "snk") => new() { DisplayName = name, InputPorts = 1, OutputPorts = 0 };
    private static FakeNode Processor(string name = "proc") => new() { DisplayName = name, InputPorts = 1, OutputPorts = 1 };

    [Fact]
    public void Validate_EmptyPipeline_ReportsError()
    {
        var result = PipelineValidator.Validate(
            nodes: new Dictionary<string, IPipelineNode>(),
            connections: []);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("no nodes", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Validate_LinearSourceToSink_IsValid()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["s"] = Source(),
            ["k"] = Sink(),
        };
        var connections = new List<(string, string)> { ("s", "k") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Validate_SourceWithIncomingConnection_ReportsError()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["a"] = Source(),
            ["b"] = Source(),
        };
        var connections = new List<(string, string)> { ("a", "b") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("InputPorts=0", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Validate_SinkWithOutgoingConnection_ReportsError()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["a"] = Sink(),
            ["b"] = Sink(),
        };
        var connections = new List<(string, string)> { ("a", "b") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("OutputPorts=0", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Validate_ConnectionToUnknownSource_ReportsError()
    {
        var nodes = new Dictionary<string, IPipelineNode> { ["k"] = Sink() };
        var connections = new List<(string, string)> { ("ghost", "k") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("ghost", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_ConnectionToUnknownTarget_ReportsError()
    {
        var nodes = new Dictionary<string, IPipelineNode> { ["s"] = Source() };
        var connections = new List<(string, string)> { ("s", "ghost") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("ghost", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_DanglingInputProducesWarning()
    {
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["s"] = Source(),
            ["k"] = Sink(),
        };
        // No connection — sink has no incoming
        var result = PipelineValidator.Validate(nodes, []);

        Assert.NotEmpty(result.Warnings);
        Assert.Contains(result.Warnings, w => w.Contains("expects input", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(result.Warnings, w => w.Contains("expects output", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Validate_NoStartNode_ReportsError()
    {
        // Two processors, both have InputPorts > 0 — no start
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["a"] = Processor(),
            ["b"] = Processor(),
        };
        var connections = new List<(string, string)> { ("a", "b"), ("b", "a") };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("no start", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(result.Errors, e => e.Contains("no end", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Validate_Cycle_ReportsError()
    {
        // a -> b -> c -> a (cycle in middle, with source & sink at ends)
        var nodes = new Dictionary<string, IPipelineNode>
        {
            ["s"] = Source(),
            ["a"] = Processor("a"),
            ["b"] = Processor("b"),
            ["k"] = Sink(),
        };
        var connections = new List<(string, string)>
        {
            ("s", "a"),
            ("a", "b"),
            ("b", "a"),  // creates a-b cycle
            ("b", "k"),
        };

        var result = PipelineValidator.Validate(nodes, connections);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("cycle", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void PipelineValidationResult_NoErrors_IsValid()
    {
        var r = new PipelineValidationResult([], ["warn"]);

        Assert.True(r.IsValid);
    }

    [Fact]
    public void PipelineValidationResult_WithErrors_NotValid()
    {
        var r = new PipelineValidationResult(["err"], []);

        Assert.False(r.IsValid);
    }
}
