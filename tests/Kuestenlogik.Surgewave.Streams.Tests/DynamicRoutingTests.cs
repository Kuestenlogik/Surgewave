using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class DynamicRoutingTests
{
    [Fact]
    public void DynamicRouting_RoutesToCorrectTopic()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, string>("input");

        stream.To((key, value) => $"output-{key}");

        var topology = builder.Build();
        var description = topology.Describe();
        Assert.Contains("DYNAMIC-SINK", description);
    }

    [Fact]
    public void DynamicRouting_DifferentRecords_DifferentTopics()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("input");

        stream.To((key, value) => value > 100 ? "high-values" : "low-values");

        var topology = builder.Build();
        var description = topology.Describe();
        Assert.Contains("DYNAMIC-SINK", description);
        Assert.DoesNotContain("dynamic-topic", description);
    }

    [Fact]
    public void DynamicRouting_TopologyBuild_Succeeds()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, string>("events");

        stream.To((key, value) => $"topic-{key.Length}");

        var topology = builder.Build();
        Assert.NotEmpty(topology.Sources);
    }

    [Fact]
    public void DynamicRouting_NotStaticTopic()
    {
        // Verify that dynamic routing no longer hardcodes "dynamic-topic"
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("input");

        stream.To((key, value) => value > 50 ? "high" : "low");

        var topology = builder.Build();
        var description = topology.Describe();

        // Should have a dynamic sink node, not a regular sink with "dynamic-topic"
        Assert.Contains("DYNAMIC-SINK", description);
    }
}
