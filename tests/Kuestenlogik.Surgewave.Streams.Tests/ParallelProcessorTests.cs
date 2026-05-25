using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class ParallelProcessorTests
{
    private static StreamsApplication CreateApp(Topology topology)
    {
        var config = new StreamsConfig { ApplicationId = "test-app", BootstrapServers = "localhost:9092" };
        return new StreamsApplication(config, topology, NullLoggerFactory.Instance);
    }

    [Fact]
    public void Parallel_CreatesParallelNode()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("input");

        stream.Parallel(4)
            .To("output");

        var topology = builder.Build();
        var description = topology.Describe();
        Assert.Contains("PARALLEL", description);
    }

    [Fact]
    public void Parallel_InterfaceHasMethod()
    {
        var builder = new StreamsBuilder();
        IStream<string, int> stream = builder.Stream<string, int>("input");

        // Verify Parallel exists on IStream
        var parallelStream = stream.Parallel(2);
        Assert.NotNull(parallelStream);
    }

    [Fact]
    public void Parallel_ChainedOperations()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("input");

        // Parallel can be chained with other operations
        stream.Filter((k, v) => v > 0)
            .Parallel(4)
            .MapValues<string>(v => v.ToString())
            .To("output");

        var topology = builder.Build();
        Assert.NotEmpty(topology.Sources);
    }

    [Fact]
    public void Parallel_TopologyBuildSucceeds()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input")
            .Parallel(8)
            .ForEach((k, v) => { });

        var topology = builder.Build();
        var description = topology.Describe();
        Assert.Contains("PARALLEL", description);
        Assert.Contains("FOREACH", description);
    }

    [Fact]
    public void Parallel_DegreeOfParallelism_AcceptsValues()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("input");

        // Various parallelism degrees
        var p1 = stream.Parallel(1);
        Assert.NotNull(p1);

        var builder2 = new StreamsBuilder();
        var stream2 = builder2.Stream<string, int>("input");
        var p16 = stream2.Parallel(16);
        Assert.NotNull(p16);
    }
}
