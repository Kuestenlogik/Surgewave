using Kuestenlogik.Surgewave.Streams.CEP;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class CEPIntegrationTests
{
    private static StreamsApplication CreateApp(Topology topology)
    {
        var config = new StreamsConfig { ApplicationId = "test-app", BootstrapServers = "localhost:9092" };
        return new StreamsApplication(config, topology, NullLoggerFactory.Instance);
    }

    [Fact]
    public void CEP_ProcessWithTopology_CreatesNode()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("events");

        var pattern = Pattern<int>.Begin("start")
            .Where(v => v > 10)
            .Next("end")
            .Where(v => v < 5);

        var patternStream = stream.Pattern(pattern);

        var result = patternStream.ProcessWithTopology<string, string>(match =>
        {
            var start = match.GetFirst("start");
            var end = match.GetFirst("end");
            return new KeyValue<string, string>("matched", $"{start}->{end}");
        });

        Assert.NotNull(result);
    }

    [Fact]
    public async Task CEP_PatternMatching_Works()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("events");

        var pattern = Pattern<int>.Begin("high")
            .Where(v => v > 100);

        var results = new List<string>();

        stream.Pattern(pattern)
            .Process<string, string>(match =>
            {
                var value = match.GetFirst("high");
                return new KeyValue<string, string>("match", $"found:{value}");
            })
            .ForEach((k, v) => results.Add(v));

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        app.ProcessRecord("events", "k1", 50);
        app.ProcessRecord("events", "k2", 150);
        app.ProcessRecord("events", "k3", 200);

        Assert.Equal(2, results.Count);
        Assert.Equal("found:150", results[0]);
        Assert.Equal("found:200", results[1]);
    }

    [Fact]
    public void CEP_NFAStateSnapshot_Properties()
    {
        var snapshot = new NFAStateSnapshot
        {
            CurrentPatternIndex = 2,
            MatchCount = 3,
            StartTimestamp = 1000,
            PatternMatchCounts = new Dictionary<string, int>
            {
                ["start"] = 1,
                ["middle"] = 2
            }
        };

        Assert.Equal(2, snapshot.CurrentPatternIndex);
        Assert.Equal(3, snapshot.MatchCount);
        Assert.Equal(1000, snapshot.StartTimestamp);
        Assert.Equal(2, snapshot.PatternMatchCounts.Count);
    }

    [Fact]
    public void CEP_PatternStream_AsStream()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("events");

        var pattern = Pattern<int>.Begin("check")
            .Where(v => v == 42);

        var matchStream = stream.Pattern(pattern).AsStream();
        Assert.NotNull(matchStream);
    }

    [Fact]
    public async Task CEP_WithinTimeConstraint()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("events");

        var pattern = Pattern<int>.Begin("start")
            .Where(v => v > 0)
            .FollowedBy("end")
            .Where(v => v < 0)
            .Within(TimeSpan.FromSeconds(10));

        var results = new List<string>();

        stream.Pattern(pattern)
            .Process<string, string>(match =>
            {
                return new KeyValue<string, string>("m", "matched");
            })
            .ForEach((k, v) => results.Add(v));

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        app.ProcessRecord("events", "k1", 10);
        app.ProcessRecord("events", "k2", -5);

        Assert.Single(results);
    }
}
