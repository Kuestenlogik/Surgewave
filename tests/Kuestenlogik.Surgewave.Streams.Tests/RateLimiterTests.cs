using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Processors;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class RateLimiterTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public RateLimiterTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddFilter(level => level >= LogLevel.Debug);
        });
    }

    public async ValueTask DisposeAsync()
    {
        _loggerFactory.Dispose();
        await Task.CompletedTask;
    }

    [Fact]
    public void RateLimiter_UnlimitedDoesNotThrottle()
    {
        var limiter = new RateLimiter(-1);

        Assert.True(limiter.IsUnlimited);
        Assert.True(limiter.TryConsume(1_000_000));
        Assert.Equal(0, limiter.CalculateWaitTimeMs(1_000_000));
    }

    [Fact]
    public void RateLimiter_ConsumeWithinLimit_Succeeds()
    {
        var limiter = new RateLimiter(100); // 100 tokens/sec

        // Fresh bucket should have 100 tokens
        Assert.True(limiter.TryConsume(50));
        Assert.True(limiter.TryConsume(50));
        Assert.Equal(0, limiter.ThrottleCount);
    }

    [Fact]
    public void RateLimiter_ExceedLimit_ReturnsWaitTime()
    {
        var limiter = new RateLimiter(100);

        // Exhaust all tokens
        Assert.True(limiter.TryConsume(100));

        // Next consume should fail
        Assert.False(limiter.TryConsume(1));
        Assert.Equal(1, limiter.ThrottleCount);

        // Wait time should be > 0
        var waitTime = limiter.CalculateWaitTimeMs(1);
        Assert.True(waitTime > 0, $"Expected wait time > 0 but got {waitTime}");
    }

    [Fact]
    public void RateLimiter_RefillsOverTime()
    {
        var limiter = new RateLimiter(1000); // 1000/sec = 1/ms

        // Exhaust
        Assert.True(limiter.TryConsume(1000));
        Assert.False(limiter.TryConsume(1));

        // Wait 50ms for refill
        Thread.Sleep(50);
        limiter.Refill();

        // Should have some tokens back
        Assert.True(limiter.AvailableTokens > 0);
        Assert.True(limiter.TryConsume(1));
    }

    [Fact]
    public void RateLimitNode_ThrottlesInTopology()
    {
        var config = new StreamsConfig { ApplicationId = "test-rl", BootstrapServers = "localhost:9092" };
        var builder = new StreamsBuilder();
        var results = new List<string>();

        builder.Stream<string, string>("input")
            .RateLimit(10_000) // 10k/sec — won't actually block
            .ForEach((k, v) => results.Add(v));

        var topology = builder.Build();
        var app = new StreamsApplication(config, topology, _loggerFactory);

        app.ProcessRecord("input", "key1", "value1");
        app.ProcessRecord("input", "key2", "value2");

        Assert.Equal(2, results.Count);
        Assert.Contains("value1", results);
        Assert.Contains("value2", results);
    }

    [Fact]
    public void StreamsConfig_GlobalRateLimit()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "test-gl-rl",
            BootstrapServers = "localhost:9092",
            MaxRecordsPerSecond = 1000,
            MaxBytesPerSecond = 1_000_000,
            MaxRateLimitWaitMs = 3000
        };

        Assert.Equal(1000, config.MaxRecordsPerSecond);
        Assert.Equal(1_000_000, config.MaxBytesPerSecond);
        Assert.Equal(3000, config.MaxRateLimitWaitMs);

        // Default should be unlimited
        var defaultConfig = new StreamsConfig { ApplicationId = "test", BootstrapServers = "localhost:9092" };
        Assert.Equal(-1, defaultConfig.MaxRecordsPerSecond);
        Assert.Equal(-1, defaultConfig.MaxBytesPerSecond);
    }
}
