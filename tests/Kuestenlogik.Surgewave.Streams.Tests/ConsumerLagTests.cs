using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Monitoring;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class ConsumerLagTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public ConsumerLagTests(ITestOutputHelper output)
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
    public void StreamsLagProvider_ReturnsZeroLag_WhenNoTasks()
    {
        var config = new StreamsConfig { ApplicationId = "lag-test", BootstrapServers = "localhost:9092" };
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").ForEach((k, v) => { });

        var topology = builder.Build();
        var app = new StreamsApplication(config, topology, _loggerFactory);

        var totalLag = app.Lag.GetTotalLag();
        Assert.Equal(0, totalLag);
    }

    [Fact]
    public void StreamsLagProvider_GetApplicationLag_ReturnsCorrectStructure()
    {
        var config = new StreamsConfig { ApplicationId = "lag-struct", BootstrapServers = "localhost:9092" };
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").ForEach((k, v) => { });

        var topology = builder.Build();
        var app = new StreamsApplication(config, topology, _loggerFactory);

        var lag = app.Lag.GetApplicationLag();
        Assert.Equal("lag-struct", lag.ApplicationId);
        Assert.Equal(0, lag.TotalLag);
        Assert.NotNull(lag.Partitions);
        Assert.True(lag.Timestamp > DateTimeOffset.MinValue);
    }

    [Fact]
    public void StreamsLagProvider_GetPartitionLag_ReturnsNull_WhenNoData()
    {
        var config = new StreamsConfig { ApplicationId = "lag-part-null", BootstrapServers = "localhost:9092" };
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").ForEach((k, v) => { });

        var topology = builder.Build();
        var app = new StreamsApplication(config, topology, _loggerFactory);

        var partitionLag = app.Lag.GetPartitionLag("nonexistent", 0);
        Assert.Null(partitionLag);
    }

    [Fact]
    public void StreamsLagProvider_GetTotalLag_ReturnsZero_Initially()
    {
        var config = new StreamsConfig { ApplicationId = "lag-total-init", BootstrapServers = "localhost:9092" };
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").ForEach((k, v) => { });

        var topology = builder.Build();
        var app = new StreamsApplication(config, topology, _loggerFactory);

        Assert.Equal(0, app.Lag.GetTotalLag());
    }

    [Fact]
    public void StreamsApplication_LagProperty_IsAccessible()
    {
        var config = new StreamsConfig { ApplicationId = "lag-app", BootstrapServers = "localhost:9092" };
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").ForEach((k, v) => { });

        var topology = builder.Build();
        var app = new StreamsApplication(config, topology, _loggerFactory);

        Assert.NotNull(app.Lag);
        Assert.IsAssignableFrom<IStreamsLagProvider>(app.Lag);

        var lag = app.Lag.GetApplicationLag();
        Assert.Equal("lag-app", lag.ApplicationId);
    }
}
