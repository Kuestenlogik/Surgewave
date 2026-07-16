using Confluent.Kafka.Admin;

namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Builders;

/// <summary>
/// Pins AdminClientBuilder validation and fluent-chaining behavior that runs
/// before any connection is made, plus AdminClientConfig raw-dictionary
/// construction.
/// </summary>
public class AdminClientBuilderTests
{
    [Fact]
    public void Build_WithoutBootstrapServers_ThrowsArgumentException()
    {
        var builder = new AdminClientBuilder(new AdminClientConfig());

        var ex = Assert.Throws<ArgumentException>(() => builder.Build());

        Assert.Contains("BootstrapServers", ex.Message);
    }

    [Fact]
    public void Build_FromEmptyRawDictionary_ThrowsArgumentException()
    {
        var builder = new AdminClientBuilder(new Dictionary<string, string>());

        Assert.Throws<ArgumentException>(() => builder.Build());
    }

    [Fact]
    public void FluentSetters_ReturnSameBuilderInstance()
    {
        var builder = new AdminClientBuilder(new AdminClientConfig());

        Assert.Same(builder, builder.SetErrorHandler((_, _) => { }));
        Assert.Same(builder, builder.SetLogHandler((_, _) => { }));
        Assert.Same(builder, builder.SetStatisticsHandler((_, _) => { }));
    }

    [Fact]
    public void AdminClientConfig_FromRawDictionary_ExposesTypedProperties()
    {
        var raw = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "broker:9092",
            ["client.id"] = "admin-1"
        };

        var config = new AdminClientConfig(raw);

        Assert.Equal("broker:9092", config.BootstrapServers);
        Assert.Equal("admin-1", config.ClientId);
    }
}
