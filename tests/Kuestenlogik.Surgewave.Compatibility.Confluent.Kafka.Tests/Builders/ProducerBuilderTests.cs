namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Builders;

/// <summary>
/// Pins ProducerBuilder validation and fluent-chaining behavior that runs before
/// any connection is made: missing bootstrap servers fail fast and every setter
/// returns the same builder instance.
/// </summary>
public class ProducerBuilderTests
{
    [Fact]
    public void Build_WithoutBootstrapServers_ThrowsArgumentException()
    {
        var builder = new ProducerBuilder<string, string>(new ProducerConfig());

        var ex = Assert.Throws<ArgumentException>(() => builder.Build());

        Assert.Contains("BootstrapServers", ex.Message);
    }

    [Fact]
    public void Build_FromEmptyRawDictionary_ThrowsArgumentException()
    {
        var builder = new ProducerBuilder<string, string>(new Dictionary<string, string>());

        Assert.Throws<ArgumentException>(() => builder.Build());
    }

    [Fact]
    public void FluentSetters_ReturnSameBuilderInstance()
    {
        var builder = new ProducerBuilder<string, string>(new ProducerConfig());

        Assert.Same(builder, builder.SetKeySerializer(Serializers.Utf8));
        Assert.Same(builder, builder.SetValueSerializer(Serializers.Utf8));
        Assert.Same(builder, builder.SetErrorHandler((_, _) => { }));
        Assert.Same(builder, builder.SetLogHandler((_, _) => { }));
        Assert.Same(builder, builder.SetStatisticsHandler((_, _) => { }));
    }
}
