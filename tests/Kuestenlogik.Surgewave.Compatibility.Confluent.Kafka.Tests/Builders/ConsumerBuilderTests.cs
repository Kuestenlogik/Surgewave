namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Builders;

/// <summary>
/// Pins ConsumerBuilder validation and fluent-chaining behavior that runs before
/// any connection is made: missing bootstrap servers fail fast and every setter
/// (including all rebalance handlers) returns the same builder instance.
/// </summary>
public class ConsumerBuilderTests
{
    [Fact]
    public void Build_WithoutBootstrapServers_ThrowsArgumentException()
    {
        var builder = new ConsumerBuilder<string, string>(new ConsumerConfig { GroupId = "group-1" });

        var ex = Assert.Throws<ArgumentException>(() => builder.Build());

        Assert.Contains("BootstrapServers", ex.Message);
    }

    [Fact]
    public void Build_FromEmptyRawDictionary_ThrowsArgumentException()
    {
        var builder = new ConsumerBuilder<string, string>(new Dictionary<string, string>());

        Assert.Throws<ArgumentException>(() => builder.Build());
    }

    [Fact]
    public void FluentSetters_ReturnSameBuilderInstance()
    {
        var builder = new ConsumerBuilder<string, string>(new ConsumerConfig());

        Assert.Same(builder, builder.SetKeyDeserializer(Deserializers.Utf8));
        Assert.Same(builder, builder.SetValueDeserializer(Deserializers.Utf8));
        Assert.Same(builder, builder.SetErrorHandler((_, _) => { }));
        Assert.Same(builder, builder.SetLogHandler((_, _) => { }));
        Assert.Same(builder, builder.SetStatisticsHandler((_, _) => { }));
        Assert.Same(builder, builder.SetPartitionsAssignedHandler((_, _) => { }));
        Assert.Same(builder, builder.SetPartitionsRevokedHandler((_, _) => { }));
        Assert.Same(builder, builder.SetPartitionsLostHandler((_, _) => { }));
        Assert.Same(builder, builder.SetOffsetsCommittedHandler((_, _) => { }));
    }
}
