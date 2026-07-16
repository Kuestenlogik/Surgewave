namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Config;

/// <summary>
/// Pins the raw librdkafka wire format of ProducerConfig: canonical key names,
/// acks/compression/partitioner string mappings in both directions, and tolerant
/// handling of unknown or malformed raw values (drop-in compatibility contract).
/// </summary>
public class ProducerConfigWireFormatTests
{
    [Theory]
    [InlineData(Acks.None, "0")]
    [InlineData(Acks.Leader, "1")]
    [InlineData(Acks.All, "-1")]
    public void Acks_Setter_WritesCanonicalRawValue(Acks acks, string expected)
    {
        var config = new ProducerConfig { Acks = acks };
        Assert.Equal(expected, config["acks"]);
    }

    [Theory]
    [InlineData("0", Acks.None)]
    [InlineData("1", Acks.Leader)]
    [InlineData("-1", Acks.All)]
    [InlineData("all", Acks.All)]
    public void Acks_Getter_ParsesRawValue(string raw, Acks expected)
    {
        var config = new ProducerConfig { ["acks"] = raw };
        Assert.Equal(expected, config.Acks);
    }

    [Fact]
    public void Acks_UnknownRawValue_ReturnsNull()
    {
        var config = new ProducerConfig { ["acks"] = "quorum" };
        Assert.Null(config.Acks);
    }

    [Fact]
    public void Acks_SetNull_RemovesKey()
    {
        var config = new ProducerConfig { Acks = Acks.All };

        config.Acks = null;

        Assert.Null(config["acks"]);
    }

    [Theory]
    [InlineData(CompressionType.None, "none")]
    [InlineData(CompressionType.Gzip, "gzip")]
    [InlineData(CompressionType.Snappy, "snappy")]
    [InlineData(CompressionType.Lz4, "lz4")]
    [InlineData(CompressionType.Zstd, "zstd")]
    public void CompressionType_Setter_WritesWireString(CompressionType compression, string expected)
    {
        var config = new ProducerConfig { CompressionType = compression };
        Assert.Equal(expected, config["compression.type"]);
    }

    [Theory]
    [InlineData("none", CompressionType.None)]
    [InlineData("gzip", CompressionType.Gzip)]
    [InlineData("snappy", CompressionType.Snappy)]
    [InlineData("lz4", CompressionType.Lz4)]
    [InlineData("zstd", CompressionType.Zstd)]
    public void CompressionType_Getter_ParsesWireString(string raw, CompressionType expected)
    {
        var config = new ProducerConfig { ["compression.type"] = raw };
        Assert.Equal(expected, config.CompressionType);
    }

    [Fact]
    public void CompressionType_UnknownWireString_ReturnsNull()
    {
        var config = new ProducerConfig { ["compression.type"] = "brotli" };
        Assert.Null(config.CompressionType);
    }

    [Theory]
    [InlineData(Partitioner.Random, "random")]
    [InlineData(Partitioner.Consistent, "consistent")]
    [InlineData(Partitioner.ConsistentRandom, "consistent_random")]
    [InlineData(Partitioner.Murmur2, "murmur2")]
    [InlineData(Partitioner.Murmur2Random, "murmur2_random")]
    public void Partitioner_Setter_WritesWireString(Partitioner partitioner, string expected)
    {
        var config = new ProducerConfig { Partitioner = partitioner };
        Assert.Equal(expected, config["partitioner"]);
    }

    [Theory]
    [InlineData("random", Partitioner.Random)]
    [InlineData("consistent", Partitioner.Consistent)]
    [InlineData("consistent_random", Partitioner.ConsistentRandom)]
    [InlineData("murmur2", Partitioner.Murmur2)]
    [InlineData("murmur2_random", Partitioner.Murmur2Random)]
    public void Partitioner_Getter_ParsesWireString(string raw, Partitioner expected)
    {
        var config = new ProducerConfig { ["partitioner"] = raw };
        Assert.Equal(expected, config.Partitioner);
    }

    [Fact]
    public void Partitioner_UnknownWireString_ReturnsNull()
    {
        var config = new ProducerConfig { ["partitioner"] = "sticky" };
        Assert.Null(config.Partitioner);
    }

    [Fact]
    public void TypedProperties_WriteCanonicalLibrdkafkaKeys()
    {
        var config = new ProducerConfig
        {
            LingerMs = 25,
            BatchNumMessages = 1000,
            BatchSize = 65536,
            MessageMaxBytes = 1048576,
            RequestTimeoutMs = 30000,
            MessageTimeoutMs = 300000,
            MessageSendMaxRetries = 5,
            RetryBackoffMs = 100,
            MaxInFlight = 5,
            EnableIdempotence = true,
            TransactionalId = "txn-1",
            TransactionTimeoutMs = 60000
        };

        Assert.Equal("25", config["linger.ms"]);
        Assert.Equal("1000", config["batch.num.messages"]);
        Assert.Equal("65536", config["batch.size"]);
        Assert.Equal("1048576", config["message.max.bytes"]);
        Assert.Equal("30000", config["request.timeout.ms"]);
        Assert.Equal("300000", config["message.timeout.ms"]);
        Assert.Equal("5", config["message.send.max.retries"]);
        Assert.Equal("100", config["retry.backoff.ms"]);
        Assert.Equal("5", config["max.in.flight.requests.per.connection"]);
        Assert.Equal("true", config["enable.idempotence"]);
        Assert.Equal("txn-1", config["transactional.id"]);
        Assert.Equal("60000", config["transaction.timeout.ms"]);
    }

    [Fact]
    public void Constructor_FromRawDictionary_ExposesTypedProperties()
    {
        var raw = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "broker:9092",
            ["acks"] = "all",
            ["compression.type"] = "zstd",
            ["linger.ms"] = "5",
            ["enable.idempotence"] = "true",
            ["partitioner"] = "murmur2_random"
        };

        var config = new ProducerConfig(raw);

        Assert.Equal("broker:9092", config.BootstrapServers);
        Assert.Equal(Acks.All, config.Acks);
        Assert.Equal(CompressionType.Zstd, config.CompressionType);
        Assert.Equal(5.0, config.LingerMs);
        Assert.True(config.EnableIdempotence);
        Assert.Equal(Partitioner.Murmur2Random, config.Partitioner);
    }

    [Fact]
    public void NumericGetter_NonNumericRawValue_ReturnsNull()
    {
        var config = new ProducerConfig { ["batch.num.messages"] = "not-a-number" };
        Assert.Null(config.BatchNumMessages);
    }

    [Fact]
    public void EnableIdempotence_False_StoredAsLowercase()
    {
        var config = new ProducerConfig { EnableIdempotence = false };
        Assert.Equal("false", config["enable.idempotence"]);
    }
}
