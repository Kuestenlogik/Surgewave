namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Config;

public class ProducerConfigTests
{
    [Fact]
    public void BootstrapServers_CanBeSetAndRead()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        Assert.Equal("localhost:9092", config.BootstrapServers);
    }

    [Fact]
    public void ClientId_CanBeSetAndRead()
    {
        var config = new ProducerConfig
        {
            ClientId = "my-producer"
        };

        Assert.Equal("my-producer", config.ClientId);
    }

    [Fact]
    public void Acks_DefaultsToNull()
    {
        var config = new ProducerConfig();
        Assert.Null(config.Acks);
    }

    [Fact]
    public void Acks_CanBeSet()
    {
        var config = new ProducerConfig { Acks = Acks.All };
        Assert.Equal(Acks.All, config.Acks);
    }

    [Fact]
    public void EnableIdempotence_DefaultsToNull()
    {
        var config = new ProducerConfig();
        Assert.Null(config.EnableIdempotence);
    }

    [Fact]
    public void EnableIdempotence_CanBeSet()
    {
        var config = new ProducerConfig { EnableIdempotence = true };
        Assert.True(config.EnableIdempotence);
    }

    [Fact]
    public void SurgewaveProtocol_DefaultsToNull()
    {
        var config = new ProducerConfig();
        Assert.Null(config.SurgewaveProtocol);
    }

    [Fact]
    public void SurgewaveProtocol_CanBeSet()
    {
        var config = new ProducerConfig { SurgewaveProtocol = "surgewave" };
        Assert.Equal("surgewave", config.SurgewaveProtocol);
    }

    [Fact]
    public void LingerMs_CanBeSet()
    {
        var config = new ProducerConfig { LingerMs = 10.0 };
        Assert.Equal(10.0, config.LingerMs);
    }

    [Fact]
    public void BatchNumMessages_CanBeSet()
    {
        var config = new ProducerConfig { BatchNumMessages = 5000 };
        Assert.Equal(5000, config.BatchNumMessages);
    }

    [Fact]
    public void CompressionType_CanBeSet()
    {
        var config = new ProducerConfig { CompressionType = CompressionType.Snappy };
        Assert.Equal(CompressionType.Snappy, config.CompressionType);
    }

    [Fact]
    public void RequestTimeoutMs_CanBeSet()
    {
        var config = new ProducerConfig { RequestTimeoutMs = 60000 };
        Assert.Equal(60000, config.RequestTimeoutMs);
    }

    [Fact]
    public void MessageTimeoutMs_CanBeSet()
    {
        var config = new ProducerConfig { MessageTimeoutMs = 600000 };
        Assert.Equal(600000, config.MessageTimeoutMs);
    }

    [Fact]
    public void TransactionalId_CanBeSet()
    {
        var config = new ProducerConfig { TransactionalId = "my-transaction" };
        Assert.Equal("my-transaction", config.TransactionalId);
    }
}
