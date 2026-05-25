namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

using System.Text;

public class MessageTests
{
    [Fact]
    public void Key_CanBeSetAndRead()
    {
        var message = new Message<string, string>
        {
            Key = "my-key"
        };

        Assert.Equal("my-key", message.Key);
    }

    [Fact]
    public void Value_CanBeSetAndRead()
    {
        var message = new Message<string, string>
        {
            Value = "my-value"
        };

        Assert.Equal("my-value", message.Value);
    }

    [Fact]
    public void Headers_DefaultsToNull()
    {
        var message = new Message<string, string>();
        Assert.Null(message.Headers);
    }

    [Fact]
    public void Headers_CanBeSet()
    {
        var headers = new Headers
        {
            { "key", Encoding.UTF8.GetBytes("value") }
        };

        var message = new Message<string, string>
        {
            Headers = headers
        };

        Assert.Same(headers, message.Headers);
    }

    [Fact]
    public void Timestamp_DefaultsToNotAvailable()
    {
        var message = new Message<string, string>();
        Assert.Equal(TimestampType.NotAvailable, message.Timestamp.Type);
    }

    [Fact]
    public void Timestamp_CanBeSet()
    {
        var timestamp = new Timestamp(DateTimeOffset.UtcNow, TimestampType.CreateTime);
        var message = new Message<string, string>
        {
            Timestamp = timestamp
        };

        Assert.Equal(TimestampType.CreateTime, message.Timestamp.Type);
    }

    [Fact]
    public void IntegerKeys_Work()
    {
        var message = new Message<int, string>
        {
            Key = 42,
            Value = "test"
        };

        Assert.Equal(42, message.Key);
        Assert.Equal("test", message.Value);
    }

    [Fact]
    public void ByteArrayValues_Work()
    {
        var message = new Message<string, byte[]>
        {
            Key = "key",
            Value = new byte[] { 1, 2, 3, 4, 5 }
        };

        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, message.Value);
    }
}
