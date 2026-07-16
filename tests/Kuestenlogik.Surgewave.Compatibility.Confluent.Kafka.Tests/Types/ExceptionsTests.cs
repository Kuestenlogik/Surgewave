namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

/// <summary>
/// Pins the Confluent.Kafka exception hierarchy: message-equals-error-reason,
/// inner-exception preservation, payloads carried by produce/consume exceptions,
/// and the Local_InvalidArg code used by (de)serialization failures.
/// </summary>
public class ExceptionsTests
{
    [Fact]
    public void KafkaException_MessageIsErrorReason()
    {
        var error = new Error(ErrorCode.Local_Transport, "broker unreachable");

        var ex = new KafkaException(error);

        Assert.Equal("broker unreachable", ex.Message);
        Assert.Same(error, ex.Error);
    }

    [Fact]
    public void KafkaException_WithoutReason_MessageIsCodeName()
    {
        var ex = new KafkaException(new Error(ErrorCode.Local_TimedOut));
        Assert.Equal("Local_TimedOut", ex.Message);
    }

    [Fact]
    public void KafkaException_PreservesInnerException()
    {
        var inner = new InvalidOperationException("root cause");

        var ex = new KafkaException(new Error(ErrorCode.Unknown), inner);

        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void ProduceException_CarriesDeliveryResult()
    {
        var result = new DeliveryResult<string, string>
        {
            Topic = "orders",
            Partition = 1,
            Offset = Offset.Unset,
            Status = PersistenceStatus.NotPersisted
        };
        var error = new Error(ErrorCode.MessageSizeTooLarge, "too large");

        var ex = new ProduceException<string, string>(error, result);

        Assert.Same(result, ex.DeliveryResult);
        Assert.Equal(ErrorCode.MessageSizeTooLarge, ex.Error.Code);
        Assert.Equal("too large", ex.Message);
    }

    [Fact]
    public void ConsumeException_FromResult_UsesUnknownError()
    {
        var result = new ConsumeResult<byte[], byte[]> { Topic = "orders", Partition = 0, Offset = 5 };

        var ex = new ConsumeException(result);

        Assert.Same(result, ex.ConsumptionResult);
        Assert.Equal(ErrorCode.Unknown, ex.Error.Code);
        Assert.Equal("Consume failed", ex.Message);
    }

    [Fact]
    public void ConsumeException_FromError_HasNoConsumptionResult()
    {
        var error = new Error(ErrorCode.OffsetOutOfRange, "offset gone");

        var ex = new ConsumeException(error);

        Assert.Null(ex.ConsumptionResult);
        Assert.Equal(ErrorCode.OffsetOutOfRange, ex.Error.Code);
        Assert.Equal("offset gone", ex.Message);
    }

    [Fact]
    public void SerializationException_UsesLocalInvalidArg()
    {
        var ex = new SerializationException("cannot serialize");

        Assert.Equal(ErrorCode.Local_InvalidArg, ex.Error.Code);
        Assert.Equal("cannot serialize", ex.Message);
    }

    [Fact]
    public void SerializationException_PreservesInnerException()
    {
        var inner = new FormatException("bad format");

        var ex = new SerializationException("cannot serialize", inner);

        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void DeserializationException_UsesLocalInvalidArg()
    {
        var ex = new DeserializationException("cannot deserialize");

        Assert.Equal(ErrorCode.Local_InvalidArg, ex.Error.Code);
        Assert.Equal("cannot deserialize", ex.Message);
    }

    [Fact]
    public void DeserializationException_PreservesInnerException()
    {
        var inner = new FormatException("bad bytes");

        var ex = new DeserializationException("cannot deserialize", inner);

        Assert.Same(inner, ex.InnerException);
    }
}
