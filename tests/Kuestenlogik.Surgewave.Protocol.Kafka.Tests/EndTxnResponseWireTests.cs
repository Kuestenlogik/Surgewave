using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// EndTxn v5 (transactions V2) answers with the producer id and epoch the client continues with (#190).
/// Without them the response was ten bytes short of what a Java client reads at v5, and every
/// commitTransaction() of a V2 client failed on the response.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class EndTxnResponseWireTests
{
    private static KafkaProtocolReader Write(short version, long producerId, short producerEpoch)
    {
        var writer = new KafkaProtocolWriter();
        new EndTxnResponse
        {
            ApiVersion = version,
            CorrelationId = 7,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
        }.WriteTo(writer);
        return new KafkaProtocolReader(writer.ToArray());
    }

    [Fact]
    public void V5_CarriesTheProducerIdAndEpoch()
    {
        var reader = Write(5, producerId: 4242, producerEpoch: 9);

        Assert.Equal(7, reader.ReadInt32());          // correlation id
        Assert.Equal(0, reader.ReadVarInt());         // response header tagged fields
        Assert.Equal(0, reader.ReadInt32());          // throttle time
        Assert.Equal(0, reader.ReadInt16());          // error code
        Assert.Equal(4242L, reader.ReadInt64());      // producer id
        Assert.Equal((short)9, reader.ReadInt16());   // producer epoch
        Assert.Equal(0, reader.ReadVarInt());         // tagged fields
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void V4_HasNoProducerFields()
    {
        var reader = Write(4, producerId: 4242, producerEpoch: 9);

        Assert.Equal(7, reader.ReadInt32());
        Assert.Equal(0, reader.ReadVarInt());
        Assert.Equal(0, reader.ReadInt32());
        Assert.Equal(0, reader.ReadInt16());
        Assert.Equal(0, reader.ReadVarInt());
        Assert.Equal(0, reader.Remaining);
    }
}
