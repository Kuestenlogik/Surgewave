using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// AddPartitionsToTxn v3 (flexible) as a Java client reads it: every partition result is a struct with its own
/// tagged fields. The writer left them out, and a Java producer (4.1) failed its first transaction with
/// "Buffer underflow while parsing response for request with header RequestHeader(apiKey=ADD_PARTITIONS_TO_TXN,
/// apiVersion=3 ...)".
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class AddPartitionsToTxnResponseWireTests
{
    [Fact]
    public void V3_EveryPartitionResultCarriesItsTaggedFields()
    {
        var writer = new KafkaProtocolWriter();
        new AddPartitionsToTxnResponse
        {
            ApiVersion = 3,
            CorrelationId = 7,
            Results = new Dictionary<string, List<AddPartitionsToTxnResponse.PartitionResult>>
            {
                ["orders"] =
                [
                    new() { Partition = 0, ErrorCode = ErrorCode.None },
                    new() { Partition = 1, ErrorCode = ErrorCode.ConcurrentTransactions },
                ],
            },
        }.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray());

        Assert.Equal(7, reader.ReadInt32());              // correlation id
        Assert.Equal(0, reader.ReadVarInt());             // response header tagged fields
        Assert.Equal(0, reader.ReadInt32());              // throttle time
        Assert.Equal(2, reader.ReadVarInt());             // one topic (compact array: count + 1)
        Assert.Equal("orders", reader.ReadCompactString());
        Assert.Equal(3, reader.ReadVarInt());             // two partitions
        Assert.Equal(0, reader.ReadInt32());
        Assert.Equal(0, reader.ReadInt16());
        Assert.Equal(0, reader.ReadVarInt());             // partition result tagged fields
        Assert.Equal(1, reader.ReadInt32());
        Assert.Equal((short)ErrorCode.ConcurrentTransactions, reader.ReadInt16());
        Assert.Equal(0, reader.ReadVarInt());             // partition result tagged fields
        Assert.Equal(0, reader.ReadVarInt());             // topic result tagged fields
        Assert.Equal(0, reader.ReadVarInt());             // response tagged fields
        Assert.Equal(0, reader.Remaining);
    }
}
