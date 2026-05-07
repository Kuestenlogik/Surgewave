using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Wire-level conformance for <c>ListTransactions</c> across all three
/// version-revisions: v0 baseline (state + producer-id filters), v1 adds
/// <c>DurationFilter</c> (KIP-994), v2 adds <c>TransactionalIdPattern</c>
/// (KIP-1152). The semantic suite (<c>Kip994ListTransactionsFiltersTests</c>)
/// drives the coordinator directly; these tests pin the on-the-wire shape so
/// a malformed parser change is caught before it ships.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip994ListTransactionsWireTests
{
    [Fact]
    public void V0_RoundtripsStateAndProducerIdFiltersOnly()
    {
        var original = new ListTransactionsRequest
        {
            ApiKey = ApiKey.ListTransactions,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "admin",
            StateFilters = ["Ongoing"],
            ProducerIdFilters = [42L, 100L],
        };

        var parsed = Roundtrip(original);

        Assert.Equal(["Ongoing"], parsed.StateFilters);
        Assert.Equal([42L, 100L], parsed.ProducerIdFilters);
        Assert.Equal(-1, parsed.DurationFilter); // not on the wire at v0
        Assert.Null(parsed.TransactionalIdPattern); // not on the wire at v0
    }

    [Fact]
    public void V1_RoundtripsDurationFilter()
    {
        var original = new ListTransactionsRequest
        {
            ApiKey = ApiKey.ListTransactions,
            ApiVersion = 1,
            CorrelationId = 2,
            ClientId = "admin",
            StateFilters = [],
            ProducerIdFilters = [],
            DurationFilter = 30_000,
        };

        var parsed = Roundtrip(original);

        Assert.Equal(30_000, parsed.DurationFilter);
        Assert.Null(parsed.TransactionalIdPattern);
    }

    [Fact]
    public void V2_RoundtripsTransactionalIdPattern()
    {
        var original = new ListTransactionsRequest
        {
            ApiKey = ApiKey.ListTransactions,
            ApiVersion = 2,
            CorrelationId = 3,
            ClientId = "admin",
            StateFilters = ["PrepareCommit", "PrepareAbort"],
            ProducerIdFilters = [777L],
            DurationFilter = 60_000,
            TransactionalIdPattern = "^orders-.*$",
        };

        var parsed = Roundtrip(original);

        Assert.Equal(["PrepareCommit", "PrepareAbort"], parsed.StateFilters);
        Assert.Equal([777L], parsed.ProducerIdFilters);
        Assert.Equal(60_000, parsed.DurationFilter);
        Assert.Equal("^orders-.*$", parsed.TransactionalIdPattern);
    }

    [Fact]
    public void V2_TransactionalIdPattern_NullSerialisesAsCompactNullString()
    {
        // A v2 client with no pattern set must still produce a wire-valid
        // request — null pattern is encoded as a compact "absent string"
        // marker by the writer.
        var original = new ListTransactionsRequest
        {
            ApiKey = ApiKey.ListTransactions,
            ApiVersion = 2,
            CorrelationId = 4,
            ClientId = "admin",
            DurationFilter = -1,
            TransactionalIdPattern = null,
        };

        var parsed = Roundtrip(original);

        Assert.Null(parsed.TransactionalIdPattern);
        Assert.Equal(-1, parsed.DurationFilter);
    }

    private static ListTransactionsRequest Roundtrip(ListTransactionsRequest original)
    {
        using var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();

        var reader = new KafkaProtocolReader(bytes);
        Assert.Equal((short)ApiKey.ListTransactions, reader.ReadInt16());
        Assert.Equal(original.ApiVersion, reader.ReadInt16());
        Assert.Equal(original.CorrelationId, reader.ReadInt32());
        Assert.Equal(original.ClientId, reader.ReadCompactString());
        reader.SkipTaggedFields();

        return ListTransactionsRequest.ReadFrom(
            reader,
            original.ApiVersion,
            original.CorrelationId,
            original.ClientId);
    }
}
