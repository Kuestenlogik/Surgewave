using Kuestenlogik.Surgewave.Core.Dlq;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Dlq;

/// <summary>
/// The payload-copy decision belongs to the router, not to whoever built the record (#126).
///
/// <para>Callers may always hand us the value — they have it, and asking them to strip it would put
/// the policy in every call site. What must not happen is that supplying it causes it to be
/// stored.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DlqRouterCopyPolicyTests
{
    [Fact]
    public async Task WithoutCopyEnabled_TheValueAndHeadersNeverReachTheTopic()
    {
        var producer = new CapturingProducer();
        var router = new DlqRouter(new DlqConfig { AutoCreateTopics = false }, producer);

        Assert.True(await router.RouteAsync(RecordWithPayload()));

        var written = DlqRecordSerializer.Deserialize(producer.LastValue!);
        Assert.Null(written.OriginalValue);
        Assert.Null(written.OriginalHeaders);
    }

    [Fact]
    public async Task WithoutCopyEnabled_TheIdentifyingMetadataIsStillThere()
    {
        // Suppressing the payload must not gut the record: what failed, where, why and how often is
        // the entire reason the DLQ exists.
        var producer = new CapturingProducer();
        var router = new DlqRouter(new DlqConfig(), producer);

        await router.RouteAsync(RecordWithPayload());

        var written = DlqRecordSerializer.Deserialize(producer.LastValue!);
        Assert.Equal("orders", written.OriginalTopic);
        Assert.Equal(7, written.OriginalPartition);
        Assert.Equal(99, written.OriginalOffset);
        Assert.Equal("System.FormatException", written.ExceptionType);
        Assert.Equal(3, written.AttemptCount);
    }

    [Fact]
    public async Task TheKeyIsKept_ItIsTheCorrelationHandle()
    {
        var producer = new CapturingProducer();
        var router = new DlqRouter(new DlqConfig(), producer);

        await router.RouteAsync(RecordWithPayload());

        Assert.Equal(new byte[] { 9, 9 }, producer.LastKey);
    }

    [Fact]
    public async Task WithCopyEnabled_ThePayloadIsWritten()
    {
        var producer = new CapturingProducer();
        var router = new DlqRouter(new DlqConfig { CopyRecordValue = true }, producer);

        await router.RouteAsync(RecordWithPayload());

        var written = DlqRecordSerializer.Deserialize(producer.LastValue!);
        Assert.Equal(new byte[] { 1, 2, 3 }, written.OriginalValue);
        Assert.NotNull(written.OriginalHeaders);
    }

    [Fact]
    public async Task WithoutAutoCreate_NoTopicIsCreated()
    {
        var producer = new CapturingProducer();
        var router = new DlqRouter(new DlqConfig(), producer);

        await router.RouteAsync(RecordWithPayload());

        Assert.Equal(0, producer.EnsureCalls);
        Assert.Equal(1, producer.ProduceCalls);
    }

    [Fact]
    public async Task WithAutoCreate_TheTopicIsEnsuredOnce()
    {
        var producer = new CapturingProducer();
        var router = new DlqRouter(new DlqConfig { AutoCreateTopics = true }, producer);

        await router.RouteAsync(RecordWithPayload());
        await router.RouteAsync(RecordWithPayload());

        Assert.Equal(1, producer.EnsureCalls);
        Assert.Equal(2, producer.ProduceCalls);
    }

    private static DlqRecord RecordWithPayload() => new()
    {
        OriginalTopic = "orders",
        OriginalPartition = 7,
        OriginalOffset = 99,
        OriginalKey = [9, 9],
        OriginalValue = [1, 2, 3],
        OriginalHeaders = new Dictionary<string, byte[]> { ["trace-id"] = [4, 5] },
        ExceptionType = "System.FormatException",
        ExceptionMessage = "unparseable",
        SourceName = "orders-sink",
        SourceType = "connect-sink",
        AttemptCount = 3
    };

    private sealed class CapturingProducer : IDlqProducer
    {
        public byte[]? LastKey { get; private set; }
        public byte[]? LastValue { get; private set; }
        public int ProduceCalls { get; private set; }
        public int EnsureCalls { get; private set; }

        public Task ProduceAsync(string topic, byte[]? key, byte[] value, CancellationToken cancellationToken = default)
        {
            LastKey = key;
            LastValue = value;
            ProduceCalls++;
            return Task.CompletedTask;
        }

        public Task EnsureTopicExistsAsync(
            string topic, int partitionCount, Dictionary<string, string>? config,
            CancellationToken cancellationToken = default)
        {
            EnsureCalls++;
            return Task.CompletedTask;
        }
    }
}
