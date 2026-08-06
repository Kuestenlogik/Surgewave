using Kuestenlogik.Surgewave.Core.Dlq;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Dlq;

/// <summary>
/// What the DLQ does by default, when nobody configured it (#126).
///
/// <para>A dead-letter queue writes a second copy of a record into a second topic — with its own
/// retention, its own ACLs, and typically a wider audience. Three defaults made that copy
/// unconditional: the payload was a required field, a missing DLQ topic was created unasked, and
/// the read that fetched the record back was bounded by a constant rather than by what the
/// destination accepts.</para>
///
/// <para>The point of these tests is that the safe posture is the DEFAULT one. An operator who
/// wants payloads duplicated can say so; nobody should get there by not deciding.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DlqSafeDefaultsTests
{
    [Fact]
    public void ByDefault_ThePayloadIsNotCopied()
    {
        var config = new DlqConfig();

        Assert.False(config.CopyRecordValue);
    }

    [Fact]
    public void ByDefault_TopicsAreNotCreatedUnasked()
    {
        var config = new DlqConfig();

        Assert.False(config.AutoCreateTopics);
    }

    [Fact]
    public void DlqTopicName_UsesAPrefix()
    {
        // A prefix makes the DLQ topics a namespace: "dlq.*" is one ACL and one quota. A suffix
        // cannot express that.
        var config = new DlqConfig();

        Assert.Equal("dlq.orders", config.GetDlqTopicName("orders"));
    }

    [Fact]
    public void DlqTopicName_HonoursAConfiguredSuffix()
    {
        // An existing deployment keeps writing where it always did — upgrading must not silently
        // relocate a topic that already holds data.
        var config = new DlqConfig { TopicSuffix = ".DLQ" };

        Assert.Equal("orders.DLQ", config.GetDlqTopicName("orders"));
    }

    [Fact]
    public void ARecordWithoutAPayload_RoundTrips()
    {
        // The serialized form has to distinguish "not kept" from "empty", or a consumer cannot tell
        // a suppressed payload from a zero-length one.
        var record = new DlqRecord
        {
            OriginalTopic = "orders",
            OriginalPartition = 0,
            OriginalOffset = 42,
            OriginalValue = null,
            ExceptionType = "System.FormatException",
            ExceptionMessage = "unparseable",
            SourceName = "orders-sink",
            SourceType = "connect-sink"
        };

        var restored = DlqRecordSerializer.Deserialize(DlqRecordSerializer.Serialize(record));

        Assert.Null(restored.OriginalValue);
        Assert.Equal("orders", restored.OriginalTopic);
        Assert.Equal(42, restored.OriginalOffset);
        Assert.Equal("System.FormatException", restored.ExceptionType);
    }

    [Fact]
    public void ARecordWithAPayload_StillRoundTrips()
    {
        var payload = new byte[] { 1, 2, 3, 4 };
        var record = new DlqRecord
        {
            OriginalTopic = "orders",
            OriginalPartition = 0,
            OriginalOffset = 42,
            OriginalValue = payload,
            ExceptionType = "System.FormatException",
            ExceptionMessage = "unparseable",
            SourceName = "orders-sink",
            SourceType = "connect-sink"
        };

        var restored = DlqRecordSerializer.Deserialize(DlqRecordSerializer.Serialize(record));

        Assert.Equal(payload, restored.OriginalValue);
    }
}
