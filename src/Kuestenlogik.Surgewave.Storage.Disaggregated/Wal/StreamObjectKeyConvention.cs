using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Wal;

/// <summary>
/// Canonical S3 key for a stream object. Format:
/// <c>topics/&lt;topic&gt;/&lt;partition&gt;/stream-&lt;baseOffset:D20&gt;.so</c>
/// — the same pattern Kafka tiered-storage tooling uses for sealed
/// segments, so an operator browsing the bucket can read the layout
/// without a separate doc page. <c>D20</c> zero-pads the offset so
/// lexicographic listing matches numeric order.
/// </summary>
public static class StreamObjectKeyConvention
{
    public static string Build(TopicPartition partition, long baseOffset) =>
        $"topics/{partition.Topic}/{partition.Partition}/stream-{baseOffset:D20}.so";
}
