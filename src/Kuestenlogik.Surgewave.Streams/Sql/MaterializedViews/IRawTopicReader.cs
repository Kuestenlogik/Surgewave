namespace Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;

/// <summary>
/// Abstraction over the broker's log layer used by the materialized view
/// refresh loop. Implementations read all messages currently available in
/// a topic across all of its partitions, in offset order per partition.
///
/// Kept as an interface so that the refresh service is independently
/// testable without spinning up a real broker.
/// </summary>
public interface IRawTopicReader
{
    /// <summary>
    /// Reads all currently committed messages of a topic.
    /// Implementations should be lazy where possible.
    /// </summary>
    /// <param name="topicName">The topic to read.</param>
    /// <returns>An enumerable of raw messages, ordered by partition and then by offset.</returns>
    IEnumerable<RawTopicMessage> ReadTopic(string topicName);
}
