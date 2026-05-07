namespace Kuestenlogik.Surgewave.Core.Lineage;

/// <summary>
/// The type of node in a lineage graph.
/// </summary>
public enum LineageNodeType
{
    /// <summary>A Kafka-style topic.</summary>
    Topic,

    /// <summary>A message producer (client).</summary>
    Producer,

    /// <summary>A message consumer (consumer group).</summary>
    Consumer,

    /// <summary>A Streams application that reads from source topics and writes to sink topics.</summary>
    StreamsApp,

    /// <summary>A Connect connector that sources or sinks data.</summary>
    Connector
}
