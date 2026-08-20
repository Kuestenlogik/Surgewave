namespace Kuestenlogik.Surgewave.Schema.Registry.Lineage;

/// <summary>
/// Answers "who reads this topic right now?" for the impact analysis (#13). Implementations
/// query live control-plane state on demand — committed consumer-group offsets, deployed
/// Streams topologies, running connectors and pipelines — instead of recording events, so
/// nothing touches the produce/fetch hot path and the answer is never stale.
///
/// <para>The registry works without one: in the standalone Schema Registry no source is
/// registered, and the impact report simply carries no pipeline names — the compatibility
/// verdict itself never depends on lineage.</para>
/// </summary>
public interface ISchemaLineageSource
{
    /// <summary>Every known reader of <paramref name="topic"/>, with its onward sink topics
    /// where the reader's topology makes them visible (empty for plain consumer groups).</summary>
    IReadOnlyList<TopicReader> GetReaders(string topic);
}
