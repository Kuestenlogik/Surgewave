namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Provenance information for a record tracing its path through pipeline nodes.
/// </summary>
public sealed record ProvenanceInfo(List<ProvenanceStep> Steps);

/// <summary>
/// A single step in the provenance path.
/// </summary>
public sealed record ProvenanceStep(string NodeId, DateTimeOffset Timestamp);
