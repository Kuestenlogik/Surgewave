namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// Provenance information for a record (frontend model).
/// </summary>
public sealed record ProvenanceInfo(List<ProvenanceStep> Steps);

/// <summary>
/// A single step in the provenance path.
/// </summary>
public sealed record ProvenanceStep(string NodeId, DateTimeOffset Timestamp);
