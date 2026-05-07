namespace Kuestenlogik.Surgewave.Clustering.Bundles;

/// <summary>
/// Describes a planned transfer of a bundle from one broker to another.
/// </summary>
public sealed record BundleTransfer(string BundleId, int FromBrokerId, int ToBrokerId);
