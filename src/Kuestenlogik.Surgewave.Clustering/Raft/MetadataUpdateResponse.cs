namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Controller RPC: MetadataUpdate response.
/// </summary>
public sealed record MetadataUpdateResponse(
    int BrokerId,
    short ErrorCode,
    long MetadataVersion
);
