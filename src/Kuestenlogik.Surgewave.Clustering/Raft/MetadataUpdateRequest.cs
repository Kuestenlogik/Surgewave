namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Controller RPC: MetadataUpdate request for non-Raft metadata propagation.
/// Sent from controller to all brokers when metadata changes.
/// </summary>
public sealed record MetadataUpdateRequest(
    int ControllerId,
    int ControllerEpoch,
    long MetadataVersion,
    MetadataCommandType CommandType,
    byte[] CommandData,
    long Timestamp
);
