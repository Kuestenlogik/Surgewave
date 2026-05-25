namespace Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;

/// <summary>
/// Reassignment status code.
/// </summary>
public enum ReassignmentStatusCode : byte
{
    Pending = 0,
    Adding = 1,
    Syncing = 2,
    Completing = 3,
    Completed = 4,
    Failed = 5,
    Cancelled = 6
}
