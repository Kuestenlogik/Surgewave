using Kuestenlogik.Surgewave.Client.Native.Commands;
using Kuestenlogik.Surgewave.Client.Native.Commands.Cluster;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;

/// <summary>
/// Cluster management operations for Surgewave native client.
/// </summary>
public sealed class SurgewaveClusterOperations
{
    private readonly CommandExecutor _executor;

    internal SurgewaveClusterOperations(SurgewaveNativeClient client)
    {
        _executor = new CommandExecutor(client);
    }

    /// <summary>
    /// Get cluster information.
    /// </summary>
    public Task<ClusterInfo> GetClusterInfoAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetClusterInfoCommand(), cancellationToken);

    /// <summary>
    /// List all brokers in the cluster.
    /// </summary>
    public Task<List<BrokerInfo>> ListBrokersAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new ListBrokersCommand(), cancellationToken);

    /// <summary>
    /// Execute a partition reassignment plan.
    /// </summary>
    public Task<ReassignmentResult> AlterPartitionReassignmentsAsync(
        List<PartitionReassignmentRequest> reassignments,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new AlterPartitionReassignmentsCommand(reassignments), cancellationToken);

    /// <summary>
    /// List active partition reassignments.
    /// </summary>
    public Task<List<PartitionReassignmentStatus>> ListPartitionReassignmentsAsync(
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new ListPartitionReassignmentsCommand(), cancellationToken);

    /// <summary>
    /// Trigger log compaction on all compactable topics.
    /// </summary>
    public Task<CompactionResultInfo> TriggerLogCompactionAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new TriggerLogCompactionCommand(), cancellationToken);

    /// <summary>
    /// Get compaction status for all compactable topics.
    /// </summary>
    public Task<List<TopicCompactionStatus>> GetCompactionStatusAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetCompactionStatusCommand(), cancellationToken);

    /// <summary>
    /// Verify log integrity by validating CRC checksums.
    /// </summary>
    /// <param name="topic">Specific topic to verify. If null, verifies all topics.</param>
    /// <param name="partition">Specific partition to verify. Requires topic to be set.</param>
    /// <param name="maxCorruptedBatches">Stop after finding this many corrupted batches. 0 = no limit.</param>
    /// <param name="includeDetails">Include details for each corrupted batch.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Verification result with corruption details.</returns>
    public Task<LogVerificationInfo> VerifyLogIntegrityAsync(
        string? topic = null,
        int? partition = null,
        int maxCorruptedBatches = 0,
        bool includeDetails = true,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(
            new VerifyLogIntegrityCommand(topic, partition, maxCorruptedBatches, includeDetails),
            cancellationToken);
}
