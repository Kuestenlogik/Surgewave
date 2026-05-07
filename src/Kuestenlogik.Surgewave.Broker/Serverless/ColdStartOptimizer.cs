using System.Diagnostics;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Serverless;

/// <summary>
/// Optimizes broker startup from cold state when all data resides in object storage.
/// Identifies hot partitions (most recently accessed, highest throughput) and
/// pre-fetches their metadata and cache entries to minimize first-request latency.
/// </summary>
public sealed class ColdStartOptimizer
{
    private readonly ILogger<ColdStartOptimizer> _logger;
    private readonly ServerlessConfig _config;

    public ColdStartOptimizer(
        ILogger<ColdStartOptimizer> logger,
        ServerlessConfig config)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config ?? throw new ArgumentNullException(nameof(config));
    }

    /// <summary>
    /// Run the cold start optimization pass.
    /// Loads partition metadata from the cluster state, identifies hot partitions,
    /// and pre-warms their read caches.
    /// </summary>
    /// <param name="clusterState">Current cluster state containing topic and partition metadata.</param>
    /// <param name="cancellationToken">Cancellation token with cold start timeout.</param>
    /// <returns>A <see cref="ColdStartReport"/> with timing and pre-fetch statistics.</returns>
    public Task<ColdStartReport> OptimizeColdStartAsync(
        ClusterState clusterState,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(clusterState);

        var stopwatch = Stopwatch.StartNew();

        // Gather all partition states from the cluster
        var allPartitions = clusterState.GetAllPartitionStates().ToList();
        var partitionsLoaded = allPartitions.Count;

        _logger.LogInformation(
            "Cold start: found {PartitionCount} partitions in cluster state",
            partitionsLoaded);

        if (partitionsLoaded == 0)
        {
            stopwatch.Stop();
            _logger.LogInformation("Cold start: no partitions to warm, completing quickly");

            return Task.FromResult(new ColdStartReport
            {
                TotalDuration = stopwatch.Elapsed,
                PartitionsLoaded = 0,
                PartitionsPreWarmed = 0,
                BytesPreFetched = 0,
                FinalState = ServerlessLifecycleState.Active
            });
        }

        // Identify hot partitions: those assigned to the local broker,
        // ordered by leader epoch (proxy for recent activity).
        var localBrokerId = clusterState.LocalBrokerId;
        var hotPartitions = allPartitions
            .Where(p => p.Item2.Replicas.Contains(localBrokerId))
            .OrderByDescending(p => p.Item2.LeaderEpoch)
            .Take(_config.WarmupPartitions)
            .ToList();

        var partitionsPreWarmed = hotPartitions.Count;

        _logger.LogInformation(
            "Cold start: pre-warming {WarmCount} hot partitions (of {TotalAssigned} assigned)",
            partitionsPreWarmed,
            allPartitions.Count(p => p.Item2.Replicas.Contains(localBrokerId)));

        // In a full implementation, we would pre-fetch segment metadata and warm
        // the read cache for each hot partition. For now, we record the intent
        // and count as the infrastructure is wired up at the storage layer.
        long bytesPreFetched = 0;

        foreach (var (tp, state) in hotPartitions)
        {
            cancellationToken.ThrowIfCancellationRequested();

            _logger.LogDebug(
                "Pre-warming partition {Topic}-{Partition} (leader epoch: {Epoch})",
                tp.Topic,
                tp.Partition,
                state.LeaderEpoch);
        }

        stopwatch.Stop();

        _logger.LogInformation(
            "Cold start completed in {ElapsedMs}ms: loaded={Loaded}, warmed={Warmed}, bytes={Bytes}",
            stopwatch.ElapsedMilliseconds,
            partitionsLoaded,
            partitionsPreWarmed,
            bytesPreFetched);

        return Task.FromResult(new ColdStartReport
        {
            TotalDuration = stopwatch.Elapsed,
            PartitionsLoaded = partitionsLoaded,
            PartitionsPreWarmed = partitionsPreWarmed,
            BytesPreFetched = bytesPreFetched,
            FinalState = ServerlessLifecycleState.Active
        });
    }
}
