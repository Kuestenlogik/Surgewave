using Kuestenlogik.Surgewave.Clustering.Cluster;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Bundles;

/// <summary>
/// Decides which bundles to split based on load thresholds and generates
/// transfer plans to redistribute bundles across brokers.
/// </summary>
public sealed partial class BundleRebalancer
{
    private readonly ILogger<BundleRebalancer> _logger;
    private readonly BundleManager _bundleManager;
    private readonly ClusterState _clusterState;
    private readonly BundleConfig _config;

    public BundleRebalancer(
        ILogger<BundleRebalancer> logger,
        BundleManager bundleManager,
        ClusterState clusterState,
        BundleConfig? config = null)
    {
        _logger = logger;
        _bundleManager = bundleManager;
        _clusterState = clusterState;
        _config = config ?? new BundleConfig();
    }

    /// <summary>
    /// Check all bundles and return the IDs of those that exceed load thresholds
    /// and should be split.
    /// </summary>
    public List<string> FindBundlesToSplit()
    {
        var report = _bundleManager.GetLoadReport();
        var result = new List<string>();

        foreach (var load in report.Bundles)
        {
            bool shouldSplit = load.TopicCount > _config.MaxTopicsPerBundle
                || load.MessageRatePerSecond > _config.MaxMessageRatePerBundle
                || load.ByteRatePerSecond > _config.MaxBandwidthMbPerBundle * 1024 * 1024;

            if (shouldSplit)
            {
                result.Add(load.BundleId);
            }
        }

        return result;
    }

    /// <summary>
    /// Find overloaded brokers and generate a plan to move bundles from them
    /// to under-loaded brokers, balancing the bundle count across the cluster.
    /// </summary>
    public List<BundleTransfer> GenerateTransferPlan()
    {
        var aliveBrokerIds = _clusterState.Brokers.Keys.ToList();
        if (aliveBrokerIds.Count < 2)
            return [];

        var allBundles = _bundleManager.AllBundles;
        var assignedBundles = allBundles.Where(b => b.OwnerBrokerId >= 0).ToList();

        if (assignedBundles.Count == 0)
            return [];

        // Count bundles per broker
        var bundlesPerBroker = new Dictionary<int, List<TopicBundle>>();
        foreach (var brokerId in aliveBrokerIds)
        {
            bundlesPerBroker[brokerId] = [];
        }

        foreach (var bundle in assignedBundles)
        {
            if (bundlesPerBroker.TryGetValue(bundle.OwnerBrokerId, out var brokerBundles))
            {
                brokerBundles.Add(bundle);
            }
        }

        double idealCount = (double)assignedBundles.Count / aliveBrokerIds.Count;
        int targetMax = (int)Math.Ceiling(idealCount);
        int targetMin = (int)Math.Floor(idealCount);

        var transfers = new List<BundleTransfer>();

        // Find overloaded brokers (more than targetMax) and under-loaded brokers (less than targetMin)
        var overloaded = bundlesPerBroker
            .Where(kv => kv.Value.Count > targetMax)
            .OrderByDescending(kv => kv.Value.Count)
            .ToList();

        var underloaded = bundlesPerBroker
            .Where(kv => kv.Value.Count < targetMin)
            .OrderBy(kv => kv.Value.Count)
            .ToList();

        // Also consider brokers at targetMin if we have brokers above targetMax
        if (underloaded.Count == 0 && overloaded.Count > 0)
        {
            underloaded = bundlesPerBroker
                .Where(kv => kv.Value.Count < targetMax)
                .OrderBy(kv => kv.Value.Count)
                .ToList();
        }

        int underIdx = 0;
        foreach (var (fromBrokerId, fromBundles) in overloaded)
        {
            while (fromBundles.Count > targetMax && underIdx < underloaded.Count)
            {
                var (toBrokerId, toBundles) = underloaded[underIdx];
                if (toBundles.Count >= targetMax)
                {
                    underIdx++;
                    continue;
                }

                var bundleToMove = fromBundles[^1];
                transfers.Add(new BundleTransfer(bundleToMove.BundleId, fromBrokerId, toBrokerId));
                fromBundles.RemoveAt(fromBundles.Count - 1);
                toBundles.Add(bundleToMove);

                if (toBundles.Count >= targetMax)
                    underIdx++;
            }
        }

        if (transfers.Count > 0)
        {
            LogTransferPlanGenerated(transfers.Count);
        }

        return transfers;
    }

    /// <summary>
    /// Perform a full rebalancing cycle: split hot bundles and redistribute.
    /// </summary>
    public void MaybeRebalanceBundles()
    {
        // Step 1: Split hot bundles if auto-split is enabled
        if (_config.AutoSplitEnabled)
        {
            var bundlesToSplit = FindBundlesToSplit();
            foreach (var bundleId in bundlesToSplit)
            {
                try
                {
                    var (lower, upper) = _bundleManager.SplitBundle(bundleId);
                    LogBundleAutoSplit(bundleId);

                    if (_config.AutoUnloadAfterSplit)
                    {
                        _bundleManager.UnloadBundle(lower.BundleId);
                        _bundleManager.UnloadBundle(upper.BundleId);
                    }
                }
                catch (InvalidOperationException ex)
                {
                    LogSplitFailed(bundleId, ex.Message);
                }
            }
        }

        // Step 2: Generate and execute transfer plan
        var transfers = GenerateTransferPlan();
        foreach (var transfer in transfers)
        {
            _bundleManager.UnloadBundle(transfer.BundleId);
            _bundleManager.AssignBundle(transfer.BundleId, transfer.ToBrokerId);
            LogBundleTransferred(transfer.BundleId, transfer.FromBrokerId, transfer.ToBrokerId);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Generated transfer plan with {Count} bundle moves")]
    private partial void LogTransferPlanGenerated(int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Auto-split bundle '{BundleId}' due to load threshold")]
    private partial void LogBundleAutoSplit(string bundleId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to split bundle '{BundleId}': {Reason}")]
    private partial void LogSplitFailed(string bundleId, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "Transferred bundle '{BundleId}' from broker {From} to broker {To}")]
    private partial void LogBundleTransferred(string bundleId, int from, int to);
}
