using Kuestenlogik.Surgewave.Clustering.Cluster;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Bundles;

/// <summary>
/// Manages topic bundle lifecycle — initialization, assignment, splitting, and unloading.
/// Thread-safe for concurrent access.
/// </summary>
public sealed partial class BundleManager
{
    private readonly ILogger<BundleManager> _logger;
    private readonly ClusterState _clusterState;
    private readonly BundleConfig _config;
    private readonly List<TopicBundle> _bundles = [];
    private readonly object _lock = new();

    public BundleManager(
        ILogger<BundleManager> logger,
        ClusterState clusterState,
        BundleConfig? config = null)
    {
        _logger = logger;
        _clusterState = clusterState;
        _config = config ?? new BundleConfig();
    }

    /// <summary>
    /// All bundles currently managed.
    /// </summary>
    public IReadOnlyList<TopicBundle> AllBundles
    {
        get
        {
            lock (_lock)
            {
                return _bundles.ToList().AsReadOnly();
            }
        }
    }

    /// <summary>
    /// Initialize bundles by splitting the full uint32 hash range [0, 0xFFFFFFFF]
    /// into <paramref name="bundleCount"/> equal bundles.
    /// </summary>
    public void Initialize(int bundleCount = 4)
    {
        if (bundleCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(bundleCount), "Bundle count must be positive.");

        lock (_lock)
        {
            _bundles.Clear();

            ulong fullRange = (ulong)uint.MaxValue + 1; // 0x1_0000_0000
            ulong rangeSize = fullRange / (ulong)bundleCount;

            for (int i = 0; i < bundleCount; i++)
            {
                uint start = (uint)(rangeSize * (ulong)i);
                uint end = (i == bundleCount - 1)
                    ? uint.MaxValue
                    : (uint)(rangeSize * (ulong)(i + 1));

                // For the last bundle, use uint.MaxValue as end sentinel
                // but ContainsHash uses < comparison, so we need end to be past the last value.
                // We handle this: last bundle includes uint.MaxValue via special end value.
                if (i == bundleCount - 1)
                {
                    // Use 0 as a sentinel for "wraps to include MaxValue"
                    // Actually, let's keep it simple: end = uint.MaxValue and adjust ContainsHash
                    // for the last bucket to use <=. But TopicBundle.ContainsHash uses <.
                    // Instead, we'll set end to 0 for the last bucket to indicate full wrap.
                    // No — simplest: just set end = 0 which means the range wraps.
                    // Let's use a different approach: the last bundle gets end = 0 meaning
                    // it covers [start, 0xFFFFFFFF]. We handle this in GetBundleForTopic.
                    end = 0; // sentinel: means "through end of range"
                }

                var bundleId = $"ns-0x{start:X8}-0x{(end == 0 ? 0xFFFFFFFFu : end):X8}";

                _bundles.Add(new TopicBundle
                {
                    BundleId = bundleId,
                    HashRangeStart = start,
                    HashRangeEnd = end,
                    OwnerBrokerId = -1
                });
            }

            LogBundlesInitialized(bundleCount);
        }
    }

    /// <summary>
    /// Find which bundle owns a topic by hashing the topic name.
    /// </summary>
    public TopicBundle? GetBundleForTopic(string topicName)
    {
        uint hash = TopicBundle.HashTopic(topicName);

        lock (_lock)
        {
            foreach (var bundle in _bundles)
            {
                if (bundle.HashRangeEnd == 0)
                {
                    // Sentinel: this bundle covers [start, 0xFFFFFFFF]
                    if (hash >= bundle.HashRangeStart)
                        return bundle;
                }
                else if (bundle.ContainsHash(hash))
                {
                    return bundle;
                }
            }

            return null;
        }
    }

    /// <summary>
    /// Get all bundles owned by a specific broker.
    /// </summary>
    public List<TopicBundle> GetBundlesForBroker(int brokerId)
    {
        lock (_lock)
        {
            return _bundles.Where(b => b.OwnerBrokerId == brokerId).ToList();
        }
    }

    /// <summary>
    /// Assign a bundle to a broker.
    /// </summary>
    public void AssignBundle(string bundleId, int brokerId)
    {
        lock (_lock)
        {
            var bundle = _bundles.Find(b => b.BundleId == bundleId)
                ?? throw new InvalidOperationException($"Bundle '{bundleId}' not found.");

            bundle.OwnerBrokerId = brokerId;
            LogBundleAssigned(bundleId, brokerId);
        }
    }

    /// <summary>
    /// Split a bundle into two halves. The original bundle is replaced by
    /// a lower and upper half. Both halves inherit the original owner.
    /// </summary>
    public (TopicBundle lower, TopicBundle upper) SplitBundle(string bundleId)
    {
        lock (_lock)
        {
            if (_bundles.Count >= _config.MaxBundlesPerNamespace)
                throw new InvalidOperationException(
                    $"Cannot split: maximum bundle count ({_config.MaxBundlesPerNamespace}) reached.");

            var bundle = _bundles.Find(b => b.BundleId == bundleId)
                ?? throw new InvalidOperationException($"Bundle '{bundleId}' not found.");

            int index = _bundles.IndexOf(bundle);

            uint effectiveEnd = bundle.HashRangeEnd == 0 ? uint.MaxValue : bundle.HashRangeEnd;
            ulong rangeSize = (ulong)effectiveEnd - bundle.HashRangeStart;

            if (rangeSize < 2)
                throw new InvalidOperationException("Bundle hash range is too small to split.");

            uint midpoint = (uint)(bundle.HashRangeStart + rangeSize / 2);

            var lower = new TopicBundle
            {
                BundleId = $"ns-0x{bundle.HashRangeStart:X8}-0x{midpoint:X8}",
                HashRangeStart = bundle.HashRangeStart,
                HashRangeEnd = midpoint,
                OwnerBrokerId = bundle.OwnerBrokerId
            };

            var upper = new TopicBundle
            {
                BundleId = $"ns-0x{midpoint:X8}-0x{effectiveEnd:X8}",
                HashRangeStart = midpoint,
                HashRangeEnd = bundle.HashRangeEnd, // preserve sentinel 0 if it was the last bundle
                OwnerBrokerId = bundle.OwnerBrokerId
            };

            _bundles.RemoveAt(index);
            _bundles.Insert(index, lower);
            _bundles.Insert(index + 1, upper);

            LogBundleSplit(bundleId, lower.BundleId, upper.BundleId);

            return (lower, upper);
        }
    }

    /// <summary>
    /// Unload a bundle from its current broker (sets owner to -1, ready for reassignment).
    /// </summary>
    public void UnloadBundle(string bundleId)
    {
        lock (_lock)
        {
            var bundle = _bundles.Find(b => b.BundleId == bundleId)
                ?? throw new InvalidOperationException($"Bundle '{bundleId}' not found.");

            int previousOwner = bundle.OwnerBrokerId;
            bundle.OwnerBrokerId = -1;

            LogBundleUnloaded(bundleId, previousOwner);
        }
    }

    /// <summary>
    /// Build a load report for all bundles, counting topics from <see cref="ClusterState"/>.
    /// Message rate, byte rate, and session count are set to zero by default
    /// and should be populated by the caller with live metrics.
    /// </summary>
    public BundleLoadReport GetLoadReport()
    {
        lock (_lock)
        {
            var topicNames = _clusterState.Topics.Keys.ToList();
            var bundleLoads = new List<BundleLoad>(_bundles.Count);

            string? hottestId = null;
            int maxTopicCount = 0;

            foreach (var bundle in _bundles)
            {
                int topicCount = 0;
                foreach (var topic in topicNames)
                {
                    uint hash = TopicBundle.HashTopic(topic);
                    bool inRange = bundle.HashRangeEnd == 0
                        ? hash >= bundle.HashRangeStart
                        : bundle.ContainsHash(hash);

                    if (inRange)
                        topicCount++;
                }

                var load = new BundleLoad
                {
                    BundleId = bundle.BundleId,
                    OwnerBrokerId = bundle.OwnerBrokerId,
                    TopicCount = topicCount
                };

                bundleLoads.Add(load);

                if (topicCount > maxTopicCount)
                {
                    maxTopicCount = topicCount;
                    hottestId = bundle.BundleId;
                }
            }

            bool splitRecommended = maxTopicCount > _config.MaxTopicsPerBundle;

            return new BundleLoadReport
            {
                Bundles = bundleLoads,
                HottestBundleId = hottestId,
                SplitRecommended = splitRecommended
            };
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Initialized {Count} bundles across hash range")]
    private partial void LogBundlesInitialized(int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Bundle '{BundleId}' assigned to broker {BrokerId}")]
    private partial void LogBundleAssigned(string bundleId, int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Bundle '{BundleId}' split into '{LowerId}' and '{UpperId}'")]
    private partial void LogBundleSplit(string bundleId, string lowerId, string upperId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Bundle '{BundleId}' unloaded from broker {PreviousOwner}")]
    private partial void LogBundleUnloaded(string bundleId, int previousOwner);
}
