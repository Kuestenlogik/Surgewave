using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Transport;

namespace Kuestenlogik.Surgewave.Edge;

/// <summary>
/// Configuration for edge-to-cloud message synchronization.
/// Controls which topics are synced, sync direction, batching, and offline buffering.
/// </summary>
public sealed class EdgeSyncConfig : IValidatableConfig
{
    /// <summary>
    /// Unique identifier for this edge node. Defaults to the machine name.
    /// Used as provenance header (<c>surgewave-edge-id</c>) on synced messages.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string EdgeId { get; set; } = Environment.MachineName;

    /// <summary>
    /// The cloud broker address in <c>host:port</c> format.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string CloudBrokerAddress { get; set; } = "localhost:9092";

    /// <summary>
    /// Transport used to reach the cloud broker. Defaults to <see cref="SurgewaveTransportType.Tcp"/>.
    /// Set to <see cref="SurgewaveTransportType.Quic"/> on edge nodes sitting on lossy or
    /// high-latency links (wifi, mobile, satellite) — QUIC delivers better throughput
    /// under packet loss and survives NAT rebinding / network handoffs without
    /// reconnect surgewaves. Requires msquic on the edge host and a QUIC-enabled cloud broker.
    /// </summary>
    public SurgewaveTransportType CloudTransport { get; set; } = SurgewaveTransportType.Tcp;

    /// <summary>
    /// List of topic patterns to sync. Use <c>*</c> to sync all topics.
    /// Exact topic names and simple wildcard patterns are supported.
    /// </summary>
    public List<string> SyncTopics { get; set; } = ["*"];

    /// <summary>
    /// The direction of message synchronization.
    /// </summary>
    public SyncDirection Direction { get; set; } = SyncDirection.EdgeToCloud;

    /// <summary>
    /// Interval in seconds between sync attempts.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int SyncIntervalSeconds { get; set; } = 30;

    /// <summary>
    /// Maximum number of messages to send per sync batch.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxBatchSize { get; set; } = 1000;

    /// <summary>
    /// Maximum offline buffer size in megabytes.
    /// When the edge broker exceeds this size, the oldest messages may be dropped.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int OfflineBufferMaxMb { get; set; } = 100;

    /// <summary>
    /// Whether to compress messages during sync transfer.
    /// </summary>
    public bool CompressSync { get; set; } = true;

    /// <summary>
    /// File path for persisting sync state (offsets, last sync time).
    /// State survives edge restarts.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string OfflineStateFile { get; set; } = "edge-sync-state.json";

    /// <summary>
    /// Timeout in milliseconds when checking cloud broker connectivity.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int ConnectivityTimeoutMs { get; set; } = 5000;

    /// <summary>
    /// Maximum number of consecutive sync failures before backing off exponentially.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxConsecutiveFailures { get; set; } = 5;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (SyncTopics.Count == 0)
            errors.Add($"{nameof(SyncTopics)}: must contain at least one entry (use \"*\" to sync all).");

        if (SyncTopics.Any(string.IsNullOrWhiteSpace))
            errors.Add($"{nameof(SyncTopics)}: must not contain empty entries.");

        return errors;
    }
}
