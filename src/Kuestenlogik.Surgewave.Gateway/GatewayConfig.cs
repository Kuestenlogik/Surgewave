using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Gateway.WebSocket;

namespace Kuestenlogik.Surgewave.Gateway;

/// <summary>
/// Configuration for the Surgewave Gateway service.
/// Supports both single-cluster (legacy) and multi-cluster configurations.
/// </summary>
public sealed class GatewayConfig : IValidatableConfig
{
    // ============ Multi-Cluster Configuration ============

    /// <summary>
    /// The default cluster ID to use when no cluster is specified in the request.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string DefaultCluster { get; set; } = "surgewave-cluster";

    /// <summary>
    /// Dictionary of cluster configurations keyed by cluster ID.
    /// </summary>
    public Dictionary<string, ClusterConfig> Clusters { get; set; } = new();

    // ============ Shared Configuration ============

    /// <summary>
    /// Connection timeout in milliseconds.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int ConnectionTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Request timeout in milliseconds.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int RequestTimeoutMs { get; set; } = 30000;

    // ============ WebSocket Configuration ============

    /// <summary>
    /// WebSocket configuration for real-time streaming.
    /// </summary>
    public WebSocketConfig WebSocket { get; set; } = new();

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));
        errors.AddRange(WebSocket.Validate());

        if (Clusters.Count > 0 && !Clusters.ContainsKey(DefaultCluster))
        {
            errors.Add($"{nameof(DefaultCluster)} '{DefaultCluster}' is not present in {nameof(Clusters)}.");
        }

        return errors;
    }
}
