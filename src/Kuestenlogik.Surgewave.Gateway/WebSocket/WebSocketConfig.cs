using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket;

/// <summary>
/// Configuration for WebSocket support in the Gateway.
/// </summary>
public sealed class WebSocketConfig : IValidatableConfig
{
    /// <summary>
    /// Whether WebSocket support is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Heartbeat/ping interval in milliseconds.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int HeartbeatIntervalMs { get; set; } = 30000;

    /// <summary>
    /// Session timeout in milliseconds. Sessions without activity will be closed.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int SessionTimeoutMs { get; set; } = 120000;

    /// <summary>
    /// Maximum WebSocket message size in bytes.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxMessageSizeBytes { get; set; } = 1048576; // 1 MB

    /// <summary>
    /// Maximum number of subscriptions per WebSocket session.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxSubscriptionsPerSession { get; set; } = 100;

    /// <summary>
    /// Whether to enable batch message delivery for higher throughput.
    /// </summary>
    public bool BatchDeliveryEnabled { get; set; } = true;

    /// <summary>
    /// Maximum number of messages per batch when batch delivery is enabled.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int BatchDeliveryMaxSize { get; set; } = 100;

    /// <summary>
    /// Maximum wait time in milliseconds before sending a partial batch.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int BatchDeliveryMaxWaitMs { get; set; } = 100;

    /// <summary>
    /// Size of the send buffer channel per session.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int SendBufferCapacity { get; set; } = 1000;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (SessionTimeoutMs <= HeartbeatIntervalMs)
            errors.Add($"{nameof(SessionTimeoutMs)} must exceed {nameof(HeartbeatIntervalMs)}.");

        return errors;
    }
}
