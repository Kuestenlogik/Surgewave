using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket;

/// <summary>
/// Configuration for the WebSocket protocol adapter.
/// Bound from the "Surgewave:WebSocket" configuration section.
/// </summary>
public sealed class WebSocketConfig : IValidatableConfig
{
    /// <summary>
    /// Configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "Surgewave:WebSocket";

    /// <summary>
    /// Enable the WebSocket protocol adapter. Default: false.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Base URL path for WebSocket endpoints. Default: "/ws".
    /// Endpoints: /ws/produce/{topic}, /ws/consume/{topic}, /ws/subscribe
    /// </summary>
    [Required]
    [RegularExpression("^/.*", ErrorMessage = "Path must start with '/'.")]
    public string Path { get; set; } = "/ws";

    /// <summary>
    /// Maximum WebSocket message size in bytes. Default: 1048576 (1 MB).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxMessageSizeBytes { get; set; } = 1_048_576;

    /// <summary>
    /// Interval for sending WebSocket ping frames to keep connections alive. Default: 30 seconds.
    /// </summary>
    public TimeSpan PingInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum number of concurrent WebSocket connections. Default: 5000.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxConnections { get; set; } = 5000;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (PingInterval <= TimeSpan.Zero)
            errors.Add($"{nameof(PingInterval)}: must be positive.");

        return errors;
    }
}
