using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// Configuration for the AMQP 0.9.1 protocol adapter.
/// Bound from the "Surgewave:Amqp" configuration section.
/// </summary>
public sealed class AmqpConfig : IValidatableConfig
{
    /// <summary>
    /// Configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "Surgewave:Amqp";

    /// <summary>
    /// Enable the AMQP 0.9.1 protocol adapter. Default: false.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// TCP port to listen for AMQP connections. Default: 5672.
    /// </summary>
    [Range(1, 65535)]
    public int Port { get; set; } = 5672;

    /// <summary>
    /// Maximum number of channels per AMQP connection. Default: 256.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxChannels { get; set; } = 256;

    /// <summary>
    /// Heartbeat interval in seconds. Connections that miss two consecutive
    /// heartbeats are considered dead. Default: 60.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int HeartbeatInterval { get; set; } = 60;

    /// <summary>
    /// Maximum number of concurrent AMQP connections. Default: 1000.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxConnections { get; set; } = 1000;

    /// <summary>
    /// Maximum AMQP frame size in bytes. Default: 131072 (128 KB).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxFrameSize { get; set; } = 131_072;

    /// <summary>
    /// Allow anonymous connections (no SASL credentials). Default: true.
    /// </summary>
    public bool AllowAnonymous { get; set; } = true;

    /// <summary>
    /// Virtual host accepted by the adapter. Default: "/".
    /// </summary>
    [Required]
    [MinLength(1)]
    public string VirtualHost { get; set; } = "/";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
