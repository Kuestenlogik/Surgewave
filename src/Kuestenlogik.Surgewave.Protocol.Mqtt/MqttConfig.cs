using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt;

/// <summary>
/// Configuration for the MQTT protocol adapter.
/// Bound from the "Surgewave:Mqtt" configuration section.
/// </summary>
public sealed class MqttConfig : IValidatableConfig
{
    /// <summary>
    /// Configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "Surgewave:Mqtt";

    /// <summary>
    /// Enable the MQTT protocol adapter. Default: false.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// TCP port to listen for MQTT connections. Default: 1883.
    /// </summary>
    [Range(1, 65535)]
    public int Port { get; set; } = 1883;

    /// <summary>
    /// Prefix applied to MQTT topics when mapping to Surgewave topics.
    /// MQTT '/' separators are converted to '.' for Surgewave topic naming.
    /// For example, MQTT topic "sensors/temp" maps to Surgewave topic "mqtt.sensors.temp".
    /// </summary>
    [Required]
    public string TopicPrefix { get; set; } = "mqtt.";

    /// <summary>
    /// Maximum number of concurrent MQTT client connections. Default: 1000.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxClients { get; set; } = 1000;

    /// <summary>
    /// Maximum MQTT message payload size in bytes. Default: 262144 (256 KB).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxMessageSizeBytes { get; set; } = 262_144;

    /// <summary>
    /// Allow anonymous MQTT connections (no username/password). Default: true.
    /// </summary>
    public bool AllowAnonymous { get; set; } = true;

    /// <summary>
    /// Keep-alive interval in seconds. Clients that do not send PINGREQ within
    /// 1.5x this interval are considered disconnected. Default: 60.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int KeepAliveSeconds { get; set; } = 60;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
