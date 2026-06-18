using Kuestenlogik.Surgewave.Plugins.Marketplace;

namespace Kuestenlogik.Surgewave.Cli.Commands.Setup;

/// <summary>
/// Everything the operator picked during the wizard. The
/// <see cref="SetupScriptGenerator"/> + <see cref="AppSettingsGenerator"/>
/// turn this into the three output files; keeping it a plain record
/// makes the file-generation side independently testable without
/// involving Spectre / stdin prompts.
/// </summary>
public sealed record SetupAnswers
{
    /// <summary>Selected storage-engine plugin entry (null = stay on the built-in default).</summary>
    public PluginMarketplaceEntry? StorageEngine { get; init; }

    /// <summary>Selected connector plugins.</summary>
    public IReadOnlyList<PluginMarketplaceEntry> Connectors { get; init; } = [];

    /// <summary>Selected protocol-adapter plugins (MQTT, AMQP, …).</summary>
    public IReadOnlyList<PluginMarketplaceEntry> Protocols { get; init; } = [];

    /// <summary>Selected schema-handler plugins (Avro, Protobuf, …).</summary>
    public IReadOnlyList<PluginMarketplaceEntry> SchemaHandlers { get; init; } = [];

    /// <summary>One of None / SaslPlain / SaslScram / Tls / MutualTls.</summary>
    public SetupAuthMethod Auth { get; init; } = SetupAuthMethod.None;

    /// <summary>When true, the broker emits OTLP traces + metrics; <see cref="OtlpEndpoint"/> required.</summary>
    public bool TelemetryEnabled { get; init; }

    /// <summary>OTLP receiver URL (HTTPS preferred). Ignored when <see cref="TelemetryEnabled"/> is false.</summary>
    public string? OtlpEndpoint { get; init; }
}

public enum SetupAuthMethod
{
    None,
    SaslPlain,
    SaslScram,
    Tls,
    MutualTls,
}
