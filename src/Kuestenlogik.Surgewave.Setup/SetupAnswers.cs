namespace Kuestenlogik.Surgewave.Setup;

/// <summary>
/// Everything the operator picked during the wizard. The
/// <see cref="SetupScriptGenerator"/> + <see cref="AppSettingsGenerator"/>
/// turn this into the three output files; keeping it a plain record
/// makes the file-generation side independently testable without
/// involving the prompt UI (Spectre.Console in the CLI or
/// MudBlazor in the Control browser wizard).
/// </summary>
public sealed record SetupAnswers
{
    /// <summary>Selected storage-engine plugin (null = stay on the built-in default).</summary>
    public SetupPluginRef? StorageEngine { get; init; }

    /// <summary>Selected connector plugins.</summary>
    public IReadOnlyList<SetupPluginRef> Connectors { get; init; } = [];

    /// <summary>Selected protocol-adapter plugins (MQTT, AMQP, …).</summary>
    public IReadOnlyList<SetupPluginRef> Protocols { get; init; } = [];

    /// <summary>Selected schema-handler plugins (Avro, Protobuf, …).</summary>
    public IReadOnlyList<SetupPluginRef> SchemaHandlers { get; init; } = [];

    /// <summary>One of None / SaslPlain / SaslScram / Tls / MutualTls.</summary>
    public SetupAuthMethod Auth { get; init; } = SetupAuthMethod.None;

    /// <summary>When true, the broker emits OTLP traces + metrics; <see cref="OtlpEndpoint"/> required.</summary>
    public bool TelemetryEnabled { get; init; }

    /// <summary>OTLP receiver URL (HTTPS preferred). Ignored when <see cref="TelemetryEnabled"/> is false.</summary>
    public string? OtlpEndpoint { get; init; }
}
