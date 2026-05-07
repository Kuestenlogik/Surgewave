using Kuestenlogik.Surgewave.Connect.Configuration;

namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// Base class for source connectors with exactly-once delivery guarantees.
/// Subclasses must return an <see cref="ExactlyOnceSourceTask"/> from <see cref="TaskClass"/>.
///
/// When the Connect worker detects this connector type, it wraps each task in an
/// <see cref="ExactlyOnceSourcePipeline"/> that uses cross-topic transactions to
/// produce messages and commit offsets atomically.
/// </summary>
/// <example>
/// <code>
/// [ConnectorMetadata(Name = "my-eos-source", Description = "Reads with exactly-once guarantees")]
/// public class MyEosSourceConnector : ExactlyOnceSourceConnector
/// {
///     public override string Version => "1.0.0";
///     public override Type TaskClass => typeof(MyEosSourceTask);
///     public override ConfigDef Config => new ConfigDef()
///         .Define("connection.url", ConfigDef.Type.String, ConfigDef.Importance.High, "Connection URL");
///     public override void Start(IDictionary&lt;string, string&gt; config) { }
///     public override void Stop() { }
///     public override IReadOnlyList&lt;IDictionary&lt;string, string&gt;&gt; TaskConfigs(int maxTasks) => [config];
/// }
/// </code>
/// </example>
public abstract class ExactlyOnceSourceConnector : SourceConnector
{
    /// <summary>
    /// Gets or sets whether exactly-once source offset tracking is enabled.
    /// When false, falls back to at-least-once delivery using the standard pipeline.
    /// </summary>
    public bool ExactlyOnceEnabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the configuration for exactly-once semantics.
    /// </summary>
    public ExactlyOnceConfig ExactlyOnceConfig { get; set; } = new();
}
