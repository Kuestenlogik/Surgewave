namespace Kuestenlogik.Surgewave.Connect;

using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// Abstract base class for all connectors (Source, Processor, Sink).
/// Provides common lifecycle, configuration, and disposal behavior.
/// </summary>
public abstract class Connector : IConnector
{
    /// <summary>Gets the connector context provided during initialization.</summary>
    protected ConnectorContext Context { get; private set; } = null!;
    private bool _disposed;

    // --- IPlugin ---

    /// <inheritdoc />
    public virtual string FeatureId => GetType().FullName ?? GetType().Name;

    /// <inheritdoc />
    public virtual string DisplayName => GetType().Name.Replace("Connector", "").Replace("Node", "");

    // --- IPipelineNode ---

    /// <summary>Number of input ports. Override in subclasses.</summary>
    public abstract int InputPorts { get; }

    /// <summary>Number of output ports. Override in subclasses.</summary>
    public abstract int OutputPorts { get; }

    // --- IConnector ---

    /// <inheritdoc />
    public abstract string Version { get; }

    /// <inheritdoc />
    public abstract Type TaskClass { get; }

    /// <inheritdoc />
    public abstract ConfigDef Config { get; }

    /// <inheritdoc />
    public virtual void Initialize(ConnectorContext context)
    {
        Context = context;
    }

    /// <inheritdoc />
    public abstract void Start(IDictionary<string, string> config);

    /// <inheritdoc />
    public abstract void Stop();

    /// <inheritdoc />
    public abstract IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks);

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            _disposed = true;
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}
