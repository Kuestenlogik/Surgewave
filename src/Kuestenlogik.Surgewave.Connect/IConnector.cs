namespace Kuestenlogik.Surgewave.Connect;

using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Base interface for all connectors (source and sink).
/// A connector is responsible for defining the configuration and creating tasks.
/// </summary>
public interface IConnector : IDisposable
{
    /// <summary>
    /// Returns the connector version.
    /// </summary>
    string Version { get; }

    /// <summary>
    /// Initialize the connector with the provided configuration.
    /// </summary>
    void Initialize(ConnectorContext context);

    /// <summary>
    /// Start the connector.
    /// </summary>
    void Start(IDictionary<string, string> config);

    /// <summary>
    /// Stop the connector.
    /// </summary>
    void Stop();

    /// <summary>
    /// Returns the task class that this connector uses.
    /// </summary>
    Type TaskClass { get; }

    /// <summary>
    /// Returns a set of configurations for tasks based on the current configuration.
    /// </summary>
    IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks);

    /// <summary>
    /// Define the configuration for the connector.
    /// </summary>
    ConfigDef Config { get; }
}
