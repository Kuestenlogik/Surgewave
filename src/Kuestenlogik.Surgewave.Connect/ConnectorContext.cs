using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Context provided to connectors by the Connect runtime.
/// </summary>
public sealed class ConnectorContext
{
    /// <summary>
    /// Request a task reconfiguration. This is useful when the connector detects
    /// changes that require a different task configuration.
    /// </summary>
    public Action? RequestTaskReconfiguration { get; init; }

    /// <summary>
    /// Request to raise an error for this connector.
    /// </summary>
    public Action<Exception>? RaiseError { get; init; }

    /// <summary>
    /// Logger instance for this connector.
    /// </summary>
    public ILogger? Logger { get; init; }
}
