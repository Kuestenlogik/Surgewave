using Kuestenlogik.Surgewave.Client.Native.Commands;
using Kuestenlogik.Surgewave.Client.Native.Commands.Connect;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Connect;

/// <summary>
/// Kafka Connect operations for Surgewave native client.
/// </summary>
public sealed class SurgewaveConnectOperations
{
    private readonly SurgewaveNativeClient _client;
    private readonly CommandExecutor _executor;

    internal SurgewaveConnectOperations(SurgewaveNativeClient client)
    {
        _client = client;
        _executor = new CommandExecutor(client);
    }

    /// <summary>
    /// List all connectors.
    /// </summary>
    public Task<IReadOnlyList<string>> ListAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new ListConnectorsCommand(), cancellationToken);

    /// <summary>
    /// Get connector information.
    /// </summary>
    public Task<ConnectorInfo?> GetConnectorAsync(string name, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetConnectorCommand(name), cancellationToken);

    /// <summary>
    /// Create a connector.
    /// </summary>
    public Task<ConnectorCreateResult> CreateConnectorAsync(string name, Dictionary<string, string> config, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new CreateConnectorCommand(name, config), cancellationToken);

    /// <summary>
    /// Start building a connector with fluent API.
    /// </summary>
    public ConnectorBuilder CreateConnector(string name) => new(_client, name);

    /// <summary>
    /// Delete a connector.
    /// </summary>
    public Task DeleteConnectorAsync(string name, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new DeleteConnectorCommand(name), cancellationToken);

    /// <summary>
    /// Get connector configuration.
    /// </summary>
    public Task<Dictionary<string, string>> GetConnectorConfigAsync(string name, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetConnectorConfigCommand(name), cancellationToken);

    /// <summary>
    /// Update connector configuration.
    /// </summary>
    public Task UpdateConnectorConfigAsync(string name, Dictionary<string, string> config, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new UpdateConnectorConfigCommand(name, config), cancellationToken);

    /// <summary>
    /// Get connector status.
    /// </summary>
    public Task<ConnectorStatus?> GetConnectorStatusAsync(string name, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetConnectorStatusCommand(name), cancellationToken);

    /// <summary>
    /// Restart a connector.
    /// </summary>
    public Task RestartConnectorAsync(string name, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new RestartConnectorCommand(name), cancellationToken);

    /// <summary>
    /// Pause a connector.
    /// </summary>
    public Task PauseConnectorAsync(string name, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new PauseConnectorCommand(name), cancellationToken);

    /// <summary>
    /// Resume a paused connector.
    /// </summary>
    public Task ResumeConnectorAsync(string name, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new ResumeConnectorCommand(name), cancellationToken);

    /// <summary>
    /// Get connector tasks.
    /// </summary>
    public Task<IReadOnlyList<ConnectorTaskInfo>> GetConnectorTasksAsync(string name, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetConnectorTasksCommand(name), cancellationToken);

    /// <summary>
    /// Restart a specific connector task.
    /// </summary>
    public Task RestartConnectorTaskAsync(string name, int taskId, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new RestartConnectorTaskCommand(name, taskId), cancellationToken);

    /// <summary>
    /// List available connector plugins.
    /// </summary>
    public Task<IReadOnlyList<PluginInfo>> ListConnectorPluginsAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new ListConnectorPluginsCommand(), cancellationToken);
}
