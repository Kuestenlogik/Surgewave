using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Protocol.Native;
using ConnectOps = Kuestenlogik.Surgewave.Broker.Native.Operations.Connect;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol Kafka Connect operations.
/// </summary>
public sealed class NativeConnectHandler : NativeHandlerBase
{
    private readonly bool _connectEnabled;

    public NativeConnectHandler(ConnectWorker? connectWorker = null, PluginDiscovery? pluginDiscovery = null, bool connectEnabled = false)
    {
        _connectEnabled = connectEnabled && connectWorker != null;

        if (connectWorker != null)
        {
            RegisterNoRequest<ConnectOps.ListConnectorsResult>(
                SurgewaveOpCode.ListConnectors, _ => new ConnectOps.ListConnectorsOperation(connectWorker));
            Register<ConnectOps.ConnectorNameRequest, ConnectOps.GetConnectorResult>(
                SurgewaveOpCode.GetConnector, _ => new ConnectOps.GetConnectorOperation(connectWorker));
            Register<ConnectOps.CreateConnectorRequest, ConnectOps.CreateConnectorResult>(
                SurgewaveOpCode.CreateConnector, _ => new ConnectOps.CreateConnectorOperation(connectWorker));
            Register<ConnectOps.ConnectorNameRequest, ConnectOps.DeleteConnectorResult>(
                SurgewaveOpCode.DeleteConnector, _ => new ConnectOps.DeleteConnectorOperation(connectWorker));
            Register<ConnectOps.ConnectorNameRequest, ConnectOps.GetConnectorConfigResult>(
                SurgewaveOpCode.GetConnectorConfig, _ => new ConnectOps.GetConnectorConfigOperation(connectWorker));
            Register<ConnectOps.UpdateConnectorConfigRequest, ConnectOps.UpdateConnectorConfigResult>(
                SurgewaveOpCode.UpdateConnectorConfig, _ => new ConnectOps.UpdateConnectorConfigOperation(connectWorker));
            Register<ConnectOps.ConnectorNameRequest, ConnectOps.GetConnectorStatusResult>(
                SurgewaveOpCode.GetConnectorStatus, _ => new ConnectOps.GetConnectorStatusOperation(connectWorker));
            Register<ConnectOps.ConnectorNameRequest, ConnectOps.RestartConnectorResult>(
                SurgewaveOpCode.RestartConnector, _ => new ConnectOps.RestartConnectorOperation(connectWorker));
            Register<ConnectOps.ConnectorNameRequest, ConnectOps.PauseConnectorResult>(
                SurgewaveOpCode.PauseConnector, _ => new ConnectOps.PauseConnectorOperation(connectWorker));
            Register<ConnectOps.ConnectorNameRequest, ConnectOps.ResumeConnectorResult>(
                SurgewaveOpCode.ResumeConnector, _ => new ConnectOps.ResumeConnectorOperation(connectWorker));
            Register<ConnectOps.ConnectorNameRequest, ConnectOps.GetConnectorTasksResult>(
                SurgewaveOpCode.GetConnectorTasks, _ => new ConnectOps.GetConnectorTasksOperation(connectWorker));
            Register<ConnectOps.RestartConnectorTaskRequest, ConnectOps.RestartConnectorTaskResult>(
                SurgewaveOpCode.RestartConnectorTask, _ => new ConnectOps.RestartConnectorTaskOperation(connectWorker));
            RegisterNoRequest<ConnectOps.ListConnectorPluginsResult>(
                SurgewaveOpCode.ListConnectorPlugins, _ => new ConnectOps.ListConnectorPluginsOperation(pluginDiscovery));
        }
    }

    protected override Task? PreExecuteCheck(NativeRequestContext context, CancellationToken cancellationToken)
    {
        if (!_connectEnabled)
        {
            return context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.ConnectDisabled, "Kafka Connect is not enabled", cancellationToken);
        }
        return null;
    }
}
