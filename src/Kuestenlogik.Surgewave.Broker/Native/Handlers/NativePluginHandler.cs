using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;
using PluginOps = Kuestenlogik.Surgewave.Broker.Native.Operations.Plugins;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol plugin/marketplace operations.
/// </summary>
public sealed class NativePluginHandler : NativeHandlerBase
{
    private readonly bool _pluginsEnabled;

    public NativePluginHandler(ConnectorRepositoryManager? repositoryManager = null, bool pluginsEnabled = false, PluginDiscovery? pluginDiscovery = null)
    {
        _pluginsEnabled = pluginsEnabled && repositoryManager != null;

        if (repositoryManager != null)
        {
            Register<SearchPluginsRequestPayload, PluginOps.SearchPluginsResult>(
                SurgewaveOpCode.SearchPlugins, _ => new PluginOps.SearchPluginsOperation(repositoryManager, pluginDiscovery));
            Register<GetPluginRequestPayload, PluginOps.GetPluginResult>(
                SurgewaveOpCode.GetPlugin, _ => new PluginOps.GetPluginOperation(repositoryManager));
            Register<InstallPluginRequestPayload, PluginOps.InstallPluginResult>(
                SurgewaveOpCode.InstallPlugin, _ => new PluginOps.InstallPluginOperation(repositoryManager));
            Register<UninstallPluginRequestPayload, PluginOps.UninstallPluginResult>(
                SurgewaveOpCode.UninstallPlugin, _ => new PluginOps.UninstallPluginOperation(repositoryManager));
            RegisterNoRequest<PluginOps.ListInstalledPluginsResult>(
                SurgewaveOpCode.ListInstalledPlugins, _ => new PluginOps.ListInstalledPluginsOperation(repositoryManager));
            Register<GetPluginDependenciesRequestPayload, PluginOps.GetPluginDependenciesResult>(
                SurgewaveOpCode.GetPluginDependencies, _ => new PluginOps.GetPluginDependenciesOperation(repositoryManager));
        }
    }

    protected override Task? PreExecuteCheck(NativeRequestContext context, CancellationToken cancellationToken)
    {
        if (!_pluginsEnabled)
        {
            return context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.PluginManagerDisabled, "Plugin management is not enabled", cancellationToken);
        }
        return null;
    }
}
