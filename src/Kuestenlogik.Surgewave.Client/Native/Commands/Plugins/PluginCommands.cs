using System.Text;
using Kuestenlogik.Surgewave.Client.Native.Operations.Plugins;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Plugins;

/// <summary>
/// Command to search for plugins in the marketplace.
/// </summary>
public sealed class SearchPluginsCommand : ISurgewaveCommand<PluginSearchResult>
{
    private readonly string _query;
    private readonly int _skip;
    private readonly int _take;

    public SearchPluginsCommand(string? query = null, int skip = 0, int take = 20)
    {
        _query = query ?? string.Empty;
        _skip = skip;
        _take = take;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.SearchPlugins;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_query);
        writer.WriteInt32(_skip);
        writer.WriteInt32(_take);
    }

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_query) + 4 + 4;

    public PluginSearchResult ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"SearchPlugins failed: {header.ErrorCode}");

        var response = SearchPluginsResponsePayload.Read(ref reader);
        return new PluginSearchResult(
            response.Plugins.Select(ToPluginInfo).ToList(),
            response.TotalCount);
    }

    private static PluginInfo ToPluginInfo(PluginInfoPayload p) => new(
        p.PackageId,
        p.Name,
        p.Version,
        p.Description,
        p.Author,
        p.License,
        p.ProjectUrl,
        p.IconUrl,
        p.IsInstalled,
        p.InstalledVersion,
        p.DownloadCount,
        p.ConnectorTypes?.ToList() ?? [],
        p.Tags?.ToList() ?? [],
        p.AvailableVersions?.ToList() ?? [],
        p.Dependencies?.Select(d => new PluginDependency(d.Id, d.Version, d.Optional)).ToList() ?? [],
        p.IsSigned,
        p.SignerIdentity,
        p.SignerProvider);
}

/// <summary>
/// Command to get plugin details.
/// </summary>
public sealed class GetPluginCommand : ISurgewaveCommand<PluginInfo?>
{
    private readonly string _packageId;
    private readonly string? _version;

    public GetPluginCommand(string packageId, string? version = null)
    {
        _packageId = packageId;
        _version = version;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetPlugin;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_packageId);
        writer.WriteNullableString(_version);
    }

    public int EstimateRequestSize() =>
        2 + Encoding.UTF8.GetByteCount(_packageId) + 1 + (_version != null ? 2 + Encoding.UTF8.GetByteCount(_version) : 0);

    public PluginInfo? ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode == SurgewaveErrorCode.PluginNotFound)
            return null;

        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"GetPlugin failed: {header.ErrorCode}");

        var response = GetPluginResponsePayload.Read(ref reader);
        if (!response.Found || response.Plugin == null)
            return null;

        return ToPluginInfo(response.Plugin.Value);
    }

    private static PluginInfo ToPluginInfo(PluginInfoPayload p) => new(
        p.PackageId,
        p.Name,
        p.Version,
        p.Description,
        p.Author,
        p.License,
        p.ProjectUrl,
        p.IconUrl,
        p.IsInstalled,
        p.InstalledVersion,
        p.DownloadCount,
        p.ConnectorTypes?.ToList() ?? [],
        p.Tags?.ToList() ?? [],
        p.AvailableVersions?.ToList() ?? [],
        p.Dependencies?.Select(d => new PluginDependency(d.Id, d.Version, d.Optional)).ToList() ?? [],
        p.IsSigned,
        p.SignerIdentity,
        p.SignerProvider);
}

/// <summary>
/// Command to install a plugin.
/// </summary>
public sealed class InstallPluginCommand : ISurgewaveCommand<PluginInstallResult>
{
    private readonly string _packageId;
    private readonly string? _version;
    private readonly bool _includeDependencies;

    public InstallPluginCommand(string packageId, string? version = null, bool includeDependencies = true)
    {
        _packageId = packageId;
        _version = version;
        _includeDependencies = includeDependencies;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.InstallPlugin;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_packageId);
        writer.WriteNullableString(_version);
        writer.WriteBoolean(_includeDependencies);
    }

    public int EstimateRequestSize() =>
        2 + Encoding.UTF8.GetByteCount(_packageId) + 1 + (_version != null ? 2 + Encoding.UTF8.GetByteCount(_version) : 0) + 1;

    public PluginInstallResult ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None && header.ErrorCode != SurgewaveErrorCode.PluginInstallFailed)
            throw new InvalidOperationException($"InstallPlugin failed: {header.ErrorCode}");

        var response = InstallPluginResponsePayload.Read(ref reader);
        return new PluginInstallResult(
            response.IsSuccess,
            response.IsPartialSuccess,
            response.InstalledPackages.Select(p => new InstalledPackageInfo(p.PackageId, p.Version, p.WasDependency)).ToList(),
            response.Errors?.ToList() ?? []);
    }
}

/// <summary>
/// Command to uninstall a plugin.
/// </summary>
public sealed class UninstallPluginCommand : ISurgewaveCommand<PluginUninstallResult>
{
    private readonly string _packageId;
    private readonly bool _removeDependencies;

    public UninstallPluginCommand(string packageId, bool removeDependencies = false)
    {
        _packageId = packageId;
        _removeDependencies = removeDependencies;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.UninstallPlugin;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_packageId);
        writer.WriteBoolean(_removeDependencies);
    }

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_packageId) + 1;

    public PluginUninstallResult ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None && header.ErrorCode != SurgewaveErrorCode.PluginUninstallFailed)
            throw new InvalidOperationException($"UninstallPlugin failed: {header.ErrorCode}");

        var response = UninstallPluginResponsePayload.Read(ref reader);
        return new PluginUninstallResult(
            response.IsSuccess,
            response.RemovedPackages?.ToList() ?? [],
            response.Error);
    }
}

/// <summary>
/// Command to list installed plugins.
/// </summary>
public sealed class ListInstalledPluginsCommand : ISurgewaveCommand<IReadOnlyList<PluginInfo>>
{
    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListInstalledPlugins;

    public void WriteRequest(ref SurgewavePayloadWriter writer) { }

    public int EstimateRequestSize() => 0;

    public IReadOnlyList<PluginInfo> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"ListInstalledPlugins failed: {header.ErrorCode}");

        var response = ListInstalledPluginsResponsePayload.Read(ref reader);
        return response.Plugins.Select(ToPluginInfo).ToList();
    }

    private static PluginInfo ToPluginInfo(PluginInfoPayload p) => new(
        p.PackageId,
        p.Name,
        p.Version,
        p.Description,
        p.Author,
        p.License,
        p.ProjectUrl,
        p.IconUrl,
        p.IsInstalled,
        p.InstalledVersion,
        p.DownloadCount,
        p.ConnectorTypes?.ToList() ?? [],
        p.Tags?.ToList() ?? [],
        p.AvailableVersions?.ToList() ?? [],
        p.Dependencies?.Select(d => new PluginDependency(d.Id, d.Version, d.Optional)).ToList() ?? [],
        p.IsSigned,
        p.SignerIdentity,
        p.SignerProvider);
}

/// <summary>
/// Command to get plugin dependency tree.
/// </summary>
public sealed class GetPluginDependenciesCommand : ISurgewaveCommand<DependencyTreeNode?>
{
    private readonly string _packageId;
    private readonly string? _version;

    public GetPluginDependenciesCommand(string packageId, string? version = null)
    {
        _packageId = packageId;
        _version = version;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetPluginDependencies;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_packageId);
        writer.WriteNullableString(_version);
    }

    public int EstimateRequestSize() =>
        2 + Encoding.UTF8.GetByteCount(_packageId) + 1 + (_version != null ? 2 + Encoding.UTF8.GetByteCount(_version) : 0);

    public DependencyTreeNode? ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode == SurgewaveErrorCode.PluginNotFound)
            return null;

        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"GetPluginDependencies failed: {header.ErrorCode}");

        var response = GetPluginDependenciesResponsePayload.Read(ref reader);
        if (!response.Found || response.Root == null)
            return null;

        return ToTreeNode(response.Root.Value);
    }

    private static DependencyTreeNode ToTreeNode(DependencyTreeNodePayload node) => new(
        node.PackageId,
        node.Version,
        node.IsInstalled,
        node.IsMissing,
        node.IsCircular,
        node.Children?.Select(ToTreeNode).ToList() ?? []);
}
