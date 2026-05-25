using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Plugins;

namespace Kuestenlogik.Surgewave.Broker.Native.Operations.Plugins;

/// <summary>
/// Result for search plugins operation.
/// </summary>
public readonly record struct SearchPluginsResult
{
    public required SearchPluginsResponsePayload Response { get; init; }
}

/// <summary>
/// Operation to search for plugins in the marketplace.
/// </summary>
public sealed class SearchPluginsOperation : IOperationHandler<SearchPluginsRequestPayload, SearchPluginsResult>
{
    private readonly ConnectorRepositoryManager _repositoryManager;
    private readonly PluginDiscovery? _pluginDiscovery;

    public SearchPluginsOperation(ConnectorRepositoryManager repositoryManager, PluginDiscovery? pluginDiscovery = null)
    {
        _repositoryManager = repositoryManager;
        _pluginDiscovery = pluginDiscovery;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.SearchPlugins;

    public SearchPluginsRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => SearchPluginsRequestPayload.Read(ref reader);

    public void ValidateRequest(in SearchPluginsRequestPayload request) { }

    public async Task<SearchPluginsResult> ExecuteAsync(SearchPluginsRequestPayload request, CancellationToken cancellationToken)
    {
        // Repository-Search liefert primaer NuGet-Treffer. Fuer die Plugin-Marketplace-UX
        // muss auch die lokale Installed-Liste in der "default"-Sicht (leere Query) erscheinen,
        // sonst zeigt der Marketplace "Installed: 0" obwohl Connector-Plugins im pluginsDirectory
        // liegen. Bei expliziter Query keine Installed-Spurious-Hits — Filter via Query-Match.
        var packages = await _repositoryManager.SearchAsync(
            request.Query,
            request.Skip,
            request.Take > 0 ? request.Take : 20,
            cancellationToken);

        var combined = new List<ConnectorPackageInfo>(packages);
        var packageIds = new HashSet<string>(combined.Select(p => p.PackageId), StringComparer.OrdinalIgnoreCase);

        // ConnectorTypes-Index: aus PluginDiscovery alle entdeckten Klassen pro Package-Prefix
        // sammeln. Klassen-Konvention: "<packageId>.<TypeName>" -> Package via StartsWith-Match.
        // Wert: Set von Types ("sink"/"source"/"processor"/...) — pro Card als Badge gerendert.
        var typesByPackage = BuildTypesByPackageIndex();

        foreach (var installed in _repositoryManager.Installer.InstalledConnectors.Values)
        {
            if (packageIds.Contains(installed.PackageId)) continue;

            // Bei leerer Query: alle Installed mitliefern. Bei expliziter Query: nur Treffer.
            if (!string.IsNullOrEmpty(request.Query)
                && !installed.PackageId.Contains(request.Query, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var connectorTypes = typesByPackage.TryGetValue(installed.PackageId, out var types)
                ? types.OrderBy(t => t, StringComparer.OrdinalIgnoreCase).ToList()
                : new List<string>();

            combined.Add(new ConnectorPackageInfo
            {
                PackageId = installed.PackageId,
                Name = string.IsNullOrEmpty(installed.Name) ? installed.PackageId : installed.Name,
                Version = installed.Version,
                Description = installed.Description,
                Author = installed.Author,
                License = installed.License,
                ProjectUrl = string.Empty,
                IconUrl = string.Empty,
                IsInstalled = true,
                InstalledVersion = installed.Version,
                DownloadCount = 0,
                ConnectorTypes = connectorTypes,
                Tags = installed.Tags.ToList(),
                AvailableVersions = [installed.Version],
                Dependencies = [],
                IsSigned = false,
                SignerIdentity = string.Empty,
                SignerProvider = string.Empty
            });
        }

        var plugins = combined.Select(ToPluginInfoPayload).ToList();

        return new SearchPluginsResult
        {
            Response = new SearchPluginsResponsePayload
            {
                Plugins = plugins,
                TotalCount = plugins.Count
            }
        };
    }

    public void WriteResponse(IPayloadWriter writer, in SearchPluginsResult response)
        => response.Response.WriteTo(writer);

    private Dictionary<string, HashSet<string>> BuildTypesByPackageIndex()
    {
        var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        if (_pluginDiscovery is null) return result;

        // InstalledConnectors-PackageId ist der Namespace-Prefix der Plugin-Klassen.
        // Match: plugin.Class.StartsWith(installed.PackageId + "."). Sammle alle Types.
        var installedIds = _repositoryManager.Installer.InstalledConnectors.Keys.ToList();
        foreach (var plugin in _pluginDiscovery.GetAllPlugins())
        {
            foreach (var packageId in installedIds)
            {
                if (!plugin.Class.StartsWith(packageId + ".", StringComparison.OrdinalIgnoreCase))
                    continue;
                if (!result.TryGetValue(packageId, out var set))
                {
                    set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    result[packageId] = set;
                }
                set.Add(plugin.Type);
                break;
            }
        }
        return result;
    }

    private static PluginInfoPayload ToPluginInfoPayload(ConnectorPackageInfo package) => new()
    {
        PackageId = package.PackageId,
        Name = package.Name,
        Version = package.Version,
        Description = package.Description,
        Author = package.Author,
        License = package.License,
        ProjectUrl = package.ProjectUrl,
        IconUrl = package.IconUrl,
        IsInstalled = package.IsInstalled,
        InstalledVersion = package.InstalledVersion,
        DownloadCount = package.DownloadCount,
        ConnectorTypes = package.ConnectorTypes,
        Tags = package.Tags,
        AvailableVersions = package.AvailableVersions,
        Dependencies = package.Dependencies
            .Select(d => new PluginDependencyPayload
            {
                Id = d.PackageId,
                Version = d.VersionConstraint,
                Optional = d.Optional
            }).ToList(),
        IsSigned = package.IsSigned,
        SignerIdentity = package.SignerIdentity,
        SignerProvider = package.SignerProvider
    };
}

/// <summary>
/// Result for get plugin operation.
/// </summary>
public readonly record struct GetPluginResult
{
    public required GetPluginResponsePayload Response { get; init; }
}

/// <summary>
/// Operation to get plugin details.
/// </summary>
public sealed class GetPluginOperation : IOperationHandler<GetPluginRequestPayload, GetPluginResult>
{
    private readonly ConnectorRepositoryManager _repositoryManager;

    public GetPluginOperation(ConnectorRepositoryManager repositoryManager)
        => _repositoryManager = repositoryManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetPlugin;

    public GetPluginRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => GetPluginRequestPayload.Read(ref reader);

    public void ValidateRequest(in GetPluginRequestPayload request)
    {
        if (string.IsNullOrEmpty(request.PackageId))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "PackageId is required");
    }

    public async Task<GetPluginResult> ExecuteAsync(GetPluginRequestPayload request, CancellationToken cancellationToken)
    {
        var package = await _repositoryManager.GetPackageAsync(request.PackageId, request.Version, cancellationToken);

        if (package == null)
        {
            return new GetPluginResult
            {
                Response = new GetPluginResponsePayload { Found = false, Plugin = null }
            };
        }

        return new GetPluginResult
        {
            Response = new GetPluginResponsePayload
            {
                Found = true,
                Plugin = ToPluginInfoPayload(package)
            }
        };
    }

    public void WriteResponse(IPayloadWriter writer, in GetPluginResult response)
        => response.Response.WriteTo(writer);

    private static PluginInfoPayload ToPluginInfoPayload(ConnectorPackageInfo package) => new()
    {
        PackageId = package.PackageId,
        Name = package.Name,
        Version = package.Version,
        Description = package.Description,
        Author = package.Author,
        License = package.License,
        ProjectUrl = package.ProjectUrl,
        IconUrl = package.IconUrl,
        IsInstalled = package.IsInstalled,
        InstalledVersion = package.InstalledVersion,
        DownloadCount = package.DownloadCount,
        ConnectorTypes = package.ConnectorTypes,
        Tags = package.Tags,
        AvailableVersions = package.AvailableVersions,
        Dependencies = package.Dependencies
            .Select(d => new PluginDependencyPayload
            {
                Id = d.PackageId,
                Version = d.VersionConstraint,
                Optional = d.Optional
            }).ToList(),
        IsSigned = package.IsSigned,
        SignerIdentity = package.SignerIdentity,
        SignerProvider = package.SignerProvider
    };
}

/// <summary>
/// Result for install plugin operation.
/// </summary>
public readonly record struct InstallPluginResult
{
    public required InstallPluginResponsePayload Response { get; init; }
}

/// <summary>
/// Operation to install a plugin.
/// </summary>
public sealed class InstallPluginOperation : IOperationHandler<InstallPluginRequestPayload, InstallPluginResult>
{
    private readonly ConnectorRepositoryManager _repositoryManager;

    public InstallPluginOperation(ConnectorRepositoryManager repositoryManager)
        => _repositoryManager = repositoryManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.InstallPlugin;

    public InstallPluginRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => InstallPluginRequestPayload.Read(ref reader);

    public void ValidateRequest(in InstallPluginRequestPayload request)
    {
        if (string.IsNullOrEmpty(request.PackageId))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "PackageId is required");
    }

    public async Task<InstallPluginResult> ExecuteAsync(InstallPluginRequestPayload request, CancellationToken cancellationToken)
    {
        try
        {
            if (request.IncludeDependencies)
            {
                var result = await _repositoryManager.InstallWithDependenciesAsync(
                    request.PackageId,
                    request.Version,
                    cancellationToken);

                return new InstallPluginResult
                {
                    Response = new InstallPluginResponsePayload
                    {
                        IsSuccess = result.IsSuccess,
                        IsPartialSuccess = result.IsPartialSuccess,
                        InstalledPackages = result.InstalledPackages
                            .Select(p => new InstalledPackageInfoPayload
                            {
                                PackageId = p.PackageId,
                                Version = p.Version,
                                WasDependency = p.IsDependency
                            }).ToList(),
                        Errors = result.Errors
                    }
                };
            }
            else
            {
                await _repositoryManager.InstallAsync(request.PackageId, request.Version, cancellationToken);

                return new InstallPluginResult
                {
                    Response = new InstallPluginResponsePayload
                    {
                        IsSuccess = true,
                        IsPartialSuccess = false,
                        InstalledPackages =
                        [
                            new InstalledPackageInfoPayload
                            {
                                PackageId = request.PackageId,
                                Version = request.Version ?? "latest",
                                WasDependency = false
                            }
                        ],
                        Errors = []
                    }
                };
            }
        }
        catch (Exception ex)
        {
            return new InstallPluginResult
            {
                Response = new InstallPluginResponsePayload
                {
                    IsSuccess = false,
                    IsPartialSuccess = false,
                    InstalledPackages = [],
                    Errors = [ex.Message]
                }
            };
        }
    }

    public void WriteResponse(IPayloadWriter writer, in InstallPluginResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for uninstall plugin operation.
/// </summary>
public readonly record struct UninstallPluginResult
{
    public required UninstallPluginResponsePayload Response { get; init; }
}

/// <summary>
/// Operation to uninstall a plugin.
/// </summary>
public sealed class UninstallPluginOperation : IOperationHandler<UninstallPluginRequestPayload, UninstallPluginResult>
{
    private readonly ConnectorRepositoryManager _repositoryManager;

    public UninstallPluginOperation(ConnectorRepositoryManager repositoryManager)
        => _repositoryManager = repositoryManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.UninstallPlugin;

    public UninstallPluginRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => UninstallPluginRequestPayload.Read(ref reader);

    public void ValidateRequest(in UninstallPluginRequestPayload request)
    {
        if (string.IsNullOrEmpty(request.PackageId))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "PackageId is required");
    }

    public Task<UninstallPluginResult> ExecuteAsync(UninstallPluginRequestPayload request, CancellationToken cancellationToken)
    {
        try
        {
            _repositoryManager.Uninstall(request.PackageId);

            return Task.FromResult(new UninstallPluginResult
            {
                Response = new UninstallPluginResponsePayload
                {
                    IsSuccess = true,
                    RemovedPackages = [request.PackageId],
                    Error = null
                }
            });
        }
        catch (Exception ex)
        {
            return Task.FromResult(new UninstallPluginResult
            {
                Response = new UninstallPluginResponsePayload
                {
                    IsSuccess = false,
                    RemovedPackages = [],
                    Error = ex.Message
                }
            });
        }
    }

    public void WriteResponse(IPayloadWriter writer, in UninstallPluginResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for list installed plugins operation.
/// </summary>
public readonly record struct ListInstalledPluginsResult
{
    public required ListInstalledPluginsResponsePayload Response { get; init; }
}

/// <summary>
/// Operation to list installed plugins.
/// </summary>
public sealed class ListInstalledPluginsOperation : INoRequestOperationHandler<ListInstalledPluginsResult>
{
    private readonly ConnectorRepositoryManager _repositoryManager;

    public ListInstalledPluginsOperation(ConnectorRepositoryManager repositoryManager)
        => _repositoryManager = repositoryManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListInstalledPlugins;

    public async Task<ListInstalledPluginsResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        var installed = _repositoryManager.Installer.InstalledConnectors.Values;

        var plugins = new List<PluginInfoPayload>();
        foreach (var connector in installed)
        {
            var package = await _repositoryManager.GetPackageAsync(connector.PackageId, connector.Version, cancellationToken);
            if (package != null)
            {
                plugins.Add(ToPluginInfoPayload(package));
            }
            else
            {
                // Package not found in repository, create minimal info
                plugins.Add(new PluginInfoPayload
                {
                    PackageId = connector.PackageId,
                    Name = connector.PackageId,
                    Version = connector.Version,
                    IsInstalled = true,
                    InstalledVersion = connector.Version,
                    ConnectorTypes = [],
                    Tags = [],
                    AvailableVersions = [connector.Version],
                    Dependencies = []
                });
            }
        }

        return new ListInstalledPluginsResult
        {
            Response = new ListInstalledPluginsResponsePayload { Plugins = plugins }
        };
    }

    public void WriteResponse(IPayloadWriter writer, in ListInstalledPluginsResult response)
        => response.Response.WriteTo(writer);

    private static PluginInfoPayload ToPluginInfoPayload(ConnectorPackageInfo package) => new()
    {
        PackageId = package.PackageId,
        Name = package.Name,
        Version = package.Version,
        Description = package.Description,
        Author = package.Author,
        License = package.License,
        ProjectUrl = package.ProjectUrl,
        IconUrl = package.IconUrl,
        IsInstalled = package.IsInstalled,
        InstalledVersion = package.InstalledVersion,
        DownloadCount = package.DownloadCount,
        ConnectorTypes = package.ConnectorTypes,
        Tags = package.Tags,
        AvailableVersions = package.AvailableVersions,
        Dependencies = package.Dependencies
            .Select(d => new PluginDependencyPayload
            {
                Id = d.PackageId,
                Version = d.VersionConstraint,
                Optional = d.Optional
            }).ToList(),
        IsSigned = package.IsSigned,
        SignerIdentity = package.SignerIdentity,
        SignerProvider = package.SignerProvider
    };
}

/// <summary>
/// Result for get plugin dependencies operation.
/// </summary>
public readonly record struct GetPluginDependenciesResult
{
    public required GetPluginDependenciesResponsePayload Response { get; init; }
}

/// <summary>
/// Operation to get plugin dependency tree.
/// </summary>
public sealed class GetPluginDependenciesOperation : IOperationHandler<GetPluginDependenciesRequestPayload, GetPluginDependenciesResult>
{
    private readonly ConnectorRepositoryManager _repositoryManager;

    public GetPluginDependenciesOperation(ConnectorRepositoryManager repositoryManager)
        => _repositoryManager = repositoryManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetPluginDependencies;

    public GetPluginDependenciesRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => GetPluginDependenciesRequestPayload.Read(ref reader);

    public void ValidateRequest(in GetPluginDependenciesRequestPayload request)
    {
        if (string.IsNullOrEmpty(request.PackageId))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "PackageId is required");
    }

    public async Task<GetPluginDependenciesResult> ExecuteAsync(GetPluginDependenciesRequestPayload request, CancellationToken cancellationToken)
    {
        var tree = await _repositoryManager.GetDependencyTreeAsync(request.PackageId, cancellationToken);

        if (tree == null)
        {
            return new GetPluginDependenciesResult
            {
                Response = new GetPluginDependenciesResponsePayload { Found = false, Root = null }
            };
        }

        return new GetPluginDependenciesResult
        {
            Response = new GetPluginDependenciesResponsePayload
            {
                Found = true,
                Root = ToDependencyTreeNodePayload(tree)
            }
        };
    }

    public void WriteResponse(IPayloadWriter writer, in GetPluginDependenciesResult response)
        => response.Response.WriteTo(writer);

    private static DependencyTreeNodePayload ToDependencyTreeNodePayload(DependencyTreeNode node) => new()
    {
        PackageId = node.PackageId,
        Version = node.Version,
        IsInstalled = node.IsInstalled,
        IsMissing = node.IsMissing,
        IsCircular = node.IsCircular,
        Children = node.Children.Select(ToDependencyTreeNodePayload).ToList()
    };
}
