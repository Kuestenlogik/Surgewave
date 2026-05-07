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

    public SearchPluginsOperation(ConnectorRepositoryManager repositoryManager)
        => _repositoryManager = repositoryManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.SearchPlugins;

    public SearchPluginsRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => SearchPluginsRequestPayload.Read(ref reader);

    public void ValidateRequest(in SearchPluginsRequestPayload request) { }

    public async Task<SearchPluginsResult> ExecuteAsync(SearchPluginsRequestPayload request, CancellationToken cancellationToken)
    {
        var packages = await _repositoryManager.SearchAsync(
            request.Query,
            request.Skip,
            request.Take > 0 ? request.Take : 20,
            cancellationToken);

        var plugins = packages.Select(ToPluginInfoPayload).ToList();

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
