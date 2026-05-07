using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Manages multiple connector repositories.
/// </summary>
public sealed class ConnectorRepositoryManager : IDisposable
{
    private readonly List<IConnectorRepository> _repositories = new();
    private readonly ConnectorInstaller _installer;

    /// <summary>
    /// Creates a new repository manager.
    /// </summary>
    /// <param name="installDirectory">Directory to install connectors to.</param>
    public ConnectorRepositoryManager(string installDirectory)
    {
        _installer = new ConnectorInstaller(installDirectory);

        // Add default NuGet.org repository
        var defaultRepo = new NuGetConnectorRepository(
            "nuget.org",
            "https://api.nuget.org/v3/index.json");
        _repositories.Add(defaultRepo);
    }

    /// <summary>
    /// Registered repositories.
    /// </summary>
    public IReadOnlyList<IConnectorRepository> Repositories => _repositories;

    /// <summary>
    /// Connector installer.
    /// </summary>
    public ConnectorInstaller Installer => _installer;

    /// <summary>
    /// Add a repository.
    /// </summary>
    /// <param name="repository">Repository to add.</param>
    public void AddRepository(IConnectorRepository repository)
    {
        _repositories.Add(repository);
    }

    /// <summary>
    /// Remove a repository by name.
    /// </summary>
    /// <param name="name">Repository name.</param>
    public void RemoveRepository(string name)
    {
        var repo = _repositories.FirstOrDefault(r => r.Name == name);
        if (repo != null)
        {
            _repositories.Remove(repo);
            (repo as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// Search all repositories for connectors.
    /// </summary>
    /// <param name="query">Search query.</param>
    /// <param name="skip">Results to skip.</param>
    /// <param name="take">Results to take.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Combined search results.</returns>
    public async Task<IReadOnlyList<ConnectorPackageInfo>> SearchAsync(
        string? query,
        int skip = 0,
        int take = 20,
        CancellationToken cancellationToken = default)
    {
        var results = new List<ConnectorPackageInfo>();

        foreach (var repo in _repositories)
        {
            try
            {
                var packages = await repo.SearchAsync(query, 0, 100, cancellationToken);

                foreach (var package in packages)
                {
                    // Check if already in results (by package ID)
                    if (results.Any(r => r.PackageId == package.PackageId))
                        continue;

                    // Add installation status
                    var installedVersion = _installer.GetInstalledVersion(package.PackageId);
                    var enrichedPackage = package with
                    {
                        IsInstalled = installedVersion != null,
                        InstalledVersion = installedVersion
                    };

                    results.Add(enrichedPackage);
                }
            }
            catch (Exception)
            {
                // Continue with other repositories
            }
        }

        // Sort by download count and apply pagination
        return results
            .OrderByDescending(p => p.DownloadCount)
            .Skip(skip)
            .Take(take)
            .ToList();
    }

    /// <summary>
    /// Get package details from any repository.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="version">Optional version.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Package info or null.</returns>
    public async Task<ConnectorPackageInfo?> GetPackageAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        foreach (var repo in _repositories)
        {
            try
            {
                var package = await repo.GetPackageAsync(packageId, version, cancellationToken);
                if (package != null)
                {
                    var installedVersion = _installer.GetInstalledVersion(package.PackageId);
                    return package with
                    {
                        IsInstalled = installedVersion != null,
                        InstalledVersion = installedVersion
                    };
                }
            }
            catch (Exception)
            {
                // Try next repository
            }
        }

        return null;
    }

    /// <summary>
    /// Install a connector from any repository.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="version">Optional version (latest if not specified).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task InstallAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        // Find the package
        var package = await GetPackageAsync(packageId, version, cancellationToken);
        if (package == null)
            throw new InvalidOperationException($"Package {packageId} not found");

        // Find the repository that has this package
        foreach (var repo in _repositories)
        {
            try
            {
                var repoPackage = await repo.GetPackageAsync(packageId, package.Version, cancellationToken);
                if (repoPackage != null)
                {
                    await _installer.InstallAsync(repo, packageId, package.Version, cancellationToken);
                    return;
                }
            }
            catch (Exception)
            {
                // Try next repository
            }
        }

        throw new InvalidOperationException($"Failed to download package {packageId}");
    }

    /// <summary>
    /// Uninstall a connector.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    public void Uninstall(string packageId)
    {
        _installer.Uninstall(packageId);
    }

    /// <summary>
    /// Update a connector to the latest version.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task UpdateAsync(string packageId, CancellationToken cancellationToken = default)
    {
        var package = await GetPackageAsync(packageId, null, cancellationToken);
        if (package == null)
            throw new InvalidOperationException($"Package {packageId} not found");

        // Only update if there's a newer version
        var currentVersion = _installer.GetInstalledVersion(packageId);
        if (currentVersion != null && currentVersion == package.Version)
            return;

        await InstallAsync(packageId, package.Version, cancellationToken);
    }

    /// <summary>
    /// Resolve dependencies for a package without installing.
    /// </summary>
    /// <param name="packageId">Package ID to resolve dependencies for.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Dependency resolution result.</returns>
    public async Task<DependencyResolutionResult> ResolveDependenciesAsync(
        string packageId,
        CancellationToken cancellationToken = default)
    {
        var resolver = new DependencyResolver(
            manifestLoader: LoadManifestAsync,
            installedVersionProvider: _installer.GetInstalledVersion);

        return await resolver.ResolveAsync(packageId, cancellationToken);
    }

    /// <summary>
    /// Install a connector with all its dependencies.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="version">Optional version (latest if not specified).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Installation result with all installed packages.</returns>
    public async Task<DependencyInstallResult> InstallWithDependenciesAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        // First, resolve all dependencies
        var resolution = await ResolveDependenciesAsync(packageId, cancellationToken);

        if (!resolution.IsSuccess)
        {
            return DependencyInstallResult.Failed(
                resolution.Errors.ToList(),
                resolution.Warnings.ToList());
        }

        var installed = new List<InstalledPackageInfo>();
        var errors = new List<string>();

        // Install in order (dependencies first)
        foreach (var dep in resolution.ToInstall)
        {
            try
            {
                await InstallAsync(dep.Id, dep.Version, cancellationToken);

                installed.Add(new InstalledPackageInfo
                {
                    PackageId = dep.Id,
                    Version = dep.Version,
                    IsDependency = !dep.IsRoot,
                    WasUpgraded = dep.InstalledVersion != null
                });
            }
            catch (Exception ex)
            {
                errors.Add($"Failed to install {dep.Id}@{dep.Version}: {ex.Message}");
            }
        }

        if (errors.Count > 0)
        {
            return DependencyInstallResult.PartialSuccess(
                installed,
                errors,
                resolution.Warnings.ToList());
        }

        return DependencyInstallResult.Succeeded(
            installed,
            resolution.AlreadyInstalled.Select(d => new InstalledPackageInfo
            {
                PackageId = d.Id,
                Version = d.Version,
                IsDependency = !d.IsRoot,
                WasUpgraded = false
            }).ToList(),
            resolution.Warnings.ToList());
    }

    /// <summary>
    /// Get the dependency tree for a package.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Dependency tree.</returns>
    public async Task<DependencyTreeNode?> GetDependencyTreeAsync(
        string packageId,
        CancellationToken cancellationToken = default)
    {
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        return await BuildTreeNodeAsync(packageId, null, visited, cancellationToken);
    }

    private async Task<DependencyTreeNode?> BuildTreeNodeAsync(
        string packageId,
        string? versionConstraint,
        HashSet<string> visited,
        CancellationToken cancellationToken)
    {
        if (visited.Contains(packageId))
        {
            // Circular reference, return minimal node
            return new DependencyTreeNode
            {
                PackageId = packageId,
                Version = versionConstraint ?? "*",
                IsCircular = true
            };
        }

        var package = await GetPackageAsync(packageId, null, cancellationToken);
        if (package == null)
            return null;

        visited.Add(packageId);

        var children = new List<DependencyTreeNode>();

        // Load full manifest to get dependencies
        var manifest = await LoadManifestAsync(packageId, cancellationToken);
        if (manifest?.SurgewaveDependencies != null)
        {
            foreach (var dep in manifest.SurgewaveDependencies)
            {
                var childNode = await BuildTreeNodeAsync(dep.Id, dep.Version, visited, cancellationToken);
                if (childNode != null)
                {
                    children.Add(childNode);
                }
                else if (!dep.Optional)
                {
                    children.Add(new DependencyTreeNode
                    {
                        PackageId = dep.Id,
                        Version = dep.Version,
                        IsMissing = true,
                        IsOptional = dep.Optional
                    });
                }
            }
        }

        visited.Remove(packageId);

        return new DependencyTreeNode
        {
            PackageId = packageId,
            Version = package.Version,
            InstalledVersion = package.InstalledVersion,
            IsInstalled = package.IsInstalled,
            Children = children
        };
    }

    private async Task<PluginManifest?> LoadManifestAsync(
        string packageId,
        CancellationToken cancellationToken)
    {
        // Try to get package info and convert to manifest
        var package = await GetPackageAsync(packageId, null, cancellationToken);
        if (package == null)
            return null;

        // Convert ConnectorPackageInfo to PluginManifest
        return new PluginManifest
        {
            Id = package.PackageId,
            Name = package.Name,
            Version = package.Version,
            Description = package.Description,
            Authors = package.Author != null ? [package.Author] : null,
            License = package.License,
            ProjectUrl = package.ProjectUrl,
            Tags = package.Tags.ToArray(),
            SurgewaveDependencies = package.Dependencies
                .Select(d => new PluginDependency
                {
                    Id = d.PackageId,
                    Version = d.VersionConstraint,
                    Optional = d.Optional
                })
                .ToArray(),
            Assemblies = []
        };
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var repo in _repositories)
        {
            (repo as IDisposable)?.Dispose();
        }
        _repositories.Clear();
        _installer.Dispose();
    }
}
