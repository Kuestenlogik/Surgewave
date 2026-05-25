using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Cli.Commands.Config;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Plugins.Sources;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Install a plugin package (surgewave plugins install)
/// </summary>
public class InstallPluginCommand : CommandBase
{
    private readonly Argument<string> _packageArg = new("package")
    {
        Description = "Path to .swpkg file, directory containing .swpkg files, glob pattern (*.swpkg), or package ID"
    };

    private readonly Option<string> _directoryOpt = new("--directory", "-d")
    {
        Description = "Target plugins directory",
        DefaultValueFactory = _ => "plugins"
    };

    private readonly Option<bool> _forceOpt = new("--force", "-f")
    {
        Description = "Overwrite existing installation"
    };

    private readonly Option<string?> _fromUrlOpt = new("--from-url")
    {
        Description = "Download and install from a direct URL"
    };

    private readonly Option<bool> _fromNuGetOpt = new("--from-nuget")
    {
        Description = "Install from NuGet repository (package argument is the package ID)"
    };

    private readonly Option<string?> _versionOpt = new("--version", "-v")
    {
        Description = "Specific version to install (with --from-nuget)"
    };

    private readonly Option<string> _installDirOpt = new("--install-dir")
    {
        Description = "Connector installation directory (for repository installs)",
        DefaultValueFactory = _ => GetDefaultInstallDirectory()
    };

    private readonly Option<bool> _noDepsOpt = new("--no-deps")
    {
        Description = "Skip automatic dependency installation"
    };

    private readonly Option<bool> _dryRunOpt = new("--dry-run")
    {
        Description = "Show what would be installed without actually installing"
    };

    private readonly Option<string?> _sourceOpt = new("--source", "-s")
    {
        Description = "Plugin source name to download from (configured via 'surgewave plugins source')"
    };

    private readonly Option<string?> _validateConfigOpt = new("--validate-config")
    {
        Description = "After install, validate the given appsettings.json against IValidatableConfig types in the freshly-installed plugin (and any other installed plugins). Reports errors but does not roll back the install."
    };

    private readonly Option<string> _signerOpt = new("--signer")
    {
        Description = "Signer provider name for signature verification (default: builtin-ecdsa). Additional providers are discovered under --directory.",
        DefaultValueFactory = _ => "builtin-ecdsa"
    };

    private readonly Option<bool> _requireSignedOpt = new("--require-signed")
    {
        Description = "Reject unsigned packages. When false, unsigned packages pass through but signed-but-untrusted ones are always rejected."
    };

    private readonly Option<string?> _rootsOpt = new("--roots")
    {
        Description = "sealbolt: comma-separated trusted root certificate paths"
    };

    // Resolved once in ExecuteAsync, used by all sub-methods that complete a successful install.
    private string? _validateConfig;
    private ISppSigner? _verifier;
    private PluginPackageSignerRegistry? _verifierRegistry;
    private bool _requireSigned;

    public InstallPluginCommand() : base("install", "Install a plugin package")
    {
        Arguments.Add(_packageArg);
        Options.Add(_directoryOpt);
        Options.Add(_forceOpt);
        Options.Add(_fromUrlOpt);
        Options.Add(_fromNuGetOpt);
        Options.Add(_versionOpt);
        Options.Add(_installDirOpt);
        Options.Add(_noDepsOpt);
        Options.Add(_dryRunOpt);
        Options.Add(_sourceOpt);
        Options.Add(_validateConfigOpt);
        Options.Add(_signerOpt);
        Options.Add(_requireSignedOpt);
        Options.Add(_rootsOpt);
        this.SetAction(ExecuteAsync);
    }

    private void ResolveVerifier(ParseResult parseResult, string pluginsDir)
    {
        var signerName = parseResult.GetValue(_signerOpt) ?? "builtin-ecdsa";
        _requireSigned = parseResult.GetValue(_requireSignedOpt);

        var hasExplicitFlags = !string.IsNullOrEmpty(parseResult.GetValue(_rootsOpt))
            || !signerName.Equals("builtin-ecdsa", StringComparison.OrdinalIgnoreCase);

        // Skip verifier construction entirely when the user has neither opted into
        // signing nor selected a non-default provider — keeps dev workflows friction-free.
        if (!_requireSigned && !hasExplicitFlags && !Directory.Exists(Path.Combine(pluginsDir, "trusted-keys")))
            return;

        var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["trusted-keys-dir"] = Path.Combine(pluginsDir, "trusted-keys")
        };
        if (parseResult.GetValue(_rootsOpt) is { Length: > 0 } roots) options["roots"] = roots;

        _verifierRegistry = PluginPackageSignerRegistry.LoadFrom(pluginsDir);
        try
        {
            _verifier = _verifierRegistry.GetProvider(signerName).Create(options);
        }
        catch
        {
            _verifierRegistry.Dispose();
            _verifierRegistry = null;
            throw;
        }
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var package = parseResult.GetValue(_packageArg);
        var directory = parseResult.GetValue(_directoryOpt) ?? "plugins";
        var force = parseResult.GetValue(_forceOpt);
        var fromUrl = parseResult.GetValue(_fromUrlOpt);
        var fromNuGet = parseResult.GetValue(_fromNuGetOpt);
        var version = parseResult.GetValue(_versionOpt);
        var installDir = parseResult.GetValue(_installDirOpt) ?? GetDefaultInstallDirectory();
        var noDeps = parseResult.GetValue(_noDepsOpt);
        var dryRun = parseResult.GetValue(_dryRunOpt);
        var sourceName = parseResult.GetValue(_sourceOpt);
        _validateConfig = parseResult.GetValue(_validateConfigOpt);

        if (string.IsNullOrEmpty(package) && string.IsNullOrEmpty(fromUrl))
        {
            WriteError("Package path, package ID, or --from-url is required.");
            return 1;
        }

        try
        {
            ResolveVerifier(parseResult, Path.GetFullPath(directory));
        }
        catch (Exception ex) when (ex is ArgumentException or KeyNotFoundException)
        {
            WriteError($"Signer configuration invalid: {ex.Message}");
            return 1;
        }

        // Handle --from-url: Download from URL first
        if (!string.IsNullOrEmpty(fromUrl))
        {
            return await InstallFromUrlAsync(fromUrl, directory, force, ct);
        }

        // Handle --from-nuget: Install from repository
        if (fromNuGet)
        {
            return await InstallFromNuGetAsync(package!, version, installDir, noDeps, dryRun, ct);
        }

        // Handle directory (with optional ** for recursive)
        var trimmedPackage = package!.TrimEnd('/', '\\');
        if (trimmedPackage.EndsWith("**"))
        {
            var baseDir = trimmedPackage[..^2].TrimEnd('/', '\\');
            if (Directory.Exists(baseDir))
                return await InstallAllFromDirectoryAsync(baseDir, directory, force, ct, recursive: true);
        }

        if (Directory.Exists(package))
        {
            return await InstallAllFromDirectoryAsync(package, directory, force, ct);
        }

        if (package.Contains('*') || package.Contains('?'))
        {
            var recursive = package.Contains("**");
            var normalized = package.Replace("**/", "").Replace("**\\", "");
            var dir = Path.GetDirectoryName(normalized);
            if (string.IsNullOrEmpty(dir)) dir = ".";
            var pattern = Path.GetFileName(normalized);
            var searchOption = recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
            var files = Directory.GetFiles(Path.GetFullPath(dir), pattern, searchOption);
            if (files.Length == 0)
            {
                WriteError($"No files matching '{package}'");
                return 1;
            }
            return await InstallMultipleAsync(files, directory, force, ct);
        }

        // Handle --source or non-local-file: Download from plugin source first
        var fullPackagePath = Path.GetFullPath(package);

        if (!string.IsNullOrEmpty(sourceName) || !File.Exists(fullPackagePath))
        {
            // Try to install from configured plugin sources
            var sourceResult = await InstallFromSourceAsync(package!, version, sourceName, directory, force, ct);
            if (sourceResult.HasValue)
                return sourceResult.Value;

            // If no source handled it and it's not a local file, error out
            if (!File.Exists(fullPackagePath))
            {
                WriteError($"Package not found: {fullPackagePath}");
                WriteMarkup("[dim]Use --source to specify a plugin source, --from-url to download from a URL, or --from-nuget to install from NuGet.[/]");
                return 1;
            }
        }

        // Default: Install from local file
        var pluginsDir = Path.GetFullPath(directory);

        var manager = new PluginPackageManager();

        // Show progress
        var result = await AnsiConsole.Status()
            .Spinner(Spinner.Known.Dots)
            .StartAsync("Installing plugin...", async ctx =>
            {
                return await manager.InstallAsync(fullPackagePath, pluginsDir, force, verifier: _verifier, requireSigned: _requireSigned, cancellationToken: ct);
            });

        if (!result.Success)
        {
            WriteError($"Installation failed: {result.Error}");
            return 1;
        }

        // Show SHA256 verification
        var sha256 = await PackageChecksumCalculator.ComputeAsync(fullPackagePath, ct);
        AnsiConsole.MarkupLine($"  [dim]SHA256:[/] {sha256}");

        var manifest = result.Manifest!;

        if (result.WasUpgrade)
        {
            WriteSuccess($"Upgraded {manifest.Name} from v{result.PreviousVersion} to v{manifest.Version}");
        }
        else
        {
            WriteSuccess($"Installed {manifest.Name} v{manifest.Version}");
        }

        // Optional post-install validation: re-run the validation pipeline against
        // the user's appsettings, now with the freshly-installed plugin's defaults
        // and IValidatableConfig types in scope. We deliberately do NOT fail the
        // install on validation errors — the install was successful, the user just
        // needs to fix their config before restarting the broker.
        if (!string.IsNullOrWhiteSpace(_validateConfig))
        {
            RunPostInstallValidation(_validateConfig, pluginsDir);
        }

        return 0;
    }

    private static void RunPostInstallValidation(string configPath, string pluginsDir)
    {
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[dim]Post-install config validation:[/]");
        // ConfigValidateCommand looks for plugins/ inside the assemblies dir, so we
        // pass the parent of the install plugins directory as assembliesDir. For the
        // common case where pluginsDir is '<broker>/plugins/', the parent is the
        // broker bin and config validate finds both broker types and plugin types.
        var parent = Path.GetDirectoryName(pluginsDir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        var assembliesDir = string.IsNullOrEmpty(parent) ? null : parent;
        var exit = ConfigValidateCommand.Execute(configPath, assembliesDir, verbose: false);
        if (exit != 0)
        {
            AnsiConsole.MarkupLine("[yellow]Validation reported errors. The plugin is installed; fix the config before restarting the broker.[/]");
        }
    }

    private async Task<int> InstallFromUrlAsync(string url, string directory, bool force, CancellationToken ct)
    {
        var pluginsDir = Path.GetFullPath(directory);

        // Download the package
        string downloadedPath;
        try
        {
            downloadedPath = await AnsiConsole.Status()
                .Spinner(Spinner.Known.Dots)
                .StartAsync($"Downloading from {url}...", async _ =>
                {
                    using var httpClient = new HttpClient();
                    var tempDir = Path.Combine(Path.GetTempPath(), $"surgewave-download-{Guid.NewGuid()}");
                    Directory.CreateDirectory(tempDir);

                    var repo = new HttpConnectorRepository("download", url, httpClient: httpClient);
                    return await repo.DownloadFromUrlAsync(url, tempDir, ct);
                });
        }
        catch (HttpRequestException ex)
        {
            WriteError($"Download failed: {ex.Message}");
            return 1;
        }

        try
        {
            // Install the downloaded package
            var manager = new PluginPackageManager();
            var result = await AnsiConsole.Status()
                .Spinner(Spinner.Known.Dots)
                .StartAsync("Installing plugin...", async _ =>
                    await manager.InstallAsync(downloadedPath, pluginsDir, force, verifier: _verifier, requireSigned: _requireSigned, cancellationToken: ct));

            if (!result.Success)
            {
                WriteError($"Installation failed: {result.Error}");
                return 1;
            }

            var manifest = result.Manifest!;
            WriteSuccess($"Installed {manifest.Name} v{manifest.Version} from URL");

            if (!string.IsNullOrWhiteSpace(_validateConfig))
            {
                RunPostInstallValidation(_validateConfig, pluginsDir);
            }
            return 0;
        }
        finally
        {
            // Clean up downloaded file
            var tempDir = Path.GetDirectoryName(downloadedPath);
            if (tempDir != null && Directory.Exists(tempDir))
            {
                try { Directory.Delete(tempDir, true); }
                catch { /* Ignore cleanup errors */ }
            }
        }
    }

    private async Task<int> InstallFromNuGetAsync(string packageId, string? version, string installDir, bool noDeps, bool dryRun, CancellationToken ct)
    {
        // Load repository configuration
        var config = RepositoryConfiguration.Load();

        using var repoManager = new ConnectorRepositoryManager(installDir);

        // Add configured repositories
        foreach (var repo in config.CreateRepositories())
        {
            if (repo.Name != "nuget.org")
            {
                repoManager.AddRepository(repo);
            }
        }

        try
        {
            // Find the package first
            var package = await AnsiConsole.Status()
                .Spinner(Spinner.Known.Dots)
                .StartAsync($"Finding {packageId}...", async _ =>
                    await repoManager.GetPackageAsync(packageId, version, ct));

            if (package == null)
            {
                WriteError($"Package not found: {packageId}");
                WriteMarkup("[dim]Use 'surgewave plugins search' to find available packages.[/]");
                return 1;
            }

            // Check if already installed at same version
            if (package.IsInstalled && package.InstalledVersion == package.Version)
            {
                WriteWarning($"{packageId} v{package.Version} is already installed.");
                return 0;
            }

            // Resolve dependencies first (unless --no-deps)
            if (!noDeps)
            {
                var resolution = await AnsiConsole.Status()
                    .Spinner(Spinner.Known.Dots)
                    .StartAsync("Resolving dependencies...", async _ =>
                        await repoManager.ResolveDependenciesAsync(packageId, ct));

                // Show warnings
                foreach (var warning in resolution.Warnings)
                {
                    WriteWarning(warning);
                }

                if (!resolution.IsSuccess)
                {
                    WriteError("Dependency resolution failed:");
                    foreach (var error in resolution.Errors)
                    {
                        WriteMarkup($"  [red]{error}[/]");
                    }
                    return 1;
                }

                // Show what will be installed
                if (resolution.ToInstall.Count > 1 || dryRun)
                {
                    AnsiConsole.WriteLine();
                    AnsiConsole.MarkupLine("[bold]Packages to install:[/]");

                    var table = new Table();
                    table.Border(TableBorder.Simple);
                    table.AddColumn("Package");
                    table.AddColumn("Version");
                    table.AddColumn("Type");

                    foreach (var dep in resolution.ToInstall)
                    {
                        var depType = dep.IsRoot ? "[cyan]requested[/]" : "[dim]dependency[/]";
                        var upgrade = dep.InstalledVersion != null
                            ? $" [yellow](upgrade from {dep.InstalledVersion})[/]"
                            : "";
                        table.AddRow($"{dep.Id}{upgrade}", dep.Version, depType);
                    }

                    AnsiConsole.Write(table);

                    if (resolution.AlreadyInstalled.Count > 0)
                    {
                        AnsiConsole.MarkupLine($"[dim]Already installed: {resolution.AlreadyInstalled.Count} package(s)[/]");
                    }
                }

                if (dryRun)
                {
                    AnsiConsole.MarkupLine("[dim]Dry run - no packages were installed.[/]");
                    return 0;
                }

                // Install with dependencies
                if (resolution.ToInstall.Count > 1)
                {
                    var result = await AnsiConsole.Status()
                        .Spinner(Spinner.Known.Dots)
                        .StartAsync($"Installing {resolution.ToInstall.Count} packages...", async _ =>
                            await repoManager.InstallWithDependenciesAsync(packageId, package.Version, ct));

                    if (!result.IsSuccess && !result.IsPartialSuccess)
                    {
                        WriteError("Installation failed:");
                        foreach (var error in result.Errors)
                        {
                            WriteMarkup($"  [red]{error}[/]");
                        }
                        return 1;
                    }

                    // Show results
                    AnsiConsole.WriteLine();
                    if (result.NewlyInstalled > 0)
                    {
                        WriteSuccess($"Installed {result.NewlyInstalled} new package(s)");
                    }
                    if (result.Upgraded > 0)
                    {
                        WriteSuccess($"Upgraded {result.Upgraded} package(s)");
                    }

                    if (result.Errors.Count > 0)
                    {
                        WriteWarning("Some packages failed to install:");
                        foreach (var error in result.Errors)
                        {
                            WriteMarkup($"  [yellow]{error}[/]");
                        }
                    }

                    WriteMarkup($"[dim]Installed to: {installDir}[/]");
                    return result.IsSuccess ? 0 : 1;
                }
            }

            // Single package install (no dependencies or --no-deps)
            if (dryRun)
            {
                AnsiConsole.MarkupLine($"Would install: {package.Name} v{package.Version}");
                AnsiConsole.MarkupLine("[dim]Dry run - no packages were installed.[/]");
                return 0;
            }

            await AnsiConsole.Status()
                .Spinner(Spinner.Known.Dots)
                .StartAsync($"Installing {packageId} v{package.Version}...", async _ =>
                    await repoManager.InstallAsync(packageId, package.Version, ct));

            if (package.IsInstalled)
            {
                WriteSuccess($"Updated {package.Name} from v{package.InstalledVersion} to v{package.Version}");
            }
            else
            {
                WriteSuccess($"Installed {package.Name} v{package.Version}");
            }

            // Show connector types
            if (package.ConnectorTypes.Count > 0)
            {
                var types = string.Join(", ", package.ConnectorTypes);
                WriteMarkup($"[dim]Connector types: {types}[/]");
            }

            WriteMarkup($"[dim]Installed to: {installDir}[/]");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Installation failed: {ex.Message}");
            return 1;
        }
    }

    private async Task<int?> InstallFromSourceAsync(
        string packageId, string? version, string? sourceName,
        string directory, bool force, CancellationToken ct)
    {
        var config = PluginSourceConfig.Load();

        if (config.Sources.Count == 0)
        {
            if (!string.IsNullOrEmpty(sourceName))
            {
                WriteError("No plugin sources configured. Add one with: surgewave plugins source add <name> <url> --type <type>");
                return 1;
            }
            return null; // Fall through to local file handling
        }

        IReadOnlyList<IPluginSource> sources;

        if (!string.IsNullOrEmpty(sourceName))
        {
            var entry = config.Sources.FirstOrDefault(s =>
                s.Name.Equals(sourceName, StringComparison.OrdinalIgnoreCase));

            if (entry == null)
            {
                WriteError($"Plugin source not found: '{sourceName}'");
                WriteMarkup($"[dim]Available sources: {string.Join(", ", config.Sources.Select(s => s.Name))}[/]");
                return 1;
            }

            sources = [PluginSourceFactory.Create(entry)];
        }
        else
        {
            sources = PluginSourceFactory.CreateAll(config);
        }

        var tempDir = Path.Combine(Path.GetTempPath(), $"surgewave-source-{Guid.NewGuid()}");

        try
        {
            foreach (var source in sources)
            {
                try
                {
                    var downloadedPath = await AnsiConsole.Status()
                        .Spinner(Spinner.Known.Dots)
                        .StartAsync($"Downloading {packageId} from {source.Name}...", async _ =>
                            await source.DownloadAsync(packageId, version, tempDir, ct));

                    // If the downloaded path is a directory (NuGet extraction), check for .swpkg
                    if (Directory.Exists(downloadedPath) && !File.Exists(downloadedPath))
                    {
                        // NuGet source returns an extracted directory — already installed
                        var pluginsDir = Path.GetFullPath(directory);
                        var targetDir = Path.Combine(pluginsDir, Path.GetFileName(downloadedPath));
                        Directory.CreateDirectory(pluginsDir);

                        if (Directory.Exists(targetDir))
                        {
                            if (!force)
                            {
                                WriteWarning($"Plugin already exists at {targetDir}. Use --force to overwrite.");
                                return 0;
                            }
                            Directory.Delete(targetDir, true);
                        }

                        CopyDirectory(downloadedPath, targetDir);
                        WriteSuccess($"Installed {packageId} from {source.Name} to {targetDir}");
                        if (!string.IsNullOrWhiteSpace(_validateConfig))
                            RunPostInstallValidation(_validateConfig, Path.GetFullPath(directory));
                        return 0;
                    }

                    // It's an .swpkg file — install it normally
                    if (File.Exists(downloadedPath))
                    {
                        var pluginsDir = Path.GetFullPath(directory);
                        var manager = new PluginPackageManager();

                        var result = await AnsiConsole.Status()
                            .Spinner(Spinner.Known.Dots)
                            .StartAsync("Installing plugin...", async _ =>
                                await manager.InstallAsync(downloadedPath, pluginsDir, force, verifier: _verifier, requireSigned: _requireSigned, cancellationToken: ct));

                        if (!result.Success)
                        {
                            WriteError($"Installation failed: {result.Error}");
                            return 1;
                        }

                        var manifest = result.Manifest!;
                        WriteSuccess($"Installed {manifest.Name} v{manifest.Version} from {source.Name}");
                        if (!string.IsNullOrWhiteSpace(_validateConfig))
                            RunPostInstallValidation(_validateConfig, pluginsDir);
                        return 0;
                    }
                }
                catch (HttpRequestException)
                {
                    // Source didn't have it, try next
                    continue;
                }
                catch (InvalidOperationException)
                {
                    // Source didn't have it, try next
                    continue;
                }
                finally
                {
                    if (source is IDisposable disposable)
                        disposable.Dispose();
                }
            }

            if (!string.IsNullOrEmpty(sourceName))
            {
                WriteError($"Package '{packageId}' not found in source '{sourceName}'.");
                return 1;
            }

            return null; // No source had it — fall through
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                try { Directory.Delete(tempDir, true); }
                catch { /* Ignore cleanup errors */ }
            }
        }
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);

        foreach (var file in Directory.GetFiles(source))
        {
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)), overwrite: true);
        }

        foreach (var dir in Directory.GetDirectories(source))
        {
            CopyDirectory(dir, Path.Combine(destination, Path.GetFileName(dir)));
        }
    }

    private async Task<int> InstallAllFromDirectoryAsync(string dirPath, string pluginsDir, bool force, CancellationToken ct, bool recursive = false)
    {
        var fullDir = Path.GetFullPath(dirPath);
        var searchOption = recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
        var pluginPackageFiles = Directory.GetFiles(fullDir, "*.swpkg", searchOption);
        if (pluginPackageFiles.Length == 0)
        {
            WriteError($"No .swpkg files found in {fullDir}{(recursive ? " (recursive)" : "")}");
            return 1;
        }
        return await InstallMultipleAsync(pluginPackageFiles, pluginsDir, force, ct);
    }

    private async Task<int> InstallMultipleAsync(string[] pluginPackageFiles, string pluginsDir, bool force, CancellationToken ct)
    {
        var targetDir = Path.GetFullPath(pluginsDir);
        var manager = new PluginPackageManager();
        var installed = 0;
        var failed = 0;

        AnsiConsole.MarkupLine($"[cyan]Installing {pluginPackageFiles.Length} plugin(s)...[/]");

        foreach (var pluginPackageFile in pluginPackageFiles.OrderBy(f => f))
        {
            var fileName = Path.GetFileName(pluginPackageFile);
            var result = await manager.InstallAsync(pluginPackageFile, targetDir, force, verifier: _verifier, requireSigned: _requireSigned, cancellationToken: ct);

            if (result.Success)
            {
                var manifest = result.Manifest!;
                var label = result.WasUpgrade
                    ? $"[green]Upgraded[/] {manifest.Name} v{manifest.Version}"
                    : $"[green]Installed[/] {manifest.Name} v{manifest.Version}";
                AnsiConsole.MarkupLine($"  {label}");
                installed++;
            }
            else
            {
                AnsiConsole.MarkupLine($"  [red]Failed[/] {fileName}: {result.Error}");
                failed++;
            }
        }

        AnsiConsole.MarkupLine($"\n[bold]{installed} installed, {failed} failed[/]");
        return failed > 0 ? 1 : 0;
    }

    private static string GetDefaultInstallDirectory()
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return Path.Combine(home, ".surgewave", "connectors");
    }
}
