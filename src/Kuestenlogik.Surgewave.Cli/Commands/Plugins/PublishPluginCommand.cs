using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Publish a plugin package to a registry (surgewave plugins publish)
/// </summary>
public class PublishPluginCommand : CommandBase
{
    private readonly Argument<string> _packageArg = new("package")
    {
        Description = "Path to the .swpkg file to publish"
    };

    private readonly Option<string?> _registryOpt = new("--registry", "-r")
    {
        Description = "Registry name from configuration"
    };

    private readonly Option<string?> _registryPathOpt = new("--registry-path")
    {
        Description = "Direct path to a local registry directory"
    };

    private readonly Option<bool> _forceOpt = new("--force", "-f")
    {
        Description = "Overwrite existing package version"
    };

    public PublishPluginCommand() : base("publish", "Publish a plugin package to a registry")
    {
        Arguments.Add(_packageArg);
        Options.Add(_registryOpt);
        Options.Add(_registryPathOpt);
        Options.Add(_forceOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var packagePath = parseResult.GetValue(_packageArg);
        var registryName = parseResult.GetValue(_registryOpt);
        var registryPath = parseResult.GetValue(_registryPathOpt);
        var force = parseResult.GetValue(_forceOpt);

        if (string.IsNullOrEmpty(packagePath))
        {
            WriteError("Package path is required.");
            return 1;
        }

        var fullPackagePath = Path.GetFullPath(packagePath);
        if (!File.Exists(fullPackagePath))
        {
            WriteError($"Package not found: {fullPackagePath}");
            return 1;
        }

        // Validate the package first
        var manager = new PluginPackageManager();
        var validation = await manager.ValidateAsync(fullPackagePath, ct);
        if (!validation.IsValid)
        {
            WriteError("Package validation failed:");
            foreach (var error in validation.Errors)
            {
                AnsiConsole.MarkupLine($"  [red]{error}[/]");
            }
            return 1;
        }

        // Resolve registry path
        string resolvedRegistryPath;
        if (!string.IsNullOrEmpty(registryPath))
        {
            resolvedRegistryPath = Path.GetFullPath(registryPath);
        }
        else if (!string.IsNullOrEmpty(registryName))
        {
            // Look up from configuration
            var config = RepositoryConfiguration.Load();
            var repo = config.Repositories.FirstOrDefault(r =>
                r.Name.Equals(registryName, StringComparison.OrdinalIgnoreCase));

            if (repo == null)
            {
                WriteError($"Registry '{registryName}' not found in configuration.");
                WriteMarkup("[dim]Use 'surgewave plugins repo list' to see configured registries.[/]");
                return 1;
            }

            resolvedRegistryPath = repo.Source;
        }
        else
        {
            // Default: local registry directory
            resolvedRegistryPath = Path.GetFullPath("registry");
        }

        // Publish
        var publisher = new LocalRegistryPublisher(resolvedRegistryPath);

        var result = await AnsiConsole.Status()
            .Spinner(Spinner.Known.Dots)
            .StartAsync("Publishing package...", async _ =>
                await publisher.PublishAsync(fullPackagePath, force, ct));

        if (!result.Success)
        {
            WriteError($"Publish failed: {result.Error}");
            return 1;
        }

        // Show warnings
        foreach (var warning in result.Warnings)
        {
            WriteWarning(warning);
        }

        WriteSuccess($"Published {result.PackageId} v{result.Version}");
        AnsiConsole.MarkupLine($"  [dim]Location:[/] {result.RegistryPath}");

        // Show SHA256
        var sha256 = await PackageChecksumCalculator.ComputeAsync(fullPackagePath, ct);
        AnsiConsole.MarkupLine($"  [dim]SHA256:[/] {sha256}");

        return 0;
    }
}
