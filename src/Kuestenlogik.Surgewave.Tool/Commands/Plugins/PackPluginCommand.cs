using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Create a plugin package from build output (surgewave plugins pack)
/// </summary>
public class PackPluginCommand : CommandBase
{
    private readonly Option<string?> _projectOpt = new("--project", "-p")
    {
        Description = "Path to the connector project directory"
    };

    private readonly Option<string?> _sourceDirOpt = new("--source-dir")
    {
        Description = "Explicit build/publish output directory (overrides --project auto-detection)"
    };

    private readonly Option<string> _outputOpt = new("--output", "-o")
    {
        Description = "Output directory for the package",
        DefaultValueFactory = _ => "artifacts/pkg"
    };

    private readonly Option<string> _configurationOpt = new("--configuration", "-c")
    {
        Description = "Build configuration (Debug/Release)",
        DefaultValueFactory = _ => "Release"
    };

    private readonly Option<string?> _manifestOpt = new("--manifest", "-m")
    {
        Description = "Path to custom plugin.json manifest"
    };

    public PackPluginCommand() : base("pack", "Create a plugin package from build output")
    {
        Options.Add(_projectOpt);
        Options.Add(_sourceDirOpt);
        Options.Add(_outputOpt);
        Options.Add(_configurationOpt);
        Options.Add(_manifestOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var projectPath = parseResult.GetValue(_projectOpt);
        var sourceDir = parseResult.GetValue(_sourceDirOpt);
        var outputDir = parseResult.GetValue(_outputOpt) ?? "artifacts/pkg";
        var configuration = parseResult.GetValue(_configurationOpt) ?? "Release";
        var manifestPath = parseResult.GetValue(_manifestOpt);

        // Find the build output directory
        string buildOutputDir;

        if (!string.IsNullOrEmpty(sourceDir))
        {
            // Explicit source directory — skip project auto-detection
            buildOutputDir = Path.GetFullPath(sourceDir);
            if (!Directory.Exists(buildOutputDir))
            {
                WriteError($"Source directory not found: {buildOutputDir}");
                return 1;
            }
        }
        else if (!string.IsNullOrEmpty(projectPath))
        {
            // User specified a project path
            var fullProjectPath = Path.GetFullPath(projectPath);

            if (!Directory.Exists(fullProjectPath))
            {
                WriteError($"Project directory not found: {fullProjectPath}");
                return 1;
            }

            // Look for build output in artifacts/bin or bin
            var artifactsBin = Path.Combine(fullProjectPath, $"../../artifacts/bin/{Path.GetFileName(fullProjectPath)}/{configuration.ToLowerInvariant()}");
            var standardBin = Path.Combine(fullProjectPath, $"bin/{configuration}/net10.0");

            if (Directory.Exists(artifactsBin))
            {
                buildOutputDir = Path.GetFullPath(artifactsBin);
            }
            else if (Directory.Exists(standardBin))
            {
                buildOutputDir = Path.GetFullPath(standardBin);
            }
            else
            {
                WriteError($"Build output not found. Did you run 'dotnet build -c {configuration}'?");
                WriteMarkup($"[dim]Expected at: {artifactsBin}[/]");
                return 1;
            }
        }
        else
        {
            // Use current directory
            buildOutputDir = Directory.GetCurrentDirectory();

            // Check if there are any DLLs to package
            var dlls = Directory.GetFiles(buildOutputDir, "*.dll");
            if (dlls.Length == 0)
            {
                WriteError("No DLLs found in current directory.");
                WriteMarkup("[dim]Use --project or --source-dir to specify the plugin source.[/]");
                return 1;
            }
        }

        WriteVerbose(parseResult, $"Build output: {buildOutputDir}");

        var manager = new PluginPackageManager();
        var fullOutputDir = Path.GetFullPath(outputDir);

        try
        {
            var packagePath = await AnsiConsole.Status()
                .Spinner(Spinner.Known.Dots)
                .StartAsync("Creating package...", async ctx =>
                {
                    return await manager.PackAsync(buildOutputDir, manifestPath, fullOutputDir, signer: null, ct);
                });

            WriteSuccess($"Created package: {packagePath}");

            // Show SHA256 checksum
            var sha256 = await PackageChecksumCalculator.ComputeAsync(packagePath, ct);
            AnsiConsole.MarkupLine($"  [dim]SHA256:[/] {sha256}");

            // Show package info
            var validation = await manager.ValidateAsync(packagePath, ct);
            if (validation.IsValid && validation.Manifest != null)
            {
                var manifest = validation.Manifest;
                AnsiConsole.MarkupLine($"  [dim]ID:[/] {manifest.Id}");
                AnsiConsole.MarkupLine($"  [dim]Version:[/] {manifest.Version}");
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to create package: {ex.Message}");
            return 1;
        }
    }
}
