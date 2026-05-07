using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Validate a plugin package (surgewave plugins validate)
/// </summary>
public class ValidatePluginCommand : CommandBase
{
    private readonly Argument<string> _packageArg = new("package")
    {
        Description = "Path to the .swpkg file to validate"
    };

    public ValidatePluginCommand() : base("validate", "Validate a plugin package")
    {
        Arguments.Add(_packageArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var packagePath = parseResult.GetValue(_packageArg);

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

        var manager = new PluginPackageManager();
        var result = await manager.ValidateAsync(fullPackagePath, ct);

        if (result.IsValid)
        {
            WriteSuccess($"Package is valid: {Path.GetFileName(packagePath)}");

            var manifest = result.Manifest!;

            // Show manifest info
            var table = new Table();
            table.AddColumn("Field");
            table.AddColumn("Value");

            table.AddRow("ID", manifest.Id);
            table.AddRow("Name", manifest.Name);
            table.AddRow("Version", manifest.Version);

            if (!string.IsNullOrEmpty(manifest.Description))
                table.AddRow("Description", manifest.Description);

            if (manifest.Authors?.Length > 0)
                table.AddRow("Authors", string.Join(", ", manifest.Authors));

            if (!string.IsNullOrEmpty(manifest.License))
                table.AddRow("License", manifest.License);

            if (manifest.Tags?.Length > 0)
                table.AddRow("Tags", string.Join(", ", manifest.Tags));

            if (!string.IsNullOrEmpty(manifest.MinRuntimeVersion))
                table.AddRow("Min Runtime", manifest.MinRuntimeVersion);

            // Compute and show SHA256
            var sha256 = await PackageChecksumCalculator.ComputeAsync(fullPackagePath, ct);
            table.AddRow("SHA256", sha256);

            AnsiConsole.Write(table);

            // Show dependencies
            if (manifest.Dependencies?.Count > 0)
            {
                WriteLine();
                AnsiConsole.MarkupLine("[bold]Dependencies:[/]");

                foreach (var dep in manifest.Dependencies)
                {
                    AnsiConsole.MarkupLine($"  [dim]{dep.Key}[/] {dep.Value}");
                }
            }

            // Show warnings
            if (result.Warnings.Count > 0)
            {
                WriteLine();
                foreach (var warning in result.Warnings)
                {
                    WriteWarning(warning);
                }
            }

            return 0;
        }
        else
        {
            WriteError($"Package is invalid: {Path.GetFileName(packagePath)}");

            foreach (var error in result.Errors)
            {
                AnsiConsole.MarkupLine($"  [red]- {error}[/]");
            }

            foreach (var warning in result.Warnings)
            {
                AnsiConsole.MarkupLine($"  [yellow]- {warning}[/]");
            }

            return 1;
        }
    }
}
