using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Templates;

/// <summary>
/// Install a pipeline template locally (surgewave templates install)
/// </summary>
public class InstallTemplateCommand : CommandBase
{
    private readonly Argument<string> _idArg = new("id")
    {
        Description = "Template ID to install"
    };

    private readonly Option<string?> _repoUrlOpt = new("--from-url")
    {
        Description = "Install from a specific repository URL"
    };

    public InstallTemplateCommand() : base("install", "Install a pipeline template locally")
    {
        Arguments.Add(_idArg);
        Options.Add(_repoUrlOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var id = parseResult.GetValue(_idArg);
        var repoUrl = parseResult.GetValue(_repoUrlOpt);

        if (string.IsNullOrEmpty(id))
        {
            WriteError("Template ID is required.");
            return 1;
        }

        using var manager = new PipelineTemplateManager();

        // Add custom repository if specified
        if (!string.IsNullOrEmpty(repoUrl))
        {
            manager.AddHttpRepository("custom", repoUrl);
        }

        // Find and install template
        var template = await AnsiConsole.Status()
            .Spinner(Spinner.Known.Dots)
            .StartAsync($"Installing template '{id}'...", async _ =>
                await manager.InstallAsync(id, ct));

        if (template == null)
        {
            WriteError($"Template not found: {id}");
            return 1;
        }

        WriteSuccess($"Installed template '{template.Name}' v{template.Version}");
        WriteMarkup($"[dim]Category: {template.Category}[/]");

        if (template.Pipeline != null)
        {
            WriteMarkup($"[dim]Nodes: {template.Pipeline.Nodes.Count}[/]");
        }

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[dim]Create a pipeline from this template using the REST API or Surgewave Control UI.[/]");

        return 0;
    }
}
