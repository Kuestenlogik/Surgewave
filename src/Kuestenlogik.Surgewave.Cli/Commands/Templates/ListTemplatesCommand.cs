using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Templates;

/// <summary>
/// List available pipeline templates (surgewave templates list)
/// </summary>
public class ListTemplatesCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Option<string?> _categoryOpt = new("--category", "-c")
    {
        Description = "Filter by category"
    };

    public ListTemplatesCommand() : base("list", "List available pipeline templates")
    {
        Options.Add(_categoryOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var category = parseResult.GetValue(_categoryOpt);
        var format = GetFormat(parseResult);

        using var manager = new PipelineTemplateManager();

        var templates = await AnsiConsole.Status()
            .Spinner(Spinner.Known.Dots)
            .StartAsync("Loading templates...", async _ =>
                await manager.SearchAsync(category: category, cancellationToken: ct));

        if (templates.Count == 0)
        {
            WriteWarning("No templates found.");
            return 0;
        }

        if (format == OutputFormat.Json)
        {
            var json = JsonSerializer.Serialize(templates.Select(t => new
            {
                t.Id,
                t.Name,
                t.Category,
                t.Description,
                t.Author,
                t.Version,
                t.Downloads,
                t.SourceRepository
            }), JsonOptions);
            System.Console.WriteLine(json);
        }
        else if (format == OutputFormat.Plain)
        {
            foreach (var template in templates)
            {
                System.Console.WriteLine($"{template.Id}\t{template.Name}\t{template.Category}");
            }
        }
        else
        {
            // Group by category
            var grouped = templates.GroupBy(t => t.Category).OrderBy(g => g.Key);

            foreach (var group in grouped)
            {
                AnsiConsole.MarkupLine($"\n[bold cyan]{group.Key}[/]");

                var table = new Table();
                table.Border(TableBorder.Rounded);
                table.AddColumn("ID");
                table.AddColumn("Name");
                table.AddColumn("Description");
                table.AddColumn("Source");

                foreach (var template in group.OrderBy(t => t.Name))
                {
                    var source = template.SourceRepository == "built-in"
                        ? "[dim]built-in[/]"
                        : $"[cyan]{template.SourceRepository}[/]";

                    var desc = template.Description.Length > 50
                        ? template.Description[..47] + "..."
                        : template.Description;

                    table.AddRow(
                        $"[yellow]{template.Id}[/]",
                        template.Name,
                        $"[dim]{desc}[/]",
                        source);
                }

                AnsiConsole.Write(table);
            }

            AnsiConsole.MarkupLine($"\n[dim]Total: {templates.Count} template(s)[/]");
            AnsiConsole.MarkupLine("[dim]Use 'surgewave templates show <id>' to see details[/]");
        }

        return 0;
    }
}
