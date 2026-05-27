using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Templates;

/// <summary>
/// Search for pipeline templates (surgewave templates search)
/// </summary>
public class SearchTemplatesCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Argument<string> _queryArg = new("query")
    {
        Description = "Search query (name, tags, description)"
    };

    private readonly Option<string?> _categoryOpt = new("--category", "-c")
    {
        Description = "Filter by category"
    };

    private readonly Option<int> _takeOpt = new("--take", "-t")
    {
        Description = "Number of results to return",
        DefaultValueFactory = _ => 20
    };

    public SearchTemplatesCommand() : base("search", "Search for pipeline templates")
    {
        Arguments.Add(_queryArg);
        Options.Add(_categoryOpt);
        Options.Add(_takeOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var query = parseResult.GetValue(_queryArg);
        var category = parseResult.GetValue(_categoryOpt);
        var take = parseResult.GetValue(_takeOpt);
        var format = GetFormat(parseResult);

        using var manager = new PipelineTemplateManager();

        var templates = await AnsiConsole.Status()
            .Spinner(Spinner.Known.Dots)
            .StartAsync($"Searching for '{query}'...", async _ =>
                await manager.SearchAsync(query, category, 0, take, ct));

        if (templates.Count == 0)
        {
            WriteWarning($"No templates found matching '{query}'.");
            return 0;
        }

        if (format == OutputFormat.Json)
        {
            var json = JsonSerializer.Serialize(templates, JsonOptions);
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
            var table = new Table();
            table.Border(TableBorder.Rounded);
            table.AddColumn("ID");
            table.AddColumn("Name");
            table.AddColumn("Category");
            table.AddColumn("Description");

            foreach (var template in templates)
            {
                var desc = template.Description.Length > 40
                    ? template.Description[..37] + "..."
                    : template.Description;

                table.AddRow(
                    $"[yellow]{template.Id}[/]",
                    template.Name,
                    $"[cyan]{template.Category}[/]",
                    $"[dim]{desc}[/]");
            }

            AnsiConsole.Write(table);
            AnsiConsole.MarkupLine($"\n[dim]Found {templates.Count} template(s)[/]");
        }

        return 0;
    }
}
