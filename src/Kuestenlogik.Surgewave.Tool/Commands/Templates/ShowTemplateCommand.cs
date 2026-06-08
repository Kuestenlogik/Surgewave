using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Templates;

/// <summary>
/// Show details of a pipeline template (surgewave templates show)
/// </summary>
public class ShowTemplateCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Argument<string> _idArg = new("id")
    {
        Description = "Template ID"
    };

    public ShowTemplateCommand() : base("show", "Show details of a pipeline template")
    {
        Arguments.Add(_idArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var id = parseResult.GetValue(_idArg);
        var format = GetFormat(parseResult);

        if (string.IsNullOrEmpty(id))
        {
            WriteError("Template ID is required.");
            return 1;
        }

        using var manager = new PipelineTemplateManager();

        var template = await AnsiConsole.Status()
            .Spinner(Spinner.Known.Dots)
            .StartAsync("Loading template...", async _ =>
                await manager.GetAsync(id, ct));

        if (template == null)
        {
            WriteError($"Template not found: {id}");
            return 1;
        }

        if (format == OutputFormat.Json)
        {
            var json = JsonSerializer.Serialize(template, JsonOptions);
            System.Console.WriteLine(json);
        }
        else if (format == OutputFormat.Plain)
        {
            System.Console.WriteLine($"{template.Id}\t{template.Name}\t{template.Category}\t{template.Version}\t{template.Author ?? string.Empty}\t{string.Join(",", template.Tags)}");
            if (template.Pipeline?.Nodes is { Count: > 0 } nodes)
            {
                foreach (var node in nodes)
                {
                    var typeName = node.ConnectorType.Split('.').LastOrDefault() ?? node.ConnectorType;
                    System.Console.WriteLine($"  {node.Label ?? node.NodeId}\t{typeName}");
                }
            }
        }
        else
        {
            // Header
            var panel = new Panel($"[bold]{template.Name}[/]")
            {
                Header = new PanelHeader($"[cyan]{template.Category}[/]"),
                Border = BoxBorder.Rounded
            };
            AnsiConsole.Write(panel);

            // Details
            var grid = new Grid();
            grid.AddColumn();
            grid.AddColumn();

            grid.AddRow("[dim]ID:[/]", $"[yellow]{template.Id}[/]");
            grid.AddRow("[dim]Description:[/]", template.Description);
            grid.AddRow("[dim]Version:[/]", template.Version);
            grid.AddRow("[dim]Author:[/]", template.Author ?? "Unknown");
            grid.AddRow("[dim]Source:[/]", template.SourceRepository ?? "Unknown");

            if (template.Tags.Count > 0)
            {
                var tags = string.Join(", ", template.Tags.Select(t => $"[cyan]{t}[/]"));
                grid.AddRow("[dim]Tags:[/]", tags);
            }

            AnsiConsole.Write(grid);

            // Pipeline nodes
            if (template.Pipeline != null)
            {
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine("[bold]Pipeline Nodes:[/]");

                var nodeTable = new Table();
                nodeTable.Border(TableBorder.Simple);
                nodeTable.AddColumn("Label");
                nodeTable.AddColumn("Type");

                foreach (var node in template.Pipeline.Nodes)
                {
                    var typeName = node.ConnectorType.Split('.').LastOrDefault() ?? node.ConnectorType;
                    nodeTable.AddRow(
                        node.Label ?? node.NodeId,
                        $"[dim]{typeName}[/]");
                }

                AnsiConsole.Write(nodeTable);

                // Connections
                if (template.Pipeline.Connections.Count > 0)
                {
                    AnsiConsole.WriteLine();
                    AnsiConsole.MarkupLine("[bold]Connections:[/]");

                    var nodeLabels = template.Pipeline.Nodes.ToDictionary(
                        n => n.NodeId,
                        n => n.Label ?? n.NodeId);

                    foreach (var conn in template.Pipeline.Connections)
                    {
                        var from = nodeLabels.GetValueOrDefault(conn.SourceNodeId, conn.SourceNodeId);
                        var to = nodeLabels.GetValueOrDefault(conn.TargetNodeId, conn.TargetNodeId);
                        AnsiConsole.MarkupLine($"  [cyan]{from}[/] -> [green]{to}[/]");
                    }
                }
            }

            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[dim]Create pipeline from this template:[/]");
            AnsiConsole.MarkupLine($"[dim]  curl -X POST http://localhost:5000/api/pipelines/templates/{id}/create[/]");
        }

        return 0;
    }
}
