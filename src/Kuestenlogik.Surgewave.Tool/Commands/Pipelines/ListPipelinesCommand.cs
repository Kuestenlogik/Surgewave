using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Pipelines.Publishing;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Pipelines;

/// <summary>
/// List pipelines on the broker (surgewave pipelines list)
/// </summary>
public class ListPipelinesCommand : CommandBase
{
    public ListPipelinesCommand() : base("list", "List pipelines on the broker")
    {
        Aliases.Add("ls");
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var format = GetFormat(parseResult);

        try
        {
            using var publisher = PipelineCli.CreatePublisher(parseResult);
            var pipelines = await publisher.ListAsync(ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(pipelines, JsonOptions.Indented));
                return 0;
            }

            if (format == OutputFormat.Plain)
            {
                foreach (var pipeline in pipelines)
                {
                    Console.WriteLine($"{pipeline.Id}\t{pipeline.Name}\t{PipelineCli.StatusText(pipeline.Status)}\t{pipeline.Nodes.Count}");
                }

                return 0;
            }

            if (pipelines.Count == 0)
            {
                WriteLine("No pipelines found.");
                return 0;
            }

            var table = new Table();
            table.AddColumn("Id");
            table.AddColumn("Name");
            table.AddColumn("Status");
            table.AddColumn("Nodes");

            foreach (var pipeline in pipelines)
            {
                table.AddRow(
                    Markup.Escape(pipeline.Id),
                    Markup.Escape(pipeline.Name),
                    PipelineCli.StatusMarkup(pipeline.Status),
                    pipeline.Nodes.Count.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }

            WriteRenderable(table);
            return 0;
        }
        catch (PipelinePublishException ex)
        {
            WriteError(ex.Message);
            return 1;
        }
    }
}
