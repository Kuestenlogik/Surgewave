using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Topics;

/// <summary>
/// List all topics (surgewave topics list)
/// </summary>
public class ListTopicsCommand : CommandBase
{
    public ListTopicsCommand() : base("list", "List all topics")
    {
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        WriteVerbose(parseResult, $"Connecting to {host}:{port}...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);
            var topics = await client.Topics.ListAsync(ct);

            if (format == OutputFormat.Json)
            {
                var output = topics.Select(t => new { t.Name, t.PartitionCount }).ToList();
                Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var topic in topics.OrderBy(t => t.Name))
                {
                    Console.WriteLine(topic.Name);
                }
            }
            else
            {
                var table = new Table();
                table.AddColumn("Topic");
                table.AddColumn("Partitions");
                table.AddColumn("Internal");

                foreach (var topic in topics.OrderBy(t => t.Name))
                {
                    var isInternal = topic.Name.StartsWith("__", StringComparison.Ordinal) ? "[dim]yes[/]" : "no";
                    table.AddRow(
                        topic.Name,
                        topic.PartitionCount.ToString(),
                        isInternal
                    );
                }

                AnsiConsole.Write(table);
                AnsiConsole.MarkupLine($"\n[dim]Total: {topics.Count} topic(s)[/]");
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list topics: {ex.Message}");
            return 1;
        }
    }
}
