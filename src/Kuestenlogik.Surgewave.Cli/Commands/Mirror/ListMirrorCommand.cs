using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// List all replication flows (surgewave mirror list)
/// </summary>
public class ListMirrorCommand : CommandBase
{
    public ListMirrorCommand() : base("list", "List all replication flows")
    {
        Aliases.Add("ls");
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var format = GetFormat(parseResult);

        try
        {
            // In a real implementation, this would query Connect API for mirror connectors
            // For now, we show a placeholder
            var mirrors = new[]
            {
                new { Name = "dc1-to-dc2", SourceCluster = "dc1", TargetCluster = "dc2", Status = "RUNNING", Tasks = 4 },
            };

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(mirrors, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var mirror in mirrors)
                {
                    Console.WriteLine($"{mirror.Name}\t{mirror.SourceCluster}->{mirror.TargetCluster}\t{mirror.Status}");
                }
            }
            else
            {
                var table = new Table();
                table.AddColumn("Name");
                table.AddColumn("Source");
                table.AddColumn("Target");
                table.AddColumn("Status");
                table.AddColumn("Tasks");

                foreach (var mirror in mirrors)
                {
                    var statusColor = mirror.Status == "RUNNING" ? "green" : "yellow";
                    table.AddRow(
                        mirror.Name,
                        mirror.SourceCluster,
                        mirror.TargetCluster,
                        $"[{statusColor}]{mirror.Status}[/]",
                        mirror.Tasks.ToString()
                    );
                }

                AnsiConsole.Write(table);
            }

            await Task.CompletedTask;
            return 0;
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }
    }
}
