using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Groups;

/// <summary>
/// List all consumer groups (surgewave groups list)
/// </summary>
public class ListGroupsCommand : CommandBase
{
    public ListGroupsCommand() : base("list", "List all consumer groups")
    {
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var groups = await client.Groups.ListAsync(ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(groups, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var group in groups)
                {
                    Console.WriteLine($"{group.GroupId}\t{group.ProtocolType}\t{group.State}");
                }
            }
            else
            {
                if (groups.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No consumer groups found.[/]");
                    return 0;
                }

                var table = new Table();
                table.AddColumn("Group ID");
                table.AddColumn("Protocol Type");
                table.AddColumn("State");

                foreach (var group in groups)
                {
                    var stateColor = group.State switch
                    {
                        "Stable" => "green",
                        "Empty" => "dim",
                        "PreparingRebalance" or "CompletingRebalance" => "yellow",
                        "Dead" => "red",
                        _ => "white"
                    };
                    table.AddRow(
                        $"[cyan]{group.GroupId}[/]",
                        group.ProtocolType,
                        $"[{stateColor}]{group.State}[/]");
                }

                AnsiConsole.Write(table);
                AnsiConsole.MarkupLine($"\n[dim]{groups.Count} consumer group(s)[/]");
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list groups: {ex.Message}");
            return 1;
        }
    }
}
