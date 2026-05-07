using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// List all connectors (surgewave connect list)
/// </summary>
public class ListConnectorsCommand : CommandBase
{
    public ListConnectorsCommand() : base("list", "List all connectors")
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

            var connectors = await client.Connect.ListAsync(ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(connectors, ConnectJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var connector in connectors)
                {
                    Console.WriteLine(connector);
                }
            }
            else
            {
                if (connectors.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No connectors found[/]");
                    return 0;
                }

                var table = new Table();
                table.AddColumn("Connector");
                table.AddColumn("Type");
                table.AddColumn("State");
                table.AddColumn("Tasks");

                foreach (var name in connectors.OrderBy(c => c))
                {
                    var status = await client.Connect.GetConnectorStatusAsync(name, ct);
                    if (status != null)
                    {
                        var state = status.State.ToUpperInvariant() switch
                        {
                            "RUNNING" => "[green]Running[/]",
                            "PAUSED" => "[yellow]Paused[/]",
                            "FAILED" => "[red]Failed[/]",
                            _ => status.State
                        };
                        table.AddRow(name, status.Type, state, status.Tasks.Count.ToString());
                    }
                    else
                    {
                        table.AddRow(name, "[dim]unknown[/]", "[dim]unknown[/]", "[dim]?[/]");
                    }
                }

                AnsiConsole.Write(table);
                AnsiConsole.MarkupLine($"\n[dim]Total: {connectors.Count} connector(s)[/]");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list connectors: {ex.Message}");
            return 1;
        }
    }
}
