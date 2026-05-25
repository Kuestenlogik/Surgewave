using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// List available connector plugins (surgewave connect plugins)
/// </summary>
public class PluginsCommand : CommandBase
{
    public PluginsCommand() : base("plugins", "List available connector plugins")
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

            var plugins = await client.Connect.ListConnectorPluginsAsync(ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(plugins.Select(p => new
                {
                    p.ClassName,
                    p.Type,
                    p.Version
                }).ToList(), ConnectJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var plugin in plugins)
                {
                    Console.WriteLine(plugin.ClassName);
                }
            }
            else
            {
                var table = new Table();
                table.AddColumn("Class");
                table.AddColumn("Type");
                table.AddColumn("Version");

                foreach (var plugin in plugins.OrderBy(p => p.ClassName))
                {
                    var typeCol = plugin.Type.ToLowerInvariant() switch
                    {
                        "source" => "[cyan]source[/]",
                        "sink" => "[green]sink[/]",
                        _ => plugin.Type
                    };
                    table.AddRow(plugin.ClassName, typeCol, plugin.Version);
                }

                AnsiConsole.Write(table);
                AnsiConsole.MarkupLine($"\n[dim]Total: {plugins.Count} plugin(s)[/]");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list plugins: {ex.Message}");
            return 1;
        }
    }
}
