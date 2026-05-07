using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// List supported schema types (surgewave schema types)
/// </summary>
public class SchemaTypesCommand : CommandBase
{
    public SchemaTypesCommand() : base("types", "List supported schema types")
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

            var types = await client.Schema.GetSchemaTypesAsync(ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(types, SchemaJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var type in types)
                {
                    Console.WriteLine(type);
                }
            }
            else
            {
                AnsiConsole.MarkupLine("[bold]Supported schema types:[/]");
                foreach (var type in types)
                {
                    AnsiConsole.MarkupLine($"  - {type}");
                }
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get schema types: {ex.Message}");
            return 1;
        }
    }
}
