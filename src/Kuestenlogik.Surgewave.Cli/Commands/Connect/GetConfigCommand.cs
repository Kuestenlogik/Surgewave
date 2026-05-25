using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// Get connector config (surgewave connect config get)
/// </summary>
public class GetConfigCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Connector name" };

    public GetConfigCommand() : base("get", "Get connector configuration")
    {
        Arguments.Add(_nameArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var name = parseResult.GetValue(_nameArg);

        WriteVerbose(parseResult, $"Getting config for connector '{name}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var config = await client.Connect.GetConnectorConfigAsync(name, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(config, ConnectJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var (key, value) in config)
                {
                    Console.WriteLine($"{key}={value}");
                }
            }
            else
            {
                var table = new Table();
                table.AddColumn("Key");
                table.AddColumn("Value");

                foreach (var (key, value) in config.OrderBy(kv => kv.Key))
                {
                    table.AddRow(key, value ?? "[dim]null[/]");
                }

                AnsiConsole.Write(table);
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get connector config: {ex.Message}");
            return 1;
        }
    }
}
