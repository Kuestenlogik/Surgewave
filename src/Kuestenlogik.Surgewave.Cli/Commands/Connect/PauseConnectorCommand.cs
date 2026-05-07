using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// Pause a connector (surgewave connect pause)
/// </summary>
public class PauseConnectorCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Connector name" };

    public PauseConnectorCommand() : base("pause", "Pause a connector")
    {
        Arguments.Add(_nameArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var name = parseResult.GetValue(_nameArg);

        WriteVerbose(parseResult, $"Pausing connector '{name}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            await client.Connect.PauseConnectorAsync(name, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new { Name = name, Paused = true }, ConnectJsonOptions.Indented));
            }
            else
            {
                WriteSuccess($"Paused connector '{name}'");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to pause connector: {ex.Message}");
            return 1;
        }
    }
}
