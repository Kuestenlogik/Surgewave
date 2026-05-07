using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// Restart a connector (surgewave connect restart)
/// </summary>
public class RestartConnectorCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Connector name" };

    public RestartConnectorCommand() : base("restart", "Restart a connector")
    {
        Arguments.Add(_nameArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var name = parseResult.GetValue(_nameArg);

        WriteVerbose(parseResult, $"Restarting connector '{name}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            await client.Connect.RestartConnectorAsync(name, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new { Name = name, Restarted = true }, ConnectJsonOptions.Indented));
            }
            else
            {
                WriteSuccess($"Restarted connector '{name}'");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to restart connector: {ex.Message}");
            return 1;
        }
    }
}
