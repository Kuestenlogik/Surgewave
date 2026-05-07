using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// Resume a connector (surgewave connect resume)
/// </summary>
public class ResumeConnectorCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Connector name" };

    public ResumeConnectorCommand() : base("resume", "Resume a paused connector")
    {
        Arguments.Add(_nameArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var name = parseResult.GetValue(_nameArg);

        WriteVerbose(parseResult, $"Resuming connector '{name}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            await client.Connect.ResumeConnectorAsync(name, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new { Name = name, Resumed = true }, ConnectJsonOptions.Indented));
            }
            else
            {
                WriteSuccess($"Resumed connector '{name}'");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to resume connector: {ex.Message}");
            return 1;
        }
    }
}
