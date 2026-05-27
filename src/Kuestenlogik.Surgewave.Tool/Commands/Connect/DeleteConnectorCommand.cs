using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// Delete a connector (surgewave connect delete)
/// </summary>
public class DeleteConnectorCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Connector name" };
    private readonly Option<bool> _yesOpt = new("--yes", "-y") { Description = "Skip confirmation prompt" };

    public DeleteConnectorCommand() : base("delete", "Delete a connector")
    {
        Arguments.Add(_nameArg);
        Options.Add(_yesOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var name = parseResult.GetValue(_nameArg);
        var localYes = parseResult.GetValue(_yesOpt);

        if (format == OutputFormat.Table &&
            !ConfirmDestructive(parseResult, $"Delete connector '{name}'?", localYes))
        {
            WriteWarning("Delete cancelled.");
            return 0;
        }

        WriteVerbose(parseResult, $"Deleting connector '{name}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            await client.Connect.DeleteConnectorAsync(name, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new { Name = name, Deleted = true }, ConnectJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"deleted {name}");
            }
            else
            {
                WriteSuccess($"Deleted connector '{name}'");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to delete connector: {ex.Message}");
            return 1;
        }
    }
}
