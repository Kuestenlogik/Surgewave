using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Groups;

/// <summary>
/// Delete a consumer group (surgewave groups delete)
/// </summary>
public class DeleteGroupCommand : CommandBase
{
    private readonly Argument<string> _groupArg = new("group") { Description = "Consumer group ID to delete" };
    private readonly Option<bool> _yesOpt = new("--yes", "-y") { Description = "Skip confirmation prompt", DefaultValueFactory = _ => false };

    public DeleteGroupCommand() : base("delete", "Delete a consumer group")
    {
        Arguments.Add(_groupArg);
        Options.Add(_yesOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var groupId = parseResult.GetValue(_groupArg);
        var localYes = parseResult.GetValue(_yesOpt);
        var format = GetFormat(parseResult);

        if (format != OutputFormat.Json && format != OutputFormat.Plain &&
            !ConfirmDestructive(parseResult, $"Delete consumer group '[cyan]{groupId}[/]'?", localYes))
        {
            WriteWarning("Delete cancelled.");
            return 0;
        }

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            await client.Groups.DeleteAsync(groupId, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new { deleted = groupId }));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"Deleted: {groupId}");
            }
            else
            {
                WriteSuccess($"Deleted consumer group '{groupId}'");
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to delete group: {ex.Message}");
            return 1;
        }
    }
}
