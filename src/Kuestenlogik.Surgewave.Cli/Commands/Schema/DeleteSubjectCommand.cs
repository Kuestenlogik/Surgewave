using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// Delete a subject (surgewave schema delete-subject)
/// </summary>
public class DeleteSubjectCommand : CommandBase
{
    private readonly Argument<string> _subjectArg = new("subject") { Description = "Subject name" };
    private readonly Option<bool> _permanentOpt = new("--permanent", "-p") { Description = "Permanently delete (hard delete)" };
    private readonly Option<bool> _yesOpt = new("--yes", "-y") { Description = "Skip confirmation prompt" };

    public DeleteSubjectCommand() : base("delete-subject", "Delete a subject and all its versions")
    {
        Arguments.Add(_subjectArg);
        Options.Add(_permanentOpt);
        Options.Add(_yesOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var subject = parseResult.GetValue(_subjectArg);
        var permanent = parseResult.GetValue(_permanentOpt);
        var localYes = parseResult.GetValue(_yesOpt);

        if (format == OutputFormat.Table)
        {
            var message = permanent
                ? $"[red]Permanently[/] delete subject '{subject}' and all its versions?"
                : $"Delete subject '{subject}'? (can be restored)";
            if (!ConfirmDestructive(parseResult, message, localYes))
            {
                WriteWarning("Delete cancelled.");
                return 0;
            }
        }

        WriteVerbose(parseResult, $"Deleting subject '{subject}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var deletedVersions = await client.Schema.DeleteSubjectAsync(subject, permanent, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    Subject = subject,
                    DeletedVersions = deletedVersions,
                    Permanent = permanent
                }, SchemaJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"deleted {subject} versions={deletedVersions.Count}");
            }
            else
            {
                WriteSuccess($"Deleted subject '{subject}' with {deletedVersions.Count} version(s)");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to delete subject: {ex.Message}");
            return 1;
        }
    }
}
