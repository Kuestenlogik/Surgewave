using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// Delete a specific version (surgewave schema delete-version)
/// </summary>
public class DeleteVersionCommand : CommandBase
{
    private readonly Argument<string> _subjectArg = new("subject") { Description = "Subject name" };
    private readonly Argument<int> _versionArg = new("version") { Description = "Version number to delete" };
    private readonly Option<bool> _permanentOpt = new("--permanent", "-p") { Description = "Permanently delete (hard delete)" };
    private readonly Option<bool> _forceOpt = new("--force", "-f") { Description = "Skip confirmation prompt" };

    public DeleteVersionCommand() : base("delete-version", "Delete a specific schema version")
    {
        Arguments.Add(_subjectArg);
        Arguments.Add(_versionArg);
        Options.Add(_permanentOpt);
        Options.Add(_forceOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var subject = parseResult.GetValue(_subjectArg);
        var version = parseResult.GetValue(_versionArg);
        var permanent = parseResult.GetValue(_permanentOpt);
        var force = parseResult.GetValue(_forceOpt);

        if (!force && format == OutputFormat.Table)
        {
            if (!AnsiConsole.Confirm($"Delete version {version} of subject '{subject}'?", false))
            {
                AnsiConsole.MarkupLine("[dim]Cancelled[/]");
                return 0;
            }
        }

        WriteVerbose(parseResult, $"Deleting version {version} of subject '{subject}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var result = await client.Schema.DeleteSubjectAsync(subject, permanent, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new { Subject = subject, Version = version }, SchemaJsonOptions.Indented));
            }
            else
            {
                WriteSuccess($"Deleted version {version} of subject '{subject}'");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to delete version: {ex.Message}");
            return 1;
        }
    }
}
