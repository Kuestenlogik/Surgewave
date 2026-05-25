using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// List all subjects (surgewave schema list)
/// </summary>
public class ListSubjectsCommand : CommandBase
{
    private readonly Option<bool> _includeDeletedOpt = new("--include-deleted", "-d") { Description = "Include soft-deleted subjects" };

    public ListSubjectsCommand() : base("list", "List all subjects")
    {
        Aliases.Add("ls");
        Options.Add(_includeDeletedOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var includeDeleted = parseResult.GetValue(_includeDeletedOpt);

        WriteVerbose(parseResult, $"Connecting to {host}:{port}...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var subjects = await client.Schema.ListAsync(ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(subjects, SchemaJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var subject in subjects)
                {
                    Console.WriteLine(subject);
                }
            }
            else
            {
                if (subjects.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No subjects found[/]");
                    return 0;
                }

                var table = new Table();
                table.AddColumn("Subject");

                foreach (var subject in subjects.OrderBy(s => s))
                {
                    table.AddRow(subject);
                }

                AnsiConsole.Write(table);
                AnsiConsole.MarkupLine($"\n[dim]Total: {subjects.Count} subject(s)[/]");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list subjects: {ex.Message}");
            return 1;
        }
    }
}
