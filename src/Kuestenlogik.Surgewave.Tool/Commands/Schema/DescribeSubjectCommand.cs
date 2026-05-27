using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// Describe a subject and its versions (surgewave schema describe)
/// </summary>
public class DescribeSubjectCommand : CommandBase
{
    private readonly Argument<string> _subjectArg = new("subject") { Description = "Subject name" };
    private readonly Option<bool> _includeDeletedOpt = new("--include-deleted", "-d") { Description = "Include soft-deleted versions" };

    public DescribeSubjectCommand() : base("describe", "Describe a subject and its versions")
    {
        Aliases.Add("show");
        Arguments.Add(_subjectArg);
        Options.Add(_includeDeletedOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var subject = parseResult.GetValue(_subjectArg);
        var includeDeleted = parseResult.GetValue(_includeDeletedOpt);

        WriteVerbose(parseResult, $"Describing subject '{subject}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var versions = await client.Schema.GetSubjectVersionsAsync(subject, ct);

            if (format == OutputFormat.Json)
            {
                var schemas = new List<object>();
                foreach (var version in versions)
                {
                    var schema = await client.Schema.GetSchemaByVersionAsync(subject, version, ct);
                    if (schema != null)
                    {
                        schemas.Add(new
                        {
                            schema.Id,
                            schema.Subject,
                            schema.Version,
                            schema.SchemaType,
                            schema.SchemaString
                        });
                    }
                }
                Console.WriteLine(JsonSerializer.Serialize(new { Subject = subject, Versions = schemas }, SchemaJsonOptions.Indented));
            }
            else
            {
                AnsiConsole.MarkupLine($"[bold]Subject:[/] {subject}");
                AnsiConsole.MarkupLine($"[bold]Versions:[/] {versions.Count}");
                AnsiConsole.WriteLine();

                if (versions.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No versions found[/]");
                    return 0;
                }

                var table = new Table();
                table.AddColumn("Version");
                table.AddColumn("ID");
                table.AddColumn("Schema Type");

                foreach (var version in versions.OrderBy(v => v))
                {
                    var schema = await client.Schema.GetSchemaByVersionAsync(subject, version, ct);
                    if (schema != null)
                    {
                        table.AddRow(
                            version.ToString(),
                            schema.Id.ToString(),
                            schema.SchemaType
                        );
                    }
                }

                AnsiConsole.Write(table);
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to describe subject: {ex.Message}");
            return 1;
        }
    }
}
