using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Protocol.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// Get a schema by ID or version (surgewave schema get)
/// </summary>
public class GetSchemaCommand : CommandBase
{
    private readonly Option<int?> _idOpt = new("--id", "-i") { Description = "Get schema by global ID" };
    private readonly Option<string?> _subjectOpt = new("--subject", "-s") { Description = "Subject name (for version lookup)" };
    private readonly Option<int?> _versionOpt = new("--version", "-v") { Description = "Version number (use with --subject, 'latest' = -1)" };

    public GetSchemaCommand() : base("get", "Get a schema by ID or subject/version")
    {
        Options.Add(_idOpt);
        Options.Add(_subjectOpt);
        Options.Add(_versionOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var schemaId = parseResult.GetValue(_idOpt);
        var subject = parseResult.GetValue(_subjectOpt);
        var version = parseResult.GetValue(_versionOpt);

        if (schemaId == null && subject == null)
        {
            WriteError("Specify either --id or --subject");
            return 1;
        }

        WriteVerbose(parseResult, $"Connecting to {host}:{port}...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            SchemaInfo? schema;
            if (schemaId != null)
            {
                schema = await client.Schema.GetSchemaByIdAsync(schemaId.Value, ct);
            }
            else
            {
                schema = await client.Schema.GetSchemaByVersionAsync(subject!, version ?? -1, ct);
            }

            if (schema == null)
            {
                WriteError("Schema not found");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    schema.Id,
                    schema.Subject,
                    schema.Version,
                    schema.SchemaType,
                    schema.SchemaString
                }, SchemaJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine(schema.SchemaString);
            }
            else
            {
                AnsiConsole.MarkupLine($"[bold]ID:[/] {schema.Id}");
                AnsiConsole.MarkupLine($"[bold]Subject:[/] {schema.Subject}");
                AnsiConsole.MarkupLine($"[bold]Version:[/] {schema.Version}");
                AnsiConsole.MarkupLine($"[bold]Type:[/] {schema.SchemaType}");
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine("[bold]Schema:[/]");

                try
                {
                    // Try to format as JSON for better readability
                    using var doc = JsonDocument.Parse(schema.SchemaString);
                    var formatted = JsonSerializer.Serialize(doc.RootElement, SchemaJsonOptions.Indented);
                    AnsiConsole.WriteLine(formatted);
                }
                catch
                {
                    AnsiConsole.WriteLine(schema.SchemaString);
                }
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get schema: {ex.Message}");
            return 1;
        }
    }
}
