using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// Register a new schema (surgewave schema register)
/// </summary>
public class RegisterSchemaCommand : CommandBase
{
    private readonly Argument<string> _subjectArg = new("subject") { Description = "Subject name" };
    private readonly Option<string> _schemaOpt = new("--schema", "-s") { Description = "Schema string (JSON)" };
    private readonly Option<string?> _fileOpt = new("--file", "-f") { Description = "Read schema from file" };
    private readonly Option<string> _typeOpt = new("--type", "-t") { Description = "Schema type (AVRO, JSON, PROTOBUF, FLATBUFFERS)", DefaultValueFactory = _ => "AVRO" };

    public RegisterSchemaCommand() : base("register", "Register a new schema version")
    {
        Arguments.Add(_subjectArg);
        Options.Add(_schemaOpt);
        Options.Add(_fileOpt);
        Options.Add(_typeOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var subject = parseResult.GetValue(_subjectArg);
        var schemaString = parseResult.GetValue(_schemaOpt);
        var schemaFile = parseResult.GetValue(_fileOpt);
        var schemaType = parseResult.GetValue(_typeOpt)!;

        // Read schema from file if specified
        if (!string.IsNullOrEmpty(schemaFile))
        {
            if (!File.Exists(schemaFile))
            {
                WriteError($"Schema file not found: {schemaFile}");
                return 1;
            }
            schemaString = await File.ReadAllTextAsync(schemaFile, ct);
        }

        if (string.IsNullOrEmpty(schemaString))
        {
            WriteError("Schema string is required. Use --schema or --file");
            return 1;
        }

        WriteVerbose(parseResult, $"Registering schema for subject '{subject}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var result = await client.Schema.RegisterSchemaAsync(subject, schemaString, schemaType, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    Id = result.SchemaId,
                    result.Version,
                    Subject = subject,
                    SchemaType = schemaType
                }, SchemaJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"registered {subject} id={result.SchemaId} version={result.Version}");
            }
            else
            {
                WriteSuccess($"Schema registered successfully");
                AnsiConsole.MarkupLine($"  [bold]Subject:[/] {subject}");
                AnsiConsole.MarkupLine($"  [bold]ID:[/] {result.SchemaId}");
                AnsiConsole.MarkupLine($"  [bold]Version:[/] {result.Version}");
                AnsiConsole.MarkupLine($"  [bold]Type:[/] {schemaType}");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to register schema: {ex.Message}");
            return 1;
        }
    }
}
