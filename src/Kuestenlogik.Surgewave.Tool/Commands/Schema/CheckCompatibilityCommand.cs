using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// Check compatibility of a schema (surgewave schema compatibility check)
/// </summary>
public class CheckCompatibilityCommand : CommandBase
{
    private readonly Argument<string> _subjectArg = new("subject") { Description = "Subject name" };
    private readonly Option<string> _schemaOpt = new("--schema", "-s") { Description = "Schema string" };
    private readonly Option<string?> _fileOpt = new("--file", "-f") { Description = "Read schema from file" };
    private readonly Option<string> _typeOpt = new("--type", "-t") { Description = "Schema type", DefaultValueFactory = _ => "AVRO" };
    private readonly Option<int?> _versionOpt = new("--version", "-v") { Description = "Check against specific version (default: latest)" };

    public CheckCompatibilityCommand() : base("check", "Check schema compatibility")
    {
        Arguments.Add(_subjectArg);
        Options.Add(_schemaOpt);
        Options.Add(_fileOpt);
        Options.Add(_typeOpt);
        Options.Add(_versionOpt);
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
        var version = parseResult.GetValue(_versionOpt);

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

        WriteVerbose(parseResult, $"Checking compatibility for subject '{subject}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var result = await client.Schema.CheckCompatibilityAsync(subject, schemaString, schemaType, version, ct);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    Subject = subject,
                    IsCompatible = result.IsCompatible,
                    Messages = result.Messages
                }, SchemaJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                var compatStr = result.IsCompatible ? "compatible" : "incompatible";
                Console.WriteLine($"{subject}\t{compatStr}");
                foreach (var message in result.Messages)
                    Console.WriteLine($"  {message}");
                if (!result.IsCompatible) return 1;
            }
            else
            {
                if (result.IsCompatible)
                {
                    WriteSuccess("Schema is compatible");
                }
                else
                {
                    WriteError("Schema is NOT compatible");
                    if (result.Messages.Count > 0)
                    {
                        AnsiConsole.MarkupLine("[yellow]Issues:[/]");
                        foreach (var message in result.Messages)
                        {
                            AnsiConsole.MarkupLine($"  - {message}");
                        }
                    }
                    return 1;
                }
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to check compatibility: {ex.Message}");
            return 1;
        }
    }
}
