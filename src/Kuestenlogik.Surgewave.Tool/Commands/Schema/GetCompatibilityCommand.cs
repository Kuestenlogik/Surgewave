using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// Get compatibility config (surgewave schema compatibility get)
/// </summary>
public class GetCompatibilityCommand : CommandBase
{
    private readonly Option<string?> _subjectOpt = new("--subject", "-s") { Description = "Subject name (omit for global)" };

    public GetCompatibilityCommand() : base("get", "Get compatibility configuration")
    {
        Options.Add(_subjectOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var subject = parseResult.GetValue(_subjectOpt);

        WriteVerbose(parseResult, $"Getting compatibility config...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var compatibility = await client.Schema.GetCompatibilityConfigAsync(subject, ct);
            var isGlobal = string.IsNullOrEmpty(subject);

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    Subject = subject ?? "(global)",
                    Compatibility = compatibility,
                    IsGlobal = isGlobal
                }, SchemaJsonOptions.Indented));
            }
            else
            {
                var scope = isGlobal ? "[dim](global default)[/]" : $"[bold]{subject}[/]";
                AnsiConsole.MarkupLine($"Compatibility for {scope}: [cyan]{compatibility}[/]");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get compatibility config: {ex.Message}");
            return 1;
        }
    }
}
