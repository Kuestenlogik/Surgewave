using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Protocol.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// Set compatibility config (surgewave schema compatibility set)
/// </summary>
public class SetCompatibilityCommand : CommandBase
{
    private readonly Argument<string> _compatibilityArg = new("compatibility") { Description = "Compatibility mode (NONE, BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE)" };
    private readonly Option<string?> _subjectOpt = new("--subject", "-s") { Description = "Subject name (omit for global)" };

    public SetCompatibilityCommand() : base("set", "Set compatibility configuration")
    {
        Arguments.Add(_compatibilityArg);
        Options.Add(_subjectOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var compatibility = parseResult.GetValue(_compatibilityArg);
        var subject = parseResult.GetValue(_subjectOpt);

        WriteVerbose(parseResult, $"Setting compatibility to {compatibility}...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var errorCode = await client.Schema.SetCompatibilityConfigAsync(compatibility, subject, ct);
            if (errorCode != SurgewaveErrorCode.None)
            {
                WriteError($"Failed to set compatibility: {errorCode}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    Subject = subject ?? "(global)",
                    Compatibility = compatibility
                }, SchemaJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"{subject ?? "(global)"} compatibility={compatibility}");
            }
            else
            {
                var scope = string.IsNullOrEmpty(subject) ? "global" : $"subject '{subject}'";
                WriteSuccess($"Set compatibility for {scope} to {compatibility}");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to set compatibility config: {ex.Message}");
            return 1;
        }
    }
}
