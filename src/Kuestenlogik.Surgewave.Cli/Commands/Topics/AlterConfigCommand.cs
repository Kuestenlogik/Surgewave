using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Topics;

/// <summary>
/// Alter topic configuration (surgewave topics alter-config)
/// Usage: surgewave topics alter-config &lt;topic&gt; --set key=value --set key=value
/// </summary>
public class AlterConfigCommand : CommandBase
{
    private readonly Argument<string> _topicArg = new("topic") { Description = "Name of the topic to configure" };
    private readonly Option<string[]> _setOpt = new("--set", "-s")
    {
        Description = "Configuration to set (key=value)",
        AllowMultipleArgumentsPerToken = true
    };

    public AlterConfigCommand() : base("alter-config", "Alter topic configuration")
    {
        Arguments.Add(_topicArg);
        Options.Add(_setOpt);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topic = parseResult.GetValue(_topicArg);
        var sets = parseResult.GetValue(_setOpt) ?? [];

        if (sets.Length == 0)
        {
            WriteError("No configuration changes specified. Use --set key=value");
            return 1;
        }

        var config = new Dictionary<string, string>();
        foreach (var set in sets)
        {
            var parts = set.Split('=', 2);
            if (parts.Length != 2)
            {
                WriteError($"Invalid config format: '{set}'. Expected key=value");
                return 1;
            }
            config[parts[0].Trim()] = parts[1].Trim();
        }

        WriteVerbose(parseResult, $"Updating config for topic '{topic}'...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);
            await client.Topics.AlterConfigAsync(topic, config, ct);

            WriteSuccess($"Updated configuration for topic '{topic}':");
            foreach (var (key, value) in config)
            {
                AnsiConsole.MarkupLine($"  [dim]{key}[/] = {value}");
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to alter config: {ex.Message}");
            return 1;
        }
    }
}
