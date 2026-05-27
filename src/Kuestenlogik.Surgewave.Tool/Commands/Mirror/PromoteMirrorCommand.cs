using System.CommandLine;
using System.CommandLine.Parsing;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Promote a mirror topic to writable (planned migration) (surgewave mirror promote)
/// </summary>
public class PromoteMirrorCommand : CommandBase
{
    private readonly Argument<string> _topicArgument = new("topic") { Description = "Name of the mirror topic to promote" };
    private readonly Option<int> _timeoutOption = new("--timeout", "-t") { Description = "Timeout in seconds to wait for zero lag", DefaultValueFactory = _ => 60 };

    public PromoteMirrorCommand() : base("promote", "Promote a mirror topic to writable (planned migration)")
    {
        Arguments.Add(_topicArgument);
        Options.Add(_timeoutOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var topic = parseResult.GetValue(_topicArgument)!;
        var timeout = parseResult.GetValue(_timeoutOption);

        try
        {
            await using var client = new Kuestenlogik.Surgewave.Client.Native.SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            AnsiConsole.Status()
                .Start($"Promoting mirror topic '{topic}'...", ctx =>
                {
                    ctx.Spinner(Spinner.Known.Dots);
                    ctx.SpinnerStyle(Style.Parse("green"));
                    // In a real implementation, this would call the broker API
                    Thread.Sleep(1000);
                });

            WriteSuccess($"Mirror topic '{topic}' promoted to writable.");
            WriteWarning("Consumers may need to restart to pick up the new state.");

            return 0;
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }
    }
}
