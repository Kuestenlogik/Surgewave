using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Link;

/// <summary>
/// Describe a cluster link (surgewave link describe)
/// </summary>
public class DescribeLinkCommand : CommandBase
{
    private readonly Option<string> _linkIdOption = new("--link-id", "-l") { Description = "Link ID to describe", Required = true };

    public DescribeLinkCommand() : base("describe", "Show details of a cluster link")
    {
        Options.Add(_linkIdOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var linkId = parseResult.GetValue(_linkIdOption)!;
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new Kuestenlogik.Surgewave.Client.Native.SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            // Placeholder
            var status = new
            {
                LinkId = linkId,
                State = "ACTIVE",
                Remote = "unknown",
                MirroredTopics = 0,
                TotalLag = 0L
            };

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(status, JsonOptions.Indented));
            }
            else
            {
                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();
                grid.AddRow("[bold]Link ID:[/]", status.LinkId);
                grid.AddRow("[bold]State:[/]", $"[green]{status.State}[/]");
                grid.AddRow("[bold]Remote:[/]", status.Remote);
                grid.AddRow("[bold]Mirrored Topics:[/]", status.MirroredTopics.ToString());
                grid.AddRow("[bold]Total Lag:[/]", $"{status.TotalLag} messages");
                AnsiConsole.Write(grid);
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }
    }
}
