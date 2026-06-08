using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Link;

/// <summary>
/// List all cluster links (surgewave link list)
/// </summary>
public class ListLinksCommand : CommandBase
{
    public ListLinksCommand() : base("list", "List all cluster links")
    {
        Aliases.Add("ls");
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new Kuestenlogik.Surgewave.Client.Native.SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            // Placeholder - will be wired to actual API
            var links = Array.Empty<object>();

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(links, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                // Placeholder: links is currently Array.Empty&lt;object&gt;.
                // When wired to the real API, replace with one
                // Console.WriteLine($"{link.Id}\t{link.Remote}\t..." per row.
            }
            else
            {
                var table = new Table();
                table.AddColumn("Link ID");
                table.AddColumn("Remote");
                table.AddColumn("State");
                table.AddColumn("Mirror Topics");
                table.AddColumn("Total Lag");

                if (links.Length == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No cluster links configured.[/]");
                }
                else
                {
                    AnsiConsole.Write(table);
                }
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
