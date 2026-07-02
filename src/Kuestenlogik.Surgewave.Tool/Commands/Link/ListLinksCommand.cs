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
            using var http = BrokerAdminHttp.Create(host);
            var response = await http.GetAsync("/api/cluster-links", ct);
            var json = await response.Content.ReadAsStringAsync(ct);

            if (!response.IsSuccessStatusCode)
            {
                WriteError($"Failed to list cluster links: {response.StatusCode} — {LinkApi.ExtractErrorMessage(json)}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(json);
                return 0;
            }

            using var doc = JsonDocument.Parse(json);
            var links = doc.RootElement.GetProperty("links");

            if (format == OutputFormat.Plain)
            {
                foreach (var link in links.EnumerateArray())
                {
                    Console.WriteLine(
                        $"{LinkApi.GetString(link, "linkId")}\t{LinkApi.GetString(link, "state")}\t" +
                        $"{LinkApi.GetString(link, "remoteClusterId")}\t{LinkApi.GetInt64(link, "mirroredTopicCount")}\t" +
                        $"{LinkApi.GetInt64(link, "totalLag")}\t{LinkApi.GetString(link, "lastFetch")}");
                }
                return 0;
            }

            if (links.GetArrayLength() == 0)
            {
                AnsiConsole.MarkupLine("[dim]No cluster links configured.[/]");
                return 0;
            }

            var table = new Table();
            table.AddColumn("Link ID");
            table.AddColumn("State");
            table.AddColumn("Remote Cluster");
            table.AddColumn("Mirror Topics");
            table.AddColumn("Total Lag");
            table.AddColumn("Last Fetch");

            foreach (var link in links.EnumerateArray())
            {
                var state = LinkApi.GetString(link, "state") ?? "unknown";
                table.AddRow(
                    Markup.Escape(LinkApi.GetString(link, "linkId") ?? ""),
                    $"[{LinkApi.StateColor(state)}]{Markup.Escape(state)}[/]",
                    Markup.Escape(LinkApi.GetString(link, "remoteClusterId") ?? "unknown"),
                    LinkApi.GetInt64(link, "mirroredTopicCount").ToString(),
                    LinkApi.GetInt64(link, "totalLag").ToString(),
                    Markup.Escape(LinkApi.GetString(link, "lastFetch") ?? "never"));
            }

            AnsiConsole.Write(table);
            return 0;
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }
    }
}
