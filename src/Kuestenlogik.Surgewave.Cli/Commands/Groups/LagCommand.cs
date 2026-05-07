using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Groups;

/// <summary>
/// Show consumer group lag (surgewave groups lag)
/// </summary>
public class LagCommand : CommandBase
{
    private readonly Argument<string?> _groupArg = new("group")
    {
        Description = "Consumer group ID (optional, shows all if not specified)",
        Arity = ArgumentArity.ZeroOrOne
    };

    private readonly Option<bool> _summaryOption = new("--summary", "-s")
    {
        Description = "Show summary only (no per-partition details)"
    };

    public LagCommand() : base("lag", "Show consumer group lag")
    {
        Arguments.Add(_groupArg);
        Options.Add(_summaryOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var groupId = parseResult.GetValue(_groupArg);
        var summaryOnly = parseResult.GetValue(_summaryOption);
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            if (string.IsNullOrEmpty(groupId))
            {
                // Show lag summary for all groups
                var summary = await client.Groups.GetLagSummaryAsync(ct);
                DisplaySummary(summary, format);
            }
            else
            {
                // Show lag for specific group
                var lag = await client.Groups.GetLagAsync(groupId, ct);
                DisplayGroupLag(lag, summaryOnly, format);
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get lag: {ex.Message}");
            return 1;
        }
    }

    private static void DisplaySummary(Client.Native.Operations.ConsumerGroups.LagSummaryResult summary, OutputFormat format)
    {
        if (format == OutputFormat.Json)
        {
            var output = new
            {
                summary.GroupCount,
                summary.GroupsWithHighLag,
                summary.TotalLag,
                summary.MaxLag,
                summary.MaxLagGroup,
                Groups = summary.Groups.Select(g => new
                {
                    g.GroupId,
                    g.State,
                    g.TotalLag,
                    g.PartitionCount,
                    g.MemberCount
                })
            };
            Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions.Indented));
        }
        else if (format == OutputFormat.Plain)
        {
            Console.WriteLine($"Groups: {summary.GroupCount}");
            Console.WriteLine($"Groups with high lag: {summary.GroupsWithHighLag}");
            Console.WriteLine($"Total lag: {summary.TotalLag}");
            Console.WriteLine($"Max lag: {summary.MaxLag}");
            Console.WriteLine($"Max lag group: {summary.MaxLagGroup ?? "N/A"}");
            Console.WriteLine();
            foreach (var g in summary.Groups)
            {
                Console.WriteLine($"{g.GroupId}: {g.TotalLag} lag ({g.PartitionCount} partitions, {g.MemberCount} members)");
            }
        }
        else
        {
            var summaryPanel = new Panel(new Markup($"""
                [bold]Groups:[/] {summary.GroupCount}
                [bold]Groups with high lag:[/] {(summary.GroupsWithHighLag > 0 ? $"[yellow]{summary.GroupsWithHighLag}[/]" : "0")}
                [bold]Total lag:[/] {FormatLag(summary.TotalLag)}
                [bold]Max lag:[/] {FormatLag(summary.MaxLag)}
                [bold]Max lag group:[/] {summary.MaxLagGroup ?? "[dim]N/A[/]"}
                """))
            {
                Header = new PanelHeader("[cyan]Lag Summary[/]")
            };
            AnsiConsole.Write(summaryPanel);

            if (summary.Groups.Count > 0)
            {
                AnsiConsole.WriteLine();
                var table = new Table();
                table.Title = new TableTitle("[bold]Consumer Groups[/]");
                table.AddColumn("Group ID");
                table.AddColumn("State");
                table.AddColumn(new TableColumn("Lag").RightAligned());
                table.AddColumn(new TableColumn("Partitions").RightAligned());
                table.AddColumn(new TableColumn("Members").RightAligned());

                foreach (var g in summary.Groups.OrderByDescending(g => g.TotalLag))
                {
                    table.AddRow(
                        TruncateText(g.GroupId, 40),
                        GetStateMarkup(g.State),
                        FormatLag(g.TotalLag),
                        g.PartitionCount.ToString(),
                        g.MemberCount.ToString());
                }

                AnsiConsole.Write(table);
            }
        }
    }

    private static void DisplayGroupLag(Client.Native.Operations.ConsumerGroups.ConsumerGroupLag lag, bool summaryOnly, OutputFormat format)
    {
        if (format == OutputFormat.Json)
        {
            var output = new
            {
                lag.GroupId,
                lag.State,
                lag.TotalLag,
                lag.PartitionCount,
                lag.MemberCount,
                Topics = lag.Topics.Select(t => new
                {
                    t.Topic,
                    t.TotalLag,
                    Partitions = summaryOnly ? null : t.Partitions.Select(p => new
                    {
                        p.Partition,
                        p.CommittedOffset,
                        p.HighWatermark,
                        p.Lag,
                        p.LogStartOffset
                    })
                })
            };
            Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions.Indented));
        }
        else if (format == OutputFormat.Plain)
        {
            Console.WriteLine($"Group: {lag.GroupId}");
            Console.WriteLine($"State: {lag.State}");
            Console.WriteLine($"Total lag: {lag.TotalLag}");
            Console.WriteLine($"Partitions: {lag.PartitionCount}");
            Console.WriteLine($"Members: {lag.MemberCount}");
            Console.WriteLine();

            foreach (var topic in lag.Topics)
            {
                Console.WriteLine($"Topic: {topic.Topic} (lag: {topic.TotalLag})");
                if (!summaryOnly)
                {
                    foreach (var p in topic.Partitions)
                    {
                        Console.WriteLine($"  Partition {p.Partition}: offset={p.CommittedOffset}, hwm={p.HighWatermark}, lag={p.Lag}");
                    }
                }
            }
        }
        else
        {
            var panel = new Panel(new Markup($"""
                [bold]State:[/] {GetStateMarkup(lag.State)}
                [bold]Total Lag:[/] {FormatLag(lag.TotalLag)}
                [bold]Partitions:[/] {lag.PartitionCount}
                [bold]Members:[/] {lag.MemberCount}
                """))
            {
                Header = new PanelHeader($"[cyan]{lag.GroupId}[/]")
            };
            AnsiConsole.Write(panel);

            foreach (var topic in lag.Topics)
            {
                AnsiConsole.WriteLine();

                if (summaryOnly)
                {
                    AnsiConsole.MarkupLine($"[bold]{topic.Topic}[/]: {FormatLag(topic.TotalLag)} lag");
                }
                else
                {
                    var table = new Table();
                    table.Title = new TableTitle($"[bold]{topic.Topic}[/] (total lag: {FormatLag(topic.TotalLag)})");
                    table.AddColumn(new TableColumn("Partition").RightAligned());
                    table.AddColumn(new TableColumn("Committed").RightAligned());
                    table.AddColumn(new TableColumn("High Watermark").RightAligned());
                    table.AddColumn(new TableColumn("Lag").RightAligned());

                    foreach (var p in topic.Partitions.OrderBy(p => p.Partition))
                    {
                        table.AddRow(
                            p.Partition.ToString(),
                            p.CommittedOffset.ToString("N0"),
                            p.HighWatermark.ToString("N0"),
                            FormatLag(p.Lag));
                    }

                    AnsiConsole.Write(table);
                }
            }
        }
    }

    private static string FormatLag(long lag) => lag switch
    {
        0 => "[green]0[/]",
        < 1000 => $"[green]{lag:N0}[/]",
        < 10000 => $"[yellow]{lag:N0}[/]",
        _ => $"[red]{lag:N0}[/]"
    };

    private static string GetStateMarkup(string state) => state switch
    {
        "Stable" => "[green]Stable[/]",
        "Empty" => "[dim]Empty[/]",
        "PreparingRebalance" => "[yellow]PreparingRebalance[/]",
        "CompletingRebalance" => "[yellow]CompletingRebalance[/]",
        "Dead" => "[red]Dead[/]",
        _ => state
    };

    private static string TruncateText(string text, int maxLength)
    {
        if (text.Length <= maxLength) return text;
        return text[..(maxLength - 3)] + "...";
    }
}
