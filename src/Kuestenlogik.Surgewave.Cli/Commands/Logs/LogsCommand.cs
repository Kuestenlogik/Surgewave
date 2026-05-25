using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Logs;

internal static class LogsJsonOptions
{
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}

/// <summary>
/// Command for log operations (surgewave logs ...)
/// </summary>
public class LogsCommand : CommandBase
{
    public LogsCommand() : base("logs", "Log segment and compaction operations")
    {
        Subcommands.Add(new CompactionStatusCommand());
        Subcommands.Add(new CompactCommand());
        Subcommands.Add(new VerifyCommand());
        Subcommands.Add(new AnalyzeCommand());
    }
}

/// <summary>
/// Show compaction status (surgewave logs compaction-status)
/// </summary>
public class CompactionStatusCommand : CommandBase
{
    public CompactionStatusCommand() : base("compaction-status", "Show compaction status for compactable topics")
    {
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var topics = await client.Cluster.GetCompactionStatusAsync(ct);

            if (format == OutputFormat.Json)
            {
                var output = topics.Select(t => new
                {
                    t.Topic,
                    t.PartitionCount,
                    t.CleanupPolicy,
                    t.SegmentCount,
                    t.TotalBytes
                });
                Console.WriteLine(JsonSerializer.Serialize(output, LogsJsonOptions.Indented));
            }
            else
            {
                if (topics.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No compactable topics found.[/]");
                    AnsiConsole.MarkupLine("[dim]Create a topic with cleanup.policy=compact to enable compaction.[/]");
                    return 0;
                }

                AnsiConsole.Write(new Rule("[bold blue]Compactable Topics[/]").LeftJustified());
                AnsiConsole.WriteLine();

                var table = new Table();
                table.AddColumn("Topic");
                table.AddColumn("Partitions");
                table.AddColumn("Cleanup Policy");
                table.AddColumn("Segments");
                table.AddColumn("Size");

                foreach (var topic in topics.OrderBy(t => t.Topic))
                {
                    var sizeStr = FormatBytes(topic.TotalBytes);
                    table.AddRow(
                        $"[cyan]{topic.Topic}[/]",
                        topic.PartitionCount.ToString(),
                        $"[green]{topic.CleanupPolicy}[/]",
                        topic.SegmentCount.ToString(),
                        sizeStr);
                }

                AnsiConsole.Write(table);
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine($"[dim]Total compactable topics: {topics.Count}[/]");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to get compaction status: {ex.Message}");
            return 1;
        }
    }

    private static string FormatBytes(long bytes)
    {
        string[] sizes = ["B", "KB", "MB", "GB", "TB"];
        int order = 0;
        double size = bytes;
        while (size >= 1024 && order < sizes.Length - 1)
        {
            order++;
            size /= 1024;
        }
        return $"{size:0.##} {sizes[order]}";
    }
}

/// <summary>
/// Trigger log compaction (surgewave logs compact)
/// </summary>
public class CompactCommand : CommandBase
{
    private readonly Option<bool> _forceOpt = new("--force", "-f") { Description = "Run compaction without confirmation", DefaultValueFactory = _ => false };

    public CompactCommand() : base("compact", "Trigger log compaction on compactable topics")
    {
        Options.Add(_forceOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var force = parseResult.GetValue(_forceOpt);

        if (!force && format != OutputFormat.Json && format != OutputFormat.Plain)
        {
            var confirm = AnsiConsole.Confirm("Trigger log compaction on all compactable topics?", false);
            if (!confirm)
            {
                AnsiConsole.MarkupLine("[dim]Cancelled[/]");
                return 0;
            }
        }

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            AnsiConsole.Status()
                .Spinner(Spinner.Known.Dots)
                .Start("Running compaction...", ctx =>
                {
                    // Can't use async in Status, so we'll show result after
                });

            var result = await client.Cluster.TriggerLogCompactionAsync(ct);

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    result.Success,
                    result.RecordsRemoved,
                    result.BytesRemoved,
                    result.SegmentsCompacted
                };
                Console.WriteLine(JsonSerializer.Serialize(output, LogsJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"Success: {result.Success}");
                Console.WriteLine($"Records removed: {result.RecordsRemoved}");
                Console.WriteLine($"Bytes removed: {result.BytesRemoved}");
                Console.WriteLine($"Segments compacted: {result.SegmentsCompacted}");
            }
            else
            {
                if (result.RecordsRemoved > 0)
                {
                    WriteSuccess("Compaction completed");
                    AnsiConsole.WriteLine();

                    var grid = new Grid();
                    grid.AddColumn();
                    grid.AddColumn();

                    grid.AddRow("[bold]Records removed:[/]", result.RecordsRemoved.ToString("N0"));
                    grid.AddRow("[bold]Bytes removed:[/]", FormatBytes(result.BytesRemoved));
                    grid.AddRow("[bold]Segments compacted:[/]", result.SegmentsCompacted.ToString());

                    AnsiConsole.Write(grid);
                }
                else
                {
                    AnsiConsole.MarkupLine("[dim]No records were compacted. Topics may already be clean or have no duplicates.[/]");
                }
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to run compaction: {ex.Message}");
            return 1;
        }
    }

    private static string FormatBytes(long bytes)
    {
        string[] sizes = ["B", "KB", "MB", "GB", "TB"];
        int order = 0;
        double size = bytes;
        while (size >= 1024 && order < sizes.Length - 1)
        {
            order++;
            size /= 1024;
        }
        return $"{size:0.##} {sizes[order]}";
    }
}
