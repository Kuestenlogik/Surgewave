using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Logs;

/// <summary>
/// Verify log integrity (surgewave logs verify)
/// </summary>
public class VerifyCommand : CommandBase
{
    private readonly Option<string?> _topicOpt = new("--topic", "-t")
    {
        Description = "Topic to verify (default: all topics)"
    };

    private readonly Option<int?> _partitionOpt = new("--partition", "-p")
    {
        Description = "Partition to verify (requires --topic)"
    };

    private readonly Option<int> _maxErrorsOpt = new("--max-errors")
    {
        Description = "Stop after finding this many corrupted batches (0 = no limit)",
        DefaultValueFactory = _ => 0
    };

    private readonly Option<bool> _detailsOpt = new("--details")
    {
        Description = "Show detailed information for each corrupted batch",
        DefaultValueFactory = _ => true
    };

    public VerifyCommand() : base("verify", "Verify log integrity by checking CRC checksums")
    {
        Options.Add(_topicOpt);
        Options.Add(_partitionOpt);
        Options.Add(_maxErrorsOpt);
        Options.Add(_detailsOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var topic = parseResult.GetValue(_topicOpt);
        var partition = parseResult.GetValue(_partitionOpt);
        var maxErrors = parseResult.GetValue(_maxErrorsOpt);
        var includeDetails = parseResult.GetValue(_detailsOpt);

        // Validate: partition requires topic
        if (partition.HasValue && string.IsNullOrEmpty(topic))
        {
            WriteError("--partition requires --topic to be specified");
            return 1;
        }

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            Client.Native.Operations.Cluster.LogVerificationInfo result;

            if (format != OutputFormat.Json && format != OutputFormat.Plain)
            {
                result = await AnsiConsole.Status()
                    .Spinner(Spinner.Known.Dots)
                    .StartAsync("Verifying log integrity...", async ctx =>
                    {
                        return await client.Cluster.VerifyLogIntegrityAsync(
                            topic, partition, maxErrors, includeDetails, ct);
                    });
            }
            else
            {
                result = await client.Cluster.VerifyLogIntegrityAsync(
                    topic, partition, maxErrors, includeDetails, ct);
            }

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    result.BatchesChecked,
                    result.CorruptedBatches,
                    result.BytesChecked,
                    result.CorruptedBytes,
                    result.PartitionsChecked,
                    DurationMs = (long)result.Duration.TotalMilliseconds,
                    result.IsValid,
                    result.TopicsVerified,
                    CorruptedBatchDetails = result.CorruptedBatchDetails.Select(d => new
                    {
                        d.Topic,
                        d.Partition,
                        d.BaseOffset,
                        ExpectedCrc = $"0x{d.ExpectedCrc:X8}",
                        ActualCrc = $"0x{d.ActualCrc:X8}",
                        d.BatchLength
                    })
                };
                Console.WriteLine(JsonSerializer.Serialize(output, LogsJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"Valid: {result.IsValid}");
                Console.WriteLine($"Batches checked: {result.BatchesChecked}");
                Console.WriteLine($"Corrupted batches: {result.CorruptedBatches}");
                Console.WriteLine($"Bytes checked: {result.BytesChecked}");
                Console.WriteLine($"Corrupted bytes: {result.CorruptedBytes}");
                Console.WriteLine($"Partitions checked: {result.PartitionsChecked}");
                Console.WriteLine($"Duration: {result.Duration.TotalMilliseconds}ms");

                if (result.CorruptedBatchDetails.Count > 0)
                {
                    Console.WriteLine();
                    Console.WriteLine("Corrupted batches:");
                    foreach (var detail in result.CorruptedBatchDetails)
                    {
                        Console.WriteLine($"  {detail.Topic}-{detail.Partition} offset={detail.BaseOffset} expected=0x{detail.ExpectedCrc:X8} actual=0x{detail.ActualCrc:X8} size={detail.BatchLength}");
                    }
                }
            }
            else
            {
                // Rich output
                if (result.IsValid)
                {
                    WriteSuccess("Log integrity verified - no corruption detected");
                }
                else
                {
                    WriteError($"Corruption detected: {result.CorruptedBatches} corrupted batch(es) found");
                }

                AnsiConsole.WriteLine();

                // Summary grid
                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();

                grid.AddRow("[bold]Batches checked:[/]", result.BatchesChecked.ToString("N0"));
                grid.AddRow("[bold]Corrupted batches:[/]", result.CorruptedBatches > 0
                    ? $"[red]{result.CorruptedBatches:N0}[/]"
                    : "[green]0[/]");
                grid.AddRow("[bold]Bytes checked:[/]", FormatBytes(result.BytesChecked));
                grid.AddRow("[bold]Corrupted bytes:[/]", result.CorruptedBytes > 0
                    ? $"[red]{FormatBytes(result.CorruptedBytes)}[/]"
                    : "[green]0 B[/]");
                grid.AddRow("[bold]Partitions checked:[/]", result.PartitionsChecked.ToString());
                grid.AddRow("[bold]Duration:[/]", $"{result.Duration.TotalMilliseconds:F0} ms");

                AnsiConsole.Write(grid);

                // Topics verified
                if (result.TopicsVerified.Count > 0)
                {
                    AnsiConsole.WriteLine();
                    AnsiConsole.MarkupLine($"[dim]Topics verified: {string.Join(", ", result.TopicsVerified)}[/]");
                }

                // Corrupted batch details
                if (result.CorruptedBatchDetails.Count > 0 && includeDetails)
                {
                    AnsiConsole.WriteLine();
                    AnsiConsole.Write(new Rule("[bold red]Corrupted Batches[/]").LeftJustified());
                    AnsiConsole.WriteLine();

                    var table = new Table();
                    table.AddColumn("Topic");
                    table.AddColumn("Partition");
                    table.AddColumn("Offset");
                    table.AddColumn("Expected CRC");
                    table.AddColumn("Actual CRC");
                    table.AddColumn("Size");

                    foreach (var detail in result.CorruptedBatchDetails)
                    {
                        table.AddRow(
                            $"[cyan]{detail.Topic}[/]",
                            detail.Partition.ToString(),
                            detail.BaseOffset.ToString("N0"),
                            $"[yellow]0x{detail.ExpectedCrc:X8}[/]",
                            $"[red]0x{detail.ActualCrc:X8}[/]",
                            FormatBytes(detail.BatchLength));
                    }

                    AnsiConsole.Write(table);
                }
            }

            return result.IsValid ? 0 : 1;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to verify log integrity: {ex.Message}");
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
