using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Backup;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Backup;

/// <summary>
/// Restore from a backup (surgewave backup restore)
/// </summary>
public class RestoreBackupCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Option<string> _inputOpt = new("--input", "-i")
    {
        Description = "Backup directory to restore from",
        Required = true
    };

    private readonly Option<string?> _dataDirectoryOpt = new("--data-dir", "-d")
    {
        Description = "Target data directory (default: ./data)"
    };

    private readonly Option<string[]?> _topicsOpt = new("--topics", "-t")
    {
        Description = "Specific topics to restore (comma-separated, default: all)"
    };

    private readonly Option<bool> _noVerifyOpt = new("--no-verify")
    {
        Description = "Skip checksum verification during restore",
        DefaultValueFactory = _ => false
    };

    private readonly Option<bool> _overwriteOpt = new("--overwrite")
    {
        Description = "Overwrite existing files",
        DefaultValueFactory = _ => false
    };

    private readonly Option<bool> _forceOpt = new("--force", "-f")
    {
        Description = "Skip confirmation prompt",
        DefaultValueFactory = _ => false
    };

    private readonly Option<long?> _targetTimestampOpt = new("--target-timestamp")
    {
        Description = "Point-in-time restore: skip segments whose latest record timestamp (Unix milliseconds) is greater than this cutoff. Segment-boundary granularity."
    };

    private readonly Option<string[]?> _targetOffsetOpt = new("--target-offset")
    {
        Description = "Point-in-time restore: per-partition offset cutoff in 'topic:partition=offset' form (repeatable). Only segments with BaseOffset <= cutoff are restored."
    };

    public RestoreBackupCommand() : base("restore", "Restore Surgewave data from a backup")
    {
        Options.Add(_inputOpt);
        Options.Add(_dataDirectoryOpt);
        Options.Add(_topicsOpt);
        Options.Add(_noVerifyOpt);
        Options.Add(_overwriteOpt);
        Options.Add(_forceOpt);
        Options.Add(_targetTimestampOpt);
        Options.Add(_targetOffsetOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var format = GetFormat(parseResult);
        var inputPath = parseResult.GetValue(_inputOpt)!;
        var dataDirectory = parseResult.GetValue(_dataDirectoryOpt) ?? "./data";
        var topics = parseResult.GetValue(_topicsOpt);
        var verifyChecksums = !parseResult.GetValue(_noVerifyOpt);
        var overwrite = parseResult.GetValue(_overwriteOpt);
        var force = parseResult.GetValue(_forceOpt);
        var targetTimestamp = parseResult.GetValue(_targetTimestampOpt);
        var targetOffsetSpecs = parseResult.GetValue(_targetOffsetOpt);

        Dictionary<string, long>? targetOffsets = null;
        if (targetOffsetSpecs is { Length: > 0 })
        {
            targetOffsets = new Dictionary<string, long>(StringComparer.Ordinal);
            foreach (var spec in targetOffsetSpecs)
            {
                if (!TryParseTargetOffsetSpec(spec, out var topic, out var partitionId, out var offset))
                {
                    WriteError($"Invalid --target-offset value '{spec}'. Expected format: 'topic:partition=offset'.");
                    return 1;
                }
                targetOffsets[RestoreOptions.PartitionKey(topic, partitionId)] = offset;
            }
        }

        var options = new RestoreOptions
        {
            Topics = topics?.ToList(),
            VerifyChecksums = verifyChecksums,
            Overwrite = overwrite,
            TargetTimestampMs = targetTimestamp,
            TargetOffsetsPerPartition = targetOffsets,
        };

        // Validate backup directory exists
        var manifestPath = Path.Combine(inputPath, BackupManifest.FileName);
        if (!File.Exists(manifestPath))
        {
            WriteError($"Backup manifest not found: {manifestPath}");
            return 1;
        }

        // Load manifest to show info
        var manifest = await BackupManifest.LoadAsync(manifestPath, ct);

        // Confirm restore
        if (!force && format != OutputFormat.Json && format != OutputFormat.Plain)
        {
            WriteLine();
            WriteMarkup("[bold]Backup to restore:[/]");

            var infoGrid = new Grid();
            infoGrid.AddColumn();
            infoGrid.AddColumn();
            infoGrid.AddRow("Backup ID:", manifest.BackupId);
            infoGrid.AddRow("Created:", manifest.CreatedAt.ToString("yyyy-MM-dd HH:mm:ss"));
            infoGrid.AddRow("Topics:", manifest.Topics.Count.ToString());
            infoGrid.AddRow("Size:", FormatBytes(manifest.TotalBytes));
            WriteRenderable(infoGrid);

            WriteLine();

            if (!AnsiConsole.Confirm($"Restore to [cyan]{dataDirectory}[/]?", false))
            {
                WriteMarkup("[dim]Cancelled[/]");
                return 0;
            }
        }

        try
        {
            var restoreService = new RestoreService();
            RestoreResult result;

            if (format != OutputFormat.Json && format != OutputFormat.Plain)
            {
                result = await AnsiConsole.Progress()
                    .Columns(
                        new TaskDescriptionColumn(),
                        new ProgressBarColumn(),
                        new PercentageColumn(),
                        new SpinnerColumn())
                    .StartAsync(async ctx =>
                    {
                        var task = ctx.AddTask("Restoring backup...", maxValue: 100);

                        restoreService.ProgressChanged += (_, e) =>
                        {
                            if (e.Progress.TotalTopics > 0)
                            {
                                task.Value = (double)e.Progress.CompletedTopics / e.Progress.TotalTopics * 100;
                                task.Description = $"Restoring {e.Progress.CurrentTopic}...";
                            }
                        };

                        var res = await restoreService.RestoreAsync(
                            inputPath,
                            dataDirectory,
                            options,
                            ct);

                        task.Value = 100;
                        task.Description = res.Success ? "Restore complete" : "Restore completed with errors";

                        return res;
                    });
            }
            else
            {
                result = await restoreService.RestoreAsync(
                    inputPath,
                    dataDirectory,
                    options,
                    ct);
            }

            // Output results
            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    result.Success,
                    result.BackupId,
                    BackupCreatedAt = result.BackupCreatedAt?.ToString("O"),
                    result.TopicsRestored,
                    result.FilesRestored,
                    result.BytesRestored,
                    result.SegmentsSkipped,
                    result.ErrorMessage,
                    VerificationErrors = result.VerificationErrors
                };
                System.Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions));
            }
            else if (format == OutputFormat.Plain)
            {
                System.Console.WriteLine($"Success: {result.Success}");
                System.Console.WriteLine($"Backup ID: {result.BackupId}");
                System.Console.WriteLine($"Topics restored: {result.TopicsRestored}");
                System.Console.WriteLine($"Files restored: {result.FilesRestored}");
                System.Console.WriteLine($"Bytes restored: {result.BytesRestored}");
                if (!result.Success)
                {
                    System.Console.WriteLine($"Error: {result.ErrorMessage}");
                }
            }
            else
            {
                if (result.Success)
                {
                    WriteSuccess("Restore completed successfully");
                }
                else
                {
                    WriteError($"Restore completed with errors: {result.ErrorMessage}");
                }

                WriteLine();

                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();

                grid.AddRow("[bold]Backup ID:[/]", result.BackupId ?? "Unknown");
                grid.AddRow("[bold]Backup Created:[/]", result.BackupCreatedAt?.ToString("yyyy-MM-dd HH:mm:ss") ?? "Unknown");
                grid.AddRow("[bold]Topics restored:[/]", result.TopicsRestored.ToString());
                grid.AddRow("[bold]Files restored:[/]", result.FilesRestored.ToString("N0"));
                grid.AddRow("[bold]Bytes restored:[/]", FormatBytes(result.BytesRestored));
                if (result.SegmentsSkipped > 0)
                {
                    grid.AddRow("[bold]Segments skipped (PIT):[/]", result.SegmentsSkipped.ToString("N0"));
                }
                grid.AddRow("[bold]Target:[/]", $"[cyan]{dataDirectory}[/]");

                WriteRenderable(grid);

                if (result.VerificationErrors.Count > 0)
                {
                    WriteLine();
                    WriteMarkup("[bold red]Verification errors:[/]");
                    foreach (var error in result.VerificationErrors.Take(10))
                    {
                        WriteMarkup($"  [red]{error}[/]");
                    }
                    if (result.VerificationErrors.Count > 10)
                    {
                        WriteMarkup($"  [dim]... and {result.VerificationErrors.Count - 10} more[/]");
                    }
                }
            }

            return result.Success ? 0 : 1;
        }
        catch (Exception ex)
        {
            WriteError($"Restore failed: {ex.Message}");
            return 1;
        }
    }

    /// <summary>
    /// Parse a per-partition offset cutoff in the form
    /// <c>topic:partition=offset</c>. Topic names may contain dots and
    /// underscores; we split on the LAST <c>:</c> before <c>=</c> so that
    /// topics with embedded colons aren't supported (Kafka topic-name rules
    /// do not allow them anyway).
    /// </summary>
    private static bool TryParseTargetOffsetSpec(
        string spec,
        out string topic,
        out int partitionId,
        out long offset)
    {
        topic = string.Empty;
        partitionId = -1;
        offset = -1;

        var eq = spec.LastIndexOf('=');
        if (eq <= 0 || eq == spec.Length - 1) return false;
        var colon = spec.LastIndexOf(':', eq - 1);
        if (colon <= 0) return false;

        topic = spec[..colon];
        if (!int.TryParse(spec.AsSpan(colon + 1, eq - colon - 1), out partitionId)) return false;
        if (partitionId < 0) return false;
        if (!long.TryParse(spec.AsSpan(eq + 1), out offset)) return false;
        if (offset < 0) return false;
        return true;
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
