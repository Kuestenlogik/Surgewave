using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Backup;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Backup;

/// <summary>
/// Create a backup (surgewave backup create)
/// </summary>
public class CreateBackupCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Option<string> _outputOpt = new("--output", "-o")
    {
        Description = "Output directory for the backup",
        Required = true
    };

    private readonly Option<string?> _dataDirectoryOpt = new("--data-dir", "-d")
    {
        Description = "Source data directory (default: ./data)"
    };

    private readonly Option<string[]?> _topicsOpt = new("--topics", "-t")
    {
        Description = "Specific topics to backup (comma-separated, default: all)"
    };

    private readonly Option<bool> _noChecksumsOpt = new("--no-checksums")
    {
        Description = "Skip computing SHA256 checksums",
        DefaultValueFactory = _ => false
    };

    private readonly Option<bool> _noMetadataOpt = new("--no-metadata")
    {
        Description = "Skip backing up metadata files",
        DefaultValueFactory = _ => false
    };

    private readonly Option<string?> _descriptionOpt = new("--description")
    {
        Description = "Optional description for this backup"
    };

    public CreateBackupCommand() : base("create", "Create a backup of Surgewave data")
    {
        Options.Add(_outputOpt);
        Options.Add(_dataDirectoryOpt);
        Options.Add(_topicsOpt);
        Options.Add(_noChecksumsOpt);
        Options.Add(_noMetadataOpt);
        Options.Add(_descriptionOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var format = GetFormat(parseResult);
        var outputPath = parseResult.GetValue(_outputOpt)!;
        var dataDirectory = parseResult.GetValue(_dataDirectoryOpt) ?? "./data";
        var topics = parseResult.GetValue(_topicsOpt);
        var computeChecksums = !parseResult.GetValue(_noChecksumsOpt);
        var includeMetadata = !parseResult.GetValue(_noMetadataOpt);
        var description = parseResult.GetValue(_descriptionOpt);

        // Validate data directory exists
        if (!Directory.Exists(dataDirectory))
        {
            WriteError($"Data directory not found: {dataDirectory}");
            return 1;
        }

        // Check if output already exists
        if (Directory.Exists(outputPath) && Directory.GetFileSystemEntries(outputPath).Length > 0)
        {
            WriteError($"Output directory already exists and is not empty: {outputPath}");
            return 1;
        }

        try
        {
            var backupService = new BackupService(dataDirectory);
            BackupManifest manifest;

            if (format != OutputFormat.Json && format != OutputFormat.Plain)
            {
                manifest = await AnsiConsole.Progress()
                    .Columns(
                        new TaskDescriptionColumn(),
                        new ProgressBarColumn(),
                        new PercentageColumn(),
                        new SpinnerColumn())
                    .StartAsync(async ctx =>
                    {
                        var task = ctx.AddTask("Creating backup...", maxValue: 100);

                        backupService.ProgressChanged += (_, e) =>
                        {
                            if (e.Progress.TotalTopics > 0)
                            {
                                task.Value = (double)e.Progress.CompletedTopics / e.Progress.TotalTopics * 100;
                                task.Description = $"Backing up {e.Progress.CurrentTopic}...";
                            }
                        };

                        var result = await backupService.CreateBackupAsync(
                            outputPath,
                            topics?.ToList(),
                            includeMetadata,
                            computeChecksums,
                            ct);

                        task.Value = 100;
                        task.Description = "Backup complete";

                        return result;
                    });
            }
            else
            {
                manifest = await backupService.CreateBackupAsync(
                    outputPath,
                    topics?.ToList(),
                    includeMetadata,
                    computeChecksums,
                    ct);
            }

            // Set description if provided
            if (!string.IsNullOrEmpty(description))
            {
                manifest.Description = description;
                await manifest.SaveAsync(Path.Combine(outputPath, BackupManifest.FileName), ct);
            }

            // Output results
            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    manifest.BackupId,
                    CreatedAt = manifest.CreatedAt.ToString("O"),
                    manifest.TotalFiles,
                    manifest.TotalBytes,
                    TopicCount = manifest.Topics.Count,
                    Topics = manifest.Topics.Select(t => t.Name),
                    OutputPath = outputPath
                };
                System.Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions));
            }
            else if (format == OutputFormat.Plain)
            {
                System.Console.WriteLine($"Backup ID: {manifest.BackupId}");
                System.Console.WriteLine($"Created: {manifest.CreatedAt}");
                System.Console.WriteLine($"Topics: {manifest.Topics.Count}");
                System.Console.WriteLine($"Files: {manifest.TotalFiles}");
                System.Console.WriteLine($"Size: {manifest.TotalBytes}");
                System.Console.WriteLine($"Output: {outputPath}");
            }
            else
            {
                WriteSuccess("Backup created successfully");
                WriteLine();

                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();

                grid.AddRow("[bold]Backup ID:[/]", manifest.BackupId);
                grid.AddRow("[bold]Created:[/]", manifest.CreatedAt.ToString("yyyy-MM-dd HH:mm:ss"));
                grid.AddRow("[bold]Topics:[/]", manifest.Topics.Count.ToString());
                grid.AddRow("[bold]Files:[/]", manifest.TotalFiles.ToString("N0"));
                grid.AddRow("[bold]Size:[/]", FormatBytes(manifest.TotalBytes));
                grid.AddRow("[bold]Checksums:[/]", computeChecksums ? "[green]Yes[/]" : "[yellow]No[/]");
                grid.AddRow("[bold]Metadata:[/]", includeMetadata ? "[green]Included[/]" : "[yellow]Excluded[/]");
                grid.AddRow("[bold]Output:[/]", $"[cyan]{outputPath}[/]");

                WriteRenderable(grid);

                if (manifest.Topics.Count > 0)
                {
                    WriteLine();
                    WriteMarkup("[bold]Topics backed up:[/]");

                    var table = new Table();
                    table.AddColumn("Topic");
                    table.AddColumn("Partitions");
                    table.AddColumn("Segments");
                    table.AddColumn("Size");

                    foreach (var topic in manifest.Topics.OrderBy(t => t.Name))
                    {
                        table.AddRow(
                            $"[cyan]{topic.Name}[/]",
                            topic.PartitionCount.ToString(),
                            topic.TotalSegments.ToString(),
                            FormatBytes(topic.TotalBytes));
                    }

                    WriteRenderable(table);
                }
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Backup failed: {ex.Message}");
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
