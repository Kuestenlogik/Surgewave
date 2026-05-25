using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Backup;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Backup;

/// <summary>
/// Verify backup integrity (surgewave backup verify)
/// </summary>
public class VerifyBackupCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Option<string> _inputOpt = new("--input", "-i")
    {
        Description = "Backup directory to verify",
        Required = true
    };

    private readonly Option<bool> _detailsOpt = new("--details")
    {
        Description = "Show details for each corrupted or missing file",
        DefaultValueFactory = _ => true
    };

    public VerifyBackupCommand() : base("verify", "Verify backup integrity by checking checksums")
    {
        Options.Add(_inputOpt);
        Options.Add(_detailsOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var format = GetFormat(parseResult);
        var inputPath = parseResult.GetValue(_inputOpt)!;
        var showDetails = parseResult.GetValue(_detailsOpt);

        // Validate backup directory exists
        var manifestPath = Path.Combine(inputPath, BackupManifest.FileName);
        if (!File.Exists(manifestPath))
        {
            WriteError($"Backup manifest not found: {manifestPath}");
            return 1;
        }

        try
        {
            var restoreService = new RestoreService();
            VerifyResult result;

            if (format != OutputFormat.Json && format != OutputFormat.Plain)
            {
                result = await AnsiConsole.Status()
                    .Spinner(Spinner.Known.Dots)
                    .StartAsync("Verifying backup integrity...", async ctx =>
                    {
                        return await restoreService.VerifyAsync(inputPath, ct);
                    });
            }
            else
            {
                result = await restoreService.VerifyAsync(inputPath, ct);
            }

            // Output results
            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    result.IsValid,
                    result.BackupId,
                    BackupCreatedAt = result.BackupCreatedAt?.ToString("O"),
                    result.TotalFiles,
                    result.TotalBytes,
                    result.TopicCount,
                    result.FilesVerified,
                    result.BytesVerified,
                    MissingFilesCount = result.MissingFiles.Count,
                    CorruptedFilesCount = result.CorruptedFiles.Count,
                    MissingFiles = result.MissingFiles,
                    CorruptedFiles = result.CorruptedFiles.Select(c => new
                    {
                        c.Path,
                        c.ExpectedHash,
                        c.ActualHash
                    }),
                    result.ErrorMessage
                };
                System.Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions));
            }
            else if (format == OutputFormat.Plain)
            {
                System.Console.WriteLine($"Valid: {result.IsValid}");
                System.Console.WriteLine($"Backup ID: {result.BackupId}");
                System.Console.WriteLine($"Files verified: {result.FilesVerified}");
                System.Console.WriteLine($"Bytes verified: {result.BytesVerified}");
                System.Console.WriteLine($"Missing files: {result.MissingFiles.Count}");
                System.Console.WriteLine($"Corrupted files: {result.CorruptedFiles.Count}");
                if (!result.IsValid)
                {
                    System.Console.WriteLine($"Error: {result.ErrorMessage}");
                }
            }
            else
            {
                if (result.IsValid)
                {
                    WriteSuccess("Backup integrity verified - no issues found");
                }
                else
                {
                    WriteError($"Backup integrity check failed: {result.ErrorMessage}");
                }

                WriteLine();

                // Summary grid
                var grid = new Grid();
                grid.AddColumn();
                grid.AddColumn();

                grid.AddRow("[bold]Backup ID:[/]", result.BackupId ?? "Unknown");
                grid.AddRow("[bold]Created:[/]", result.BackupCreatedAt?.ToString("yyyy-MM-dd HH:mm:ss") ?? "Unknown");
                grid.AddRow("[bold]Topics:[/]", result.TopicCount.ToString());
                grid.AddRow("[bold]Files verified:[/]", $"{result.FilesVerified:N0} / {result.TotalFiles:N0}");
                grid.AddRow("[bold]Bytes verified:[/]", $"{FormatBytes(result.BytesVerified)} / {FormatBytes(result.TotalBytes)}");
                grid.AddRow("[bold]Missing files:[/]", result.MissingFiles.Count > 0
                    ? $"[red]{result.MissingFiles.Count}[/]"
                    : "[green]0[/]");
                grid.AddRow("[bold]Corrupted files:[/]", result.CorruptedFiles.Count > 0
                    ? $"[red]{result.CorruptedFiles.Count}[/]"
                    : "[green]0[/]");

                WriteRenderable(grid);

                // Show missing files if any
                if (result.MissingFiles.Count > 0 && showDetails)
                {
                    WriteLine();
                    WriteRenderable(new Rule("[bold red]Missing Files[/]").LeftJustified());
                    WriteLine();

                    var missingTable = new Table();
                    missingTable.AddColumn("File Path");

                    foreach (var file in result.MissingFiles.Take(20))
                    {
                        missingTable.AddRow($"[red]{file}[/]");
                    }

                    if (result.MissingFiles.Count > 20)
                    {
                        missingTable.AddRow($"[dim]... and {result.MissingFiles.Count - 20} more[/]");
                    }

                    WriteRenderable(missingTable);
                }

                // Show corrupted files if any
                if (result.CorruptedFiles.Count > 0 && showDetails)
                {
                    WriteLine();
                    WriteRenderable(new Rule("[bold red]Corrupted Files[/]").LeftJustified());
                    WriteLine();

                    var corruptedTable = new Table();
                    corruptedTable.AddColumn("File Path");
                    corruptedTable.AddColumn("Expected Hash");
                    corruptedTable.AddColumn("Actual Hash");

                    foreach (var file in result.CorruptedFiles.Take(20))
                    {
                        corruptedTable.AddRow(
                            $"[red]{file.Path}[/]",
                            $"[yellow]{file.ExpectedHash[..16]}...[/]",
                            $"[red]{file.ActualHash[..16]}...[/]");
                    }

                    if (result.CorruptedFiles.Count > 20)
                    {
                        corruptedTable.AddRow(
                            $"[dim]... and {result.CorruptedFiles.Count - 20} more[/]",
                            "",
                            "");
                    }

                    WriteRenderable(corruptedTable);
                }
            }

            return result.IsValid ? 0 : 1;
        }
        catch (Exception ex)
        {
            WriteError($"Verification failed: {ex.Message}");
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
