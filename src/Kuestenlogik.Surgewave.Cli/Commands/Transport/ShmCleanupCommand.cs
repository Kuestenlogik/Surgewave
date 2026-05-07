using System.CommandLine;
using System.CommandLine.Parsing;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Transport;

/// <summary>
/// Shared memory cleanup command (surgewave transport shm-cleanup)
/// </summary>
public class ShmCleanupCommand : CommandBase
{
    private readonly Option<bool> _forceOption = new("--force", "-f") { Description = "Force cleanup without confirmation" };
    private readonly Option<bool> _dryRunOption = new("--dry-run", "-n") { Description = "Show what would be cleaned without actually deleting" };

    public ShmCleanupCommand() : base("shm-cleanup", "Clean up orphaned shared memory files")
    {
        Options.Add(_forceOption);
        Options.Add(_dryRunOption);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var force = parseResult.GetValue(_forceOption);
        var dryRun = parseResult.GetValue(_dryRunOption);
        // This command only works on Unix-like systems
        if (OperatingSystem.IsWindows())
        {
            WriteWarning("Shared memory cleanup is not needed on Windows.");
            AnsiConsole.MarkupLine("[dim]Windows automatically cleans up named objects when all handles are closed.[/]");
            return 0;
        }

        try
        {
            var filesToClean = FindOrphanedFiles();

            if (filesToClean.Count == 0)
            {
                WriteSuccess("No orphaned shared memory files found.");
                return 0;
            }

            AnsiConsole.MarkupLine($"Found [bold]{filesToClean.Count}[/] orphaned shared memory file(s):");
            AnsiConsole.WriteLine();

            var table = new Table();
            table.AddColumn("File");
            table.AddColumn("Size");
            table.AddColumn("Last Modified");
            table.Border = TableBorder.Rounded;

            long totalSize = 0;
            foreach (var file in filesToClean)
            {
                var info = new FileInfo(file);
                if (info.Exists)
                {
                    table.AddRow(
                        Path.GetFileName(file),
                        FormatSize(info.Length),
                        info.LastWriteTime.ToString("g"));
                    totalSize += info.Length;
                }
            }

            AnsiConsole.Write(table);
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine($"Total size: [bold]{FormatSize(totalSize)}[/]");
            AnsiConsole.WriteLine();

            if (dryRun)
            {
                WriteWarning("Dry run - no files were deleted.");
                return 0;
            }

            if (!force)
            {
                var confirm = AnsiConsole.Confirm("Delete these files?", false);
                if (!confirm)
                {
                    AnsiConsole.MarkupLine("[dim]Cleanup cancelled.[/]");
                    return 0;
                }
            }

            // Delete files
            var deleted = 0;
            var failed = 0;

            await AnsiConsole.Status()
                .StartAsync("Cleaning up...", async ctx =>
                {
                    foreach (var file in filesToClean)
                    {
                        ctx.Status($"Deleting {Path.GetFileName(file)}...");

                        try
                        {
                            File.Delete(file);
                            deleted++;
                        }
                        catch (Exception ex)
                        {
                            WriteError($"Failed to delete {file}: {ex.Message}");
                            failed++;
                        }

                        await Task.Delay(10); // Small delay for visual feedback
                    }
                });

            AnsiConsole.WriteLine();

            if (failed == 0)
            {
                WriteSuccess($"Successfully deleted {deleted} file(s).");
            }
            else
            {
                WriteWarning($"Deleted {deleted} file(s), {failed} failed.");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to clean up: {ex.Message}");
            return 1;
        }
    }

    private static List<string> FindOrphanedFiles()
    {
        var orphanedFiles = new List<string>();

        // Check both Linux and macOS paths
        var directories = new List<string>();

        if (OperatingSystem.IsLinux() && Directory.Exists("/dev/shm"))
        {
            directories.Add("/dev/shm");
        }

        if (OperatingSystem.IsMacOS() && Directory.Exists("/tmp"))
        {
            directories.Add("/tmp");
        }

        foreach (var directory in directories)
        {
            try
            {
                // Find all surgewave-related files
                var surgewaveFiles = Directory.GetFiles(directory, "surgewave-*");

                foreach (var file in surgewaveFiles)
                {
                    // Check if this is a client file
                    var fileName = Path.GetFileName(file);

                    // Client files follow pattern: surgewave-broker-{port}-client-{uuid}-{request|response}
                    if (fileName.Contains("-client-"))
                    {
                        // Check if this looks like an orphaned file
                        // An orphaned file is one where:
                        // 1. Only one of request/response exists
                        // 2. File is older than a few minutes (connection timeout)
                        var info = new FileInfo(file);
                        var age = DateTime.UtcNow - info.LastWriteTimeUtc;

                        if (age > TimeSpan.FromMinutes(5))
                        {
                            // Check if paired file exists
                            string pairedFile;
                            if (file.EndsWith("-request", StringComparison.Ordinal))
                            {
                                pairedFile = file.Replace("-request", "-response", StringComparison.Ordinal);
                            }
                            else if (file.EndsWith("-response", StringComparison.Ordinal))
                            {
                                pairedFile = file.Replace("-response", "-request", StringComparison.Ordinal);
                            }
                            else
                            {
                                continue;
                            }

                            // If only one file of the pair exists, or both are old, add to cleanup
                            if (!File.Exists(pairedFile) || age > TimeSpan.FromHours(1))
                            {
                                orphanedFiles.Add(file);
                            }
                        }
                    }
                    // Legacy broker files
                    else if (fileName.EndsWith("-to-client", StringComparison.Ordinal) || fileName.EndsWith("-to-server", StringComparison.Ordinal))
                    {
                        var info = new FileInfo(file);
                        var age = DateTime.UtcNow - info.LastWriteTimeUtc;

                        // Only clean up if older than 1 hour (broker likely dead)
                        if (age > TimeSpan.FromHours(1))
                        {
                            orphanedFiles.Add(file);
                        }
                    }
                }
            }
            catch (UnauthorizedAccessException)
            {
                // Skip directories we can't access
            }
        }

        return orphanedFiles;
    }

    private static string FormatSize(long bytes)
    {
        if (bytes < 1024)
            return $"{bytes} B";
        if (bytes < 1024 * 1024)
            return $"{bytes / 1024.0:F1} KB";
        if (bytes < 1024 * 1024 * 1024)
            return $"{bytes / (1024.0 * 1024):F1} MB";
        return $"{bytes / (1024.0 * 1024 * 1024):F1} GB";
    }
}
