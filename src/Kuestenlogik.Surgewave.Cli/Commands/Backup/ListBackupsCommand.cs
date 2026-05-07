using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Backup;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Backup;

/// <summary>
/// List available backups (surgewave backup list)
/// </summary>
public class ListBackupsCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Option<string?> _locationOpt = new("--location", "-l")
    {
        Description = "Directory containing backups (default: ./backups)"
    };

    public ListBackupsCommand() : base("list", "List available backups")
    {
        Options.Add(_locationOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var format = GetFormat(parseResult);
        var location = parseResult.GetValue(_locationOpt) ?? "./backups";

        if (!Directory.Exists(location))
        {
            if (format == OutputFormat.Json)
            {
                System.Console.WriteLine("[]");
            }
            else if (format == OutputFormat.Plain)
            {
                System.Console.WriteLine("No backups found");
            }
            else
            {
                WriteMarkup("[dim]No backups found at specified location.[/]");
            }
            return 0;
        }

        try
        {
            var restoreService = new RestoreService();
            var backups = await restoreService.ListBackupsAsync(location, ct);

            if (backups.Count == 0)
            {
                if (format == OutputFormat.Json)
                {
                    System.Console.WriteLine("[]");
                }
                else if (format == OutputFormat.Plain)
                {
                    System.Console.WriteLine("No backups found");
                }
                else
                {
                    WriteMarkup("[dim]No backups found at specified location.[/]");
                }
                return 0;
            }

            if (format == OutputFormat.Json)
            {
                var output = backups.Select(b => new
                {
                    b.BackupId,
                    CreatedAt = b.CreatedAt.ToString("O"),
                    b.TotalFiles,
                    b.TotalBytes,
                    TopicCount = b.Topics.Count,
                    Topics = b.Topics.Select(t => t.Name),
                    b.Description,
                    b.Verified
                });
                System.Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var backup in backups)
                {
                    System.Console.WriteLine($"ID: {backup.BackupId}");
                    System.Console.WriteLine($"Created: {backup.CreatedAt}");
                    System.Console.WriteLine($"Topics: {backup.Topics.Count}");
                    System.Console.WriteLine($"Files: {backup.TotalFiles}");
                    System.Console.WriteLine($"Size: {backup.TotalBytes}");
                    if (!string.IsNullOrEmpty(backup.Description))
                    {
                        System.Console.WriteLine($"Description: {backup.Description}");
                    }
                    System.Console.WriteLine();
                }
            }
            else
            {
                WriteRenderable(new Rule("[bold blue]Available Backups[/]").LeftJustified());
                WriteLine();

                var table = new Table();
                table.AddColumn("Backup ID");
                table.AddColumn("Created");
                table.AddColumn("Topics");
                table.AddColumn("Files");
                table.AddColumn("Size");
                table.AddColumn("Description");

                foreach (var backup in backups)
                {
                    table.AddRow(
                        $"[cyan]{backup.BackupId[..8]}...[/]",
                        backup.CreatedAt.ToString("yyyy-MM-dd HH:mm"),
                        backup.Topics.Count.ToString(),
                        backup.TotalFiles.ToString("N0"),
                        FormatBytes(backup.TotalBytes),
                        backup.Description ?? "[dim]-[/]");
                }

                WriteRenderable(table);
                WriteLine();
                WriteMarkup($"[dim]Total backups: {backups.Count}[/]");
            }

            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list backups: {ex.Message}");
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
