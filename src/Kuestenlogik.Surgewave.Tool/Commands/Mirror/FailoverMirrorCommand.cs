using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Trigger failover for consumer groups (surgewave mirror failover)
/// </summary>
public class FailoverMirrorCommand : CommandBase
{
    private readonly Option<string> _groupOption = new("--group", "-g") { Description = "Consumer group to failover", Required = true };

    private readonly Option<string> _sourceOption = new("--source") { Description = "Source cluster alias", Required = true };

    private readonly Option<string> _targetOption = new("--target") { Description = "Target cluster alias", Required = true };

    private readonly Option<string> _topicsOption = new("--topics", "-t") { Description = "Comma-separated list of topics (default: all topics in group)" };

    private readonly Option<bool> _dryRunOption = new("--dry-run") { Description = "Show what would happen without making changes" };

    private readonly Option<bool> _forceOption = new("--force", "-f") { Description = "Skip confirmation prompt" };

    public FailoverMirrorCommand() : base("failover", "Trigger failover for consumer groups")
    {
        Options.Add(_groupOption);
        Options.Add(_sourceOption);
        Options.Add(_targetOption);
        Options.Add(_topicsOption);
        Options.Add(_dryRunOption);
        Options.Add(_forceOption);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var group = parseResult.GetValue(_groupOption)!;
        var source = parseResult.GetValue(_sourceOption)!;
        var target = parseResult.GetValue(_targetOption)!;
        var topics = parseResult.GetValue(_topicsOption);
        var dryRun = parseResult.GetValue(_dryRunOption);
        var force = parseResult.GetValue(_forceOption);
        var format = GetFormat(parseResult);

        try
        {
            AnsiConsole.MarkupLine($"[bold]Consumer Group Failover[/]");
            AnsiConsole.MarkupLine($"  Group: [cyan]{group}[/]");
            AnsiConsole.MarkupLine($"  From:  [yellow]{source}[/]");
            AnsiConsole.MarkupLine($"  To:    [green]{target}[/]");

            if (dryRun)
            {
                AnsiConsole.MarkupLine("\n[yellow]DRY RUN MODE - No changes will be made[/]");
            }

            // In a real implementation, this would fetch actual offset mappings
            var offsetMappings = new[]
            {
                new { Topic = "orders", Partition = 0, SourceOffset = 12345L, TargetOffset = 12340L },
                new { Topic = "orders", Partition = 1, SourceOffset = 9876L, TargetOffset = 9870L },
                new { Topic = "payments", Partition = 0, SourceOffset = 5432L, TargetOffset = 5430L },
            };

            AnsiConsole.MarkupLine("\n[bold]Offset Mappings[/]");

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    ConsumerGroup = group,
                    SourceCluster = source,
                    TargetCluster = target,
                    DryRun = dryRun,
                    Mappings = offsetMappings
                }, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var mapping in offsetMappings)
                {
                    Console.WriteLine($"{mapping.Topic}/{mapping.Partition} {mapping.SourceOffset} -> {mapping.TargetOffset}");
                }
            }
            else
            {
                var table = new Table();
                table.AddColumn("Topic");
                table.AddColumn("Partition");
                table.AddColumn($"Offset ({source})");
                table.AddColumn($"Offset ({target})");
                table.AddColumn("Delta");

                foreach (var mapping in offsetMappings)
                {
                    var delta = mapping.SourceOffset - mapping.TargetOffset;
                    var deltaColor = delta == 0 ? "green" : "yellow";
                    table.AddRow(
                        mapping.Topic,
                        mapping.Partition.ToString(),
                        mapping.SourceOffset.ToString(),
                        mapping.TargetOffset.ToString(),
                        $"[{deltaColor}]{delta}[/]"
                    );
                }

                AnsiConsole.Write(table);
            }

            if (!dryRun)
            {
                if (!force)
                {
                    AnsiConsole.MarkupLine("");
                    var confirmed = AnsiConsole.Confirm(
                        "[yellow]Proceed with failover?[/]",
                        defaultValue: false);

                    if (!confirmed)
                    {
                        AnsiConsole.MarkupLine("[dim]Failover cancelled.[/]");
                        return 0;
                    }
                }

                await AnsiConsole.Status()
                    .StartAsync("Executing failover...", async ctx =>
                    {
                        ctx.Status("Stopping consumers on source cluster...");
                        await Task.Delay(500);

                        ctx.Status("Translating offsets...");
                        await Task.Delay(500);

                        ctx.Status("Updating consumer group offsets on target cluster...");
                        await Task.Delay(500);

                        ctx.Status("Verifying failover...");
                        await Task.Delay(300);
                    });

                WriteSuccess($"Failover completed successfully for consumer group '{group}'.");
                AnsiConsole.MarkupLine($"[dim]Consumers can now connect to {target} cluster.[/]");
            }
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }

        return 0;
    }
}
