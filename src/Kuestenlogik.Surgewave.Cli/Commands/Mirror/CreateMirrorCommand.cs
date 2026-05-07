using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Create a cross-cluster replication flow (surgewave mirror create)
/// </summary>
public class CreateMirrorCommand : CommandBase
{
    private readonly Option<string> _nameOption = new("--name", "-n") { Description = "Name for this replication flow", Required = true };

    private readonly Option<string> _sourceAliasOption = new("--source-alias") { Description = "Alias for the source cluster", Required = true };

    private readonly Option<string> _sourceServersOption = new("--source-servers") { Description = "Bootstrap servers for source cluster", Required = true };

    private readonly Option<string> _targetAliasOption = new("--target-alias") { Description = "Alias for the target cluster", Required = true };

    private readonly Option<string> _targetServersOption = new("--target-servers") { Description = "Bootstrap servers for target cluster", Required = true };

    private readonly Option<string> _topicsOption = new("--topics", "-t") { Description = "Topics regex pattern to replicate", DefaultValueFactory = _ => ".*" };

    private readonly Option<string> _topicsWhitelistOption = new("--topics-whitelist") { Description = "Comma-separated list of topics to replicate" };

    private readonly Option<string> _topicsBlacklistOption = new("--topics-blacklist") { Description = "Comma-separated list of topics to exclude" };

    private readonly Option<int> _tasksOption = new("--tasks") { Description = "Number of parallel replication tasks", DefaultValueFactory = _ => 4 };

    private readonly Option<bool> _syncOffsetsOption = new("--sync-offsets") { Description = "Sync consumer group offsets for failover", DefaultValueFactory = _ => true };

    private readonly Option<bool> _emitHeartbeatsOption = new("--emit-heartbeats") { Description = "Emit heartbeat records for health monitoring", DefaultValueFactory = _ => true };

    public CreateMirrorCommand() : base("create", "Create a new replication flow")
    {
        Options.Add(_nameOption);
        Options.Add(_sourceAliasOption);
        Options.Add(_sourceServersOption);
        Options.Add(_targetAliasOption);
        Options.Add(_targetServersOption);
        Options.Add(_topicsOption);
        Options.Add(_topicsWhitelistOption);
        Options.Add(_topicsBlacklistOption);
        Options.Add(_tasksOption);
        Options.Add(_syncOffsetsOption);
        Options.Add(_emitHeartbeatsOption);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameOption)!;
        var sourceAlias = parseResult.GetValue(_sourceAliasOption)!;
        var sourceServers = parseResult.GetValue(_sourceServersOption)!;
        var targetAlias = parseResult.GetValue(_targetAliasOption)!;
        var targetServers = parseResult.GetValue(_targetServersOption)!;
        var topics = parseResult.GetValue(_topicsOption);
        var topicsWhitelist = parseResult.GetValue(_topicsWhitelistOption);
        var topicsBlacklist = parseResult.GetValue(_topicsBlacklistOption);
        var tasks = parseResult.GetValue(_tasksOption);
        var syncOffsets = parseResult.GetValue(_syncOffsetsOption);
        var emitHeartbeats = parseResult.GetValue(_emitHeartbeatsOption);

        var format = GetFormat(parseResult);

        WriteVerbose(parseResult, $"Creating replication flow '{name}'...");
        WriteVerbose(parseResult, $"  Source: {sourceAlias} ({sourceServers})");
        WriteVerbose(parseResult, $"  Target: {targetAlias} ({targetServers})");

        try
        {
            // Build connector configuration
            var config = new Dictionary<string, object>
            {
                ["name"] = name,
                ["connector.class"] = "Kuestenlogik.Surgewave.Connect.Mirror.MirrorSourceConnector",
                ["source.cluster.alias"] = sourceAlias,
                ["source.bootstrap.servers"] = sourceServers,
                ["target.cluster.alias"] = targetAlias,
                ["target.bootstrap.servers"] = targetServers,
                ["topics"] = topics ?? ".*",
                ["tasks.max"] = tasks,
                ["sync.group.offsets.enabled"] = syncOffsets,
                ["emit.heartbeats.enabled"] = emitHeartbeats
            };

            if (!string.IsNullOrEmpty(topicsWhitelist))
                config["topics.whitelist"] = topicsWhitelist;
            if (!string.IsNullOrEmpty(topicsBlacklist))
                config["topics.blacklist"] = topicsBlacklist;

            // In a real implementation, this would create connectors via Connect REST API
            // For now, we output the configuration
            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(config, JsonOptions.Indented));
            }
            else
            {
                var table = new Table();
                table.AddColumn("Property");
                table.AddColumn("Value");
                table.Title = new TableTitle($"[bold]Mirror: {name}[/]");

                foreach (var (key, value) in config)
                {
                    table.AddRow(key, value.ToString() ?? "");
                }

                AnsiConsole.Write(table);
                AnsiConsole.MarkupLine($"\n[green]Replication flow '{name}' configuration generated.[/]");
                AnsiConsole.MarkupLine("[dim]Use 'surgewave connect create' to deploy this configuration.[/]");
            }

            await Task.CompletedTask;
        }
        catch (Exception ex)
        {
            WriteError(ex);
            return 1;
        }

        return 0;
    }
}
