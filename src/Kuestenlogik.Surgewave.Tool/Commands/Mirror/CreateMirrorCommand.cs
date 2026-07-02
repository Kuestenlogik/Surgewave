using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Mirror;

/// <summary>
/// Create a cross-cluster replication flow (surgewave mirror create)
/// </summary>
public class CreateMirrorCommand : CommandBase
{
    private const string SourceConnectorClass = "Kuestenlogik.Surgewave.Connector.Mirror.MirrorSourceConnector";
    private const string CheckpointConnectorClass = "Kuestenlogik.Surgewave.Connector.Mirror.MirrorCheckpointConnector";
    private const string HeartbeatConnectorClass = "Kuestenlogik.Surgewave.Connector.Mirror.MirrorHeartbeatConnector";

    private readonly Option<string> _nameOption = new("--name", "-n") { Description = "Name for this replication flow", Required = true };

    private readonly Option<string> _sourceAliasOption = new("--source-alias") { Description = "Alias for the source cluster", Required = true };

    private readonly Option<string> _sourceServersOption = new("--source-servers") { Description = "Bootstrap servers for source cluster", Required = true };

    private readonly Option<string> _targetAliasOption = new("--target-alias") { Description = "Alias for the target cluster", Required = true };

    private readonly Option<string> _targetServersOption = new("--target-servers") { Description = "Bootstrap servers for target cluster", Required = true };

    private readonly Option<string> _topicsOption = new("--topics", "-t") { Description = "Topics regex pattern to replicate", DefaultValueFactory = _ => ".*" };

    private readonly Option<string> _topicsWhitelistOption = new("--topics-whitelist") { Description = "Comma-separated list of topics to replicate" };

    private readonly Option<string> _topicsBlacklistOption = new("--topics-blacklist") { Description = "Comma-separated list of topics to exclude" };

    private readonly Option<int> _tasksOption = new("--tasks") { Description = "Number of parallel replication tasks", DefaultValueFactory = _ => 4 };

    private readonly Option<bool> _syncOffsetsOption = new("--sync-offsets") { Description = "Sync consumer group offsets for failover (deploys checkpoint connector)", DefaultValueFactory = _ => true };

    private readonly Option<bool> _emitHeartbeatsOption = new("--emit-heartbeats") { Description = "Emit heartbeat records for health monitoring (deploys heartbeat connector)", DefaultValueFactory = _ => true };

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

        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);

        WriteVerbose(parseResult, $"Creating replication flow '{name}'...");
        WriteVerbose(parseResult, $"  Source: {sourceAlias} ({sourceServers})");
        WriteVerbose(parseResult, $"  Target: {targetAlias} ({targetServers})");

        // Source connector config
        var sourceConfig = new Dictionary<string, string>
        {
            ["connector.class"] = SourceConnectorClass,
            ["source.cluster.alias"] = sourceAlias,
            ["source.bootstrap.servers"] = sourceServers,
            ["target.cluster.alias"] = targetAlias,
            ["target.bootstrap.servers"] = targetServers,
            ["topics"] = topics ?? ".*",
            ["tasks.max"] = tasks.ToString()
        };

        if (!string.IsNullOrEmpty(topicsWhitelist))
            sourceConfig["topics.whitelist"] = topicsWhitelist;
        if (!string.IsNullOrEmpty(topicsBlacklist))
            sourceConfig["topics.blacklist"] = topicsBlacklist;

        // Companion connectors are only deployed when requested
        var deployments = new List<(string ConnectorName, Dictionary<string, string> Config)>
        {
            ($"{name}-source", sourceConfig)
        };

        if (syncOffsets)
        {
            deployments.Add(($"{name}-checkpoint", new Dictionary<string, string>
            {
                ["connector.class"] = CheckpointConnectorClass,
                ["source.cluster.alias"] = sourceAlias,
                ["source.bootstrap.servers"] = sourceServers,
                ["target.cluster.alias"] = targetAlias,
                ["target.bootstrap.servers"] = targetServers,
                ["sync.group.offsets.enabled"] = "true"
            }));
        }

        if (emitHeartbeats)
        {
            deployments.Add(($"{name}-heartbeat", new Dictionary<string, string>
            {
                ["connector.class"] = HeartbeatConnectorClass,
                ["source.cluster.alias"] = sourceAlias,
                ["target.cluster.alias"] = targetAlias
            }));
        }

        var created = new List<(string ConnectorName, int TaskCount)>();

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            foreach (var (connectorName, config) in deployments)
            {
                WriteVerbose(parseResult, $"  Deploying connector '{connectorName}' ({config["connector.class"]})...");
                var result = await client.Connect.CreateConnectorAsync(connectorName, config, ct);
                created.Add((result.Name, result.TaskCount));
            }

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    Name = name,
                    Connectors = created.Select(c => new { Name = c.ConnectorName, c.TaskCount }).ToList()
                };
                Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var (connectorName, taskCount) in created)
                {
                    Console.WriteLine($"created {connectorName} tasks={taskCount}");
                }
            }
            else
            {
                var table = new Table();
                table.AddColumn("Connector");
                table.AddColumn("Tasks");
                table.Title = new TableTitle($"[bold]Mirror: {name}[/]");

                foreach (var (connectorName, taskCount) in created)
                {
                    table.AddRow(connectorName, taskCount.ToString());
                }

                AnsiConsole.Write(table);
                WriteSuccess($"Replication flow '{name}' created ({created.Count} connector(s) deployed).");
                AnsiConsole.MarkupLine("[dim]Use 'surgewave mirror status' to monitor replication.[/]");
            }

            return 0;
        }
        catch (Exception ex)
        {
            if (created.Count > 0)
            {
                WriteError($"Failed to create replication flow '{name}': {ex.Message} " +
                    $"(already deployed: {string.Join(", ", created.Select(c => c.ConnectorName))} — " +
                    $"use 'surgewave mirror delete {name}' to clean up)");
            }
            else
            {
                WriteError($"Failed to create replication flow '{name}': {ex.Message}");
            }
            return 1;
        }
    }
}
