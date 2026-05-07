using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Admin;
using Kuestenlogik.Surgewave.Protocol.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Acls;

/// <summary>
/// List all ACLs (surgewave acls list)
/// </summary>
public class ListAclsCommand : CommandBase
{
    private readonly Option<string?> _principalOpt = new("--principal", "-p") { Description = "Filter by principal (e.g., User:alice)" };
    private readonly Option<string?> _resourceOpt = new("--resource", "-r") { Description = "Filter by resource name" };
    private readonly Option<string?> _resourceTypeOpt = new("--resource-type", "-t") { Description = "Filter by resource type (topic, group, cluster, transactional-id)", DefaultValueFactory = _ => null };

    public ListAclsCommand() : base("list", "List all ACLs")
    {
        Options.Add(_principalOpt);
        Options.Add(_resourceOpt);
        Options.Add(_resourceTypeOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var principal = parseResult.GetValue(_principalOpt);
        var resourceName = parseResult.GetValue(_resourceOpt);
        var resourceTypeStr = parseResult.GetValue(_resourceTypeOpt);

        WriteVerbose(parseResult, $"Connecting to {host}:{port}...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var resourceType = ParseResourceType(resourceTypeStr);
            var result = await client.Admin.DescribeAclsAsync(
                resourceType: resourceType,
                resourceName: resourceName,
                principal: principal,
                cancellationToken: ct);

            if (result.ErrorCode != SurgewaveErrorCode.None)
            {
                WriteError($"Failed to list ACLs: {result.ErrorCode}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                var output = result.Acls.Select(acl => new
                {
                    ResourceType = acl.ResourceType.ToString(),
                    acl.ResourceName,
                    PatternType = acl.PatternType.ToString(),
                    acl.Principal,
                    acl.Host,
                    Operation = acl.Operation.ToString(),
                    Permission = acl.Permission.ToString()
                }).ToList();
                Console.WriteLine(JsonSerializer.Serialize(output, AclsJsonOptions.Indented));
            }
            else if (format == OutputFormat.Plain)
            {
                foreach (var acl in result.Acls)
                {
                    Console.WriteLine($"{acl.ResourceType}\t{acl.ResourceName}\t{acl.Principal}\t{acl.Operation}\t{acl.Permission}");
                }
            }
            else
            {
                var table = new Table();
                table.AddColumn("Resource Type");
                table.AddColumn("Resource Name");
                table.AddColumn("Pattern");
                table.AddColumn("Principal");
                table.AddColumn("Host");
                table.AddColumn("Operation");
                table.AddColumn("Permission");

                foreach (var acl in result.Acls.OrderBy(a => a.ResourceType).ThenBy(a => a.ResourceName))
                {
                    var permission = acl.Permission == AclPermission.Allow
                        ? "[green]Allow[/]"
                        : "[red]Deny[/]";
                    table.AddRow(
                        acl.ResourceType.ToString(),
                        acl.ResourceName ?? "*",
                        acl.PatternType.ToString(),
                        acl.Principal ?? "*",
                        acl.Host ?? "*",
                        acl.Operation.ToString(),
                        permission
                    );
                }

                AnsiConsole.Write(table);
                AnsiConsole.MarkupLine($"\n[dim]Total: {result.Acls.Count} ACL(s)[/]");
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to list ACLs: {ex.Message}");
            return 1;
        }
    }

    private static AclResourceType ParseResourceType(string? type) => type?.ToLowerInvariant() switch
    {
        "topic" => AclResourceType.Topic,
        "group" => AclResourceType.Group,
        "cluster" => AclResourceType.Cluster,
        "transactional-id" or "transactionalid" => AclResourceType.TransactionalId,
        _ => AclResourceType.Any
    };
}
