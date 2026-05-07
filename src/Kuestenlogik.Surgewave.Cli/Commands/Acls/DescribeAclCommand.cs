using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Admin;
using Kuestenlogik.Surgewave.Protocol.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Acls;

/// <summary>
/// Describe ACLs for a specific resource (surgewave acls describe)
/// </summary>
public class DescribeAclCommand : CommandBase
{
    private readonly Argument<string> _resourceTypeArg = new("resource-type") { Description = "Resource type (topic, group, cluster, transactional-id)" };
    private readonly Argument<string> _resourceNameArg = new("resource-name") { Description = "Resource name" };

    public DescribeAclCommand() : base("describe", "Describe ACLs for a specific resource")
    {
        Arguments.Add(_resourceTypeArg);
        Arguments.Add(_resourceNameArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var resourceTypeStr = parseResult.GetValue(_resourceTypeArg);
        var resourceName = parseResult.GetValue(_resourceNameArg);

        WriteVerbose(parseResult, $"Describing ACLs for {resourceTypeStr}:{resourceName}...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var resourceType = resourceTypeStr!.ToLowerInvariant() switch
            {
                "topic" => AclResourceType.Topic,
                "group" => AclResourceType.Group,
                "cluster" => AclResourceType.Cluster,
                "transactional-id" or "transactionalid" => AclResourceType.TransactionalId,
                _ => throw new ArgumentException($"Unknown resource type: {resourceTypeStr}")
            };

            var result = await client.Admin.DescribeAclsAsync(
                resourceType: resourceType,
                resourceName: resourceName,
                cancellationToken: ct);

            if (result.ErrorCode != SurgewaveErrorCode.None)
            {
                WriteError($"Failed to describe ACLs: {result.ErrorCode}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                var output = new
                {
                    ResourceType = resourceTypeStr,
                    ResourceName = resourceName,
                    Acls = result.Acls.Select(acl => new
                    {
                        acl.Principal,
                        acl.Host,
                        Operation = acl.Operation.ToString(),
                        Permission = acl.Permission.ToString()
                    }).ToList()
                };
                Console.WriteLine(JsonSerializer.Serialize(output, AclsJsonOptions.Indented));
            }
            else
            {
                AnsiConsole.MarkupLine($"[bold]Resource:[/] {resourceTypeStr}:{resourceName}");
                AnsiConsole.WriteLine();

                if (result.Acls.Count == 0)
                {
                    AnsiConsole.MarkupLine("[dim]No ACLs found[/]");
                    return 0;
                }

                var table = new Table();
                table.AddColumn("Principal");
                table.AddColumn("Host");
                table.AddColumn("Operation");
                table.AddColumn("Permission");

                foreach (var acl in result.Acls)
                {
                    var permission = acl.Permission == AclPermission.Allow
                        ? "[green]Allow[/]"
                        : "[red]Deny[/]";
                    table.AddRow(
                        acl.Principal ?? "*",
                        acl.Host ?? "*",
                        acl.Operation.ToString(),
                        permission
                    );
                }

                AnsiConsole.Write(table);
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to describe ACLs: {ex.Message}");
            return 1;
        }
    }
}
