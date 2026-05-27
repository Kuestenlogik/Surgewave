using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Admin;
using Kuestenlogik.Surgewave.Protocol.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Acls;

/// <summary>
/// Remove ACLs (surgewave acls remove)
/// </summary>
public class RemoveAclCommand : CommandBase
{
    private readonly Option<string?> _principalOpt = new("--principal", "-p") { Description = "Filter by principal" };
    private readonly Option<string?> _resourceTypeOpt = new("--resource-type", "-t") { Description = "Filter by resource type" };
    private readonly Option<string?> _resourceNameOpt = new("--resource", "-r") { Description = "Filter by resource name" };
    private readonly Option<string?> _operationOpt = new("--operation", "-o") { Description = "Filter by operation" };
    private readonly Option<bool> _yesOpt = new("--yes", "-y") { Description = "Skip confirmation prompt" };

    public RemoveAclCommand() : base("remove", "Remove ACLs matching filter")
    {
        Options.Add(_principalOpt);
        Options.Add(_resourceTypeOpt);
        Options.Add(_resourceNameOpt);
        Options.Add(_operationOpt);
        Options.Add(_yesOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var principal = parseResult.GetValue(_principalOpt);
        var resourceTypeStr = parseResult.GetValue(_resourceTypeOpt);
        var resourceName = parseResult.GetValue(_resourceNameOpt);
        var operationStr = parseResult.GetValue(_operationOpt);
        var localYes = parseResult.GetValue(_yesOpt);

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var resourceType = ParseResourceType(resourceTypeStr);
            var operation = ParseOperation(operationStr);

            // First, describe what will be deleted
            var existing = await client.Admin.DescribeAclsAsync(
                resourceType: resourceType,
                resourceName: resourceName,
                principal: principal,
                operation: operation,
                cancellationToken: ct);

            if (existing.Acls.Count == 0)
            {
                WriteWarning("No ACLs match the specified filter");
                return 0;
            }

            AnsiConsole.MarkupLine($"[yellow]Found {existing.Acls.Count} ACL(s) matching filter:[/]");
            foreach (var acl in existing.Acls)
            {
                AnsiConsole.MarkupLine($"  - {acl.Principal} {acl.Permission} {acl.Operation} on {acl.ResourceType}:{acl.ResourceName}");
            }

            if (format == OutputFormat.Table &&
                !ConfirmDestructive(parseResult, "\nDelete these ACLs?", localYes))
            {
                WriteWarning("Delete cancelled.");
                return 0;
            }

            var result = await client.Admin.DeleteAclsAsync(
                resourceType: resourceType,
                resourceName: resourceName,
                principal: principal,
                operation: operation,
                cancellationToken: ct);

            if (result.ErrorCode != SurgewaveErrorCode.None)
            {
                WriteError($"Failed to delete ACLs: {result.ErrorCode}");
                return 1;
            }

            if (format == OutputFormat.Json)
            {
                Console.WriteLine(JsonSerializer.Serialize(new { Deleted = result.DeletedAcls.Count }));
            }
            else if (format == OutputFormat.Plain)
            {
                Console.WriteLine($"deleted {result.DeletedAcls.Count}");
            }
            else
            {
                WriteSuccess($"Deleted {result.DeletedAcls.Count} ACL(s)");
            }
        }
        catch (Exception ex)
        {
            WriteError($"Failed to remove ACLs: {ex.Message}");
            return 1;
        }

        return 0;
    }

    private static AclResourceType ParseResourceType(string? type) => type?.ToLowerInvariant() switch
    {
        "topic" => AclResourceType.Topic,
        "group" => AclResourceType.Group,
        "cluster" => AclResourceType.Cluster,
        "transactional-id" or "transactionalid" => AclResourceType.TransactionalId,
        _ => AclResourceType.Any
    };

    private static AclOperation ParseOperation(string? op) => op?.ToLowerInvariant() switch
    {
        "read" => AclOperation.Read,
        "write" => AclOperation.Write,
        "create" => AclOperation.Create,
        "delete" => AclOperation.Delete,
        "alter" => AclOperation.Alter,
        "describe" => AclOperation.Describe,
        "all" => AclOperation.All,
        _ => AclOperation.Any
    };
}
