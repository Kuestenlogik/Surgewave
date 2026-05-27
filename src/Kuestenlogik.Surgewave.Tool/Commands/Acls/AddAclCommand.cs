using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Admin;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Cli.Commands.Acls;

/// <summary>
/// Add an ACL (surgewave acls add)
/// </summary>
public class AddAclCommand : CommandBase
{
    private readonly Option<string> _principalOpt = new("--principal", "-p") { Description = "Principal (e.g., User:alice)", Required = true };
    private readonly Option<string> _resourceTypeOpt = new("--resource-type", "-t") { Description = "Resource type (topic, group, cluster, transactional-id)", Required = true };
    private readonly Option<string> _resourceNameOpt = new("--resource", "-r") { Description = "Resource name (use * for all)", Required = true };
    private readonly Option<string> _operationOpt = new("--operation", "-o") { Description = "Operation (read, write, create, delete, alter, describe, all)", Required = true };
    private readonly Option<string> _permissionOpt = new("--permission") { Description = "Permission type (allow, deny)", DefaultValueFactory = _ => "allow" };
    private readonly Option<string> _hostOpt = new("--host", "-h") { Description = "Host (* for all)", DefaultValueFactory = _ => "*" };
    private readonly Option<string> _patternTypeOpt = new("--pattern-type") { Description = "Pattern type (literal, prefixed)", DefaultValueFactory = _ => "literal" };

    public AddAclCommand() : base("add", "Add a new ACL")
    {
        Options.Add(_principalOpt);
        Options.Add(_resourceTypeOpt);
        Options.Add(_resourceNameOpt);
        Options.Add(_operationOpt);
        Options.Add(_permissionOpt);
        Options.Add(_hostOpt);
        Options.Add(_patternTypeOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var principal = parseResult.GetValue(_principalOpt)!;
        var resourceTypeStr = parseResult.GetValue(_resourceTypeOpt)!;
        var resourceName = parseResult.GetValue(_resourceNameOpt)!;
        var operationStr = parseResult.GetValue(_operationOpt)!;
        var permissionStr = parseResult.GetValue(_permissionOpt)!;
        var hostValue = parseResult.GetValue(_hostOpt)!;
        var patternTypeStr = parseResult.GetValue(_patternTypeOpt)!;

        WriteVerbose(parseResult, $"Adding ACL for {principal} on {resourceTypeStr}:{resourceName}...");

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            var acl = new AclEntry
            {
                ResourceType = ParseResourceType(resourceTypeStr),
                ResourceName = resourceName,
                PatternType = ParsePatternType(patternTypeStr),
                Principal = principal,
                Host = hostValue,
                Operation = ParseOperation(operationStr),
                Permission = ParsePermission(permissionStr)
            };

            var results = await client.Admin.CreateAclsAsync([acl], ct);

            if (results.Count > 0 && results[0].ErrorCode != SurgewaveErrorCode.None)
            {
                WriteError($"Failed to add ACL: {results[0].ErrorCode} - {results[0].ErrorMessage}");
                return 1;
            }

            WriteSuccess($"Added ACL: {principal} {permissionStr} {operationStr} on {resourceTypeStr}:{resourceName}");
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Failed to add ACL: {ex.Message}");
            return 1;
        }
    }

    private static AclResourceType ParseResourceType(string type) => type.ToLowerInvariant() switch
    {
        "topic" => AclResourceType.Topic,
        "group" => AclResourceType.Group,
        "cluster" => AclResourceType.Cluster,
        "transactional-id" or "transactionalid" => AclResourceType.TransactionalId,
        _ => throw new ArgumentException($"Unknown resource type: {type}")
    };

    private static AclPatternType ParsePatternType(string type) => type.ToLowerInvariant() switch
    {
        "literal" => AclPatternType.Literal,
        "prefixed" => AclPatternType.Prefixed,
        _ => AclPatternType.Literal
    };

    private static AclOperation ParseOperation(string op) => op.ToLowerInvariant() switch
    {
        "read" => AclOperation.Read,
        "write" => AclOperation.Write,
        "create" => AclOperation.Create,
        "delete" => AclOperation.Delete,
        "alter" => AclOperation.Alter,
        "describe" => AclOperation.Describe,
        "all" => AclOperation.All,
        "cluster-action" or "clusteraction" => AclOperation.ClusterAction,
        "describe-configs" or "describeconfigs" => AclOperation.DescribeConfigs,
        "alter-configs" or "alterconfigs" => AclOperation.AlterConfigs,
        "idempotent-write" or "idempotentwrite" => AclOperation.IdempotentWrite,
        _ => throw new ArgumentException($"Unknown operation: {op}")
    };

    private static AclPermission ParsePermission(string perm) => perm.ToLowerInvariant() switch
    {
        "allow" => AclPermission.Allow,
        "deny" => AclPermission.Deny,
        _ => AclPermission.Allow
    };
}
