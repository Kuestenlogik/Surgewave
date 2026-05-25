using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol security/ACL operations.
/// </summary>
public sealed class NativeSecurityHandler : INativeRequestHandler
{
    private readonly AclAuthorizer? _aclAuthorizer;
    private readonly BrokerConfig _config;

    public IEnumerable<SurgewaveOpCode> SupportedOpCodes =>
    [
        SurgewaveOpCode.DescribeAcls,
        SurgewaveOpCode.CreateAcls,
        SurgewaveOpCode.DeleteAcls
    ];

    public NativeSecurityHandler(AclAuthorizer? aclAuthorizer, BrokerConfig config)
    {
        _aclAuthorizer = aclAuthorizer;
        _config = config;
    }

    public Task HandleAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        return context.Header.OpCode switch
        {
            SurgewaveOpCode.DescribeAcls => HandleDescribeAclsAsync(context, payload, cancellationToken),
            SurgewaveOpCode.CreateAcls => HandleCreateAclsAsync(context, payload, cancellationToken),
            SurgewaveOpCode.DeleteAcls => HandleDeleteAclsAsync(context, payload, cancellationToken),
            _ => Task.CompletedTask
        };
    }

    private async Task HandleDescribeAclsAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (_aclAuthorizer == null)
        {
            using var errorWriter = new BigEndianWriter();
            errorWriter.Write((ushort)SurgewaveErrorCode.SecurityDisabled);
            errorWriter.Write(0); // 0 ACLs
            await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.DescribeAcls,
                SurgewaveErrorCode.SecurityDisabled, errorWriter.AsMemory(), cancellationToken);
            return;
        }

        var reader = new SurgewavePayloadReader(payload.Span);
        var resourceTypeFilter = (AclResourceType)reader.ReadUInt8();
        var resourceNameFilter = reader.ReadString();
        var patternTypeFilter = (AclPatternType)reader.ReadUInt8();
        var principalFilter = reader.ReadString();
        var hostFilter = reader.ReadString();
        var operationFilter = (AclOperation)reader.ReadUInt8();
        var permissionFilter = (AclPermission)reader.ReadUInt8();

        var matchingAcls = _aclAuthorizer.ListAcls(acl =>
        {
            if (resourceTypeFilter != AclResourceType.Any && resourceTypeFilter != acl.ResourceType) return false;
            if (resourceNameFilter != null && !string.Equals(resourceNameFilter, acl.ResourceName, StringComparison.OrdinalIgnoreCase)) return false;
            if (patternTypeFilter != AclPatternType.Any && patternTypeFilter != AclPatternType.Match && patternTypeFilter != acl.PatternType) return false;
            if (principalFilter != null && !string.Equals(principalFilter, acl.Principal, StringComparison.OrdinalIgnoreCase)) return false;
            if (hostFilter != null && !string.Equals(hostFilter, acl.Host, StringComparison.OrdinalIgnoreCase)) return false;
            if (operationFilter != AclOperation.Any && operationFilter != acl.Operation) return false;
            if (permissionFilter != AclPermission.Any && permissionFilter != acl.Permission) return false;
            return true;
        }).ToList();

        using var writer = new BigEndianWriter();
        writer.Write((ushort)SurgewaveErrorCode.None);
        writer.Write(matchingAcls.Count);

        foreach (var acl in matchingAcls)
        {
            writer.Write((byte)acl.ResourceType);
            writer.WriteNullableString(acl.ResourceName);
            writer.Write((byte)acl.PatternType);
            writer.WriteNullableString(acl.Principal);
            writer.WriteNullableString(acl.Host);
            writer.Write((byte)acl.Operation);
            writer.Write((byte)acl.Permission);
        }

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.DescribeAcls,
            SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
    }

    private async Task HandleCreateAclsAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var count = reader.ReadInt32();
        var results = new List<(SurgewaveErrorCode ErrorCode, string? ErrorMessage)>(count);

        if (_aclAuthorizer == null)
        {
            for (int i = 0; i < count; i++)
            {
                // Skip the entry data
                reader.ReadUInt8(); // resourceType
                reader.ReadString(); // resourceName
                reader.ReadUInt8(); // patternType
                reader.ReadString(); // principal
                reader.ReadString(); // host
                reader.ReadUInt8(); // operation
                reader.ReadUInt8(); // permission
                results.Add((SurgewaveErrorCode.SecurityDisabled, "ACL authorization is not enabled"));
            }
        }
        else
        {
            for (int i = 0; i < count; i++)
            {
                try
                {
                    var resourceType = (AclResourceType)reader.ReadUInt8();
                    var resourceName = reader.ReadString() ?? string.Empty;
                    var patternType = (AclPatternType)reader.ReadUInt8();
                    var principal = reader.ReadString() ?? string.Empty;
                    var host = reader.ReadString() ?? "*";
                    var operation = (AclOperation)reader.ReadUInt8();
                    var permission = (AclPermission)reader.ReadUInt8();

                    _aclAuthorizer.AddAcl(new AclEntry
                    {
                        Principal = principal,
                        Host = host,
                        ResourceType = resourceType,
                        ResourceName = resourceName,
                        PatternType = patternType,
                        Operation = operation,
                        Permission = permission
                    });
                    results.Add((SurgewaveErrorCode.None, null));
                }
                catch (Exception ex)
                {
                    results.Add((SurgewaveErrorCode.UnknownError, ex.Message));
                }
            }

            // Save to file if configured
            if (_config.Security.AclFile != null)
            {
                try { _aclAuthorizer.SaveToFile(_config.Security.AclFile); }
                catch { /* Ignore save errors */ }
            }
        }

        using var writer = new BigEndianWriter();
        writer.Write(results.Count);
        foreach (var (errorCode, errorMessage) in results)
        {
            writer.Write((ushort)errorCode);
            writer.WriteNullableString(errorMessage);
        }

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.CreateAcls,
            SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
    }

    private async Task HandleDeleteAclsAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (_aclAuthorizer == null)
        {
            using var errorWriter = new BigEndianWriter();
            errorWriter.Write((ushort)SurgewaveErrorCode.SecurityDisabled);
            errorWriter.Write(0); // 0 deleted ACLs
            await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.DeleteAcls,
                SurgewaveErrorCode.SecurityDisabled, errorWriter.AsMemory(), cancellationToken);
            return;
        }

        var reader = new SurgewavePayloadReader(payload.Span);
        var resourceTypeFilter = (AclResourceType)reader.ReadUInt8();
        var resourceNameFilter = reader.ReadString();
        var patternTypeFilter = (AclPatternType)reader.ReadUInt8();
        var principalFilter = reader.ReadString();
        var hostFilter = reader.ReadString();
        var operationFilter = (AclOperation)reader.ReadUInt8();
        var permissionFilter = (AclPermission)reader.ReadUInt8();

        // Find matching ACLs before deleting
        var matchingAcls = _aclAuthorizer.ListAcls(acl =>
        {
            if (resourceTypeFilter != AclResourceType.Any && resourceTypeFilter != acl.ResourceType) return false;
            if (resourceNameFilter != null && !string.Equals(resourceNameFilter, acl.ResourceName, StringComparison.OrdinalIgnoreCase)) return false;
            if (patternTypeFilter != AclPatternType.Any && patternTypeFilter != AclPatternType.Match && patternTypeFilter != acl.PatternType) return false;
            if (principalFilter != null && !string.Equals(principalFilter, acl.Principal, StringComparison.OrdinalIgnoreCase)) return false;
            if (hostFilter != null && !string.Equals(hostFilter, acl.Host, StringComparison.OrdinalIgnoreCase)) return false;
            if (operationFilter != AclOperation.Any && operationFilter != acl.Operation) return false;
            if (permissionFilter != AclPermission.Any && permissionFilter != acl.Permission) return false;
            return true;
        }).ToList();

        // Delete matching ACLs
        _aclAuthorizer.RemoveAcls(acl => matchingAcls.Contains(acl));

        // Save to file if configured
        if (_config.Security.AclFile != null)
        {
            try { _aclAuthorizer.SaveToFile(_config.Security.AclFile); }
            catch { /* Ignore save errors */ }
        }

        using var writer = new BigEndianWriter();
        writer.Write((ushort)SurgewaveErrorCode.None);
        writer.Write(matchingAcls.Count);

        foreach (var acl in matchingAcls)
        {
            writer.Write((byte)acl.ResourceType);
            writer.WriteNullableString(acl.ResourceName);
            writer.Write((byte)acl.PatternType);
            writer.WriteNullableString(acl.Principal);
            writer.WriteNullableString(acl.Host);
            writer.Write((byte)acl.Operation);
            writer.Write((byte)acl.Permission);
        }

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.DeleteAcls,
            SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
    }
}
