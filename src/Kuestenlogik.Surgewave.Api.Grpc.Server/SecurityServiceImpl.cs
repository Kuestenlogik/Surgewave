using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// ACL binding DTO for internal representation.
/// Uses proto enum values to avoid dependency on Broker types.
/// </summary>
public record AclBindingDto(
    int ResourceType,
    string ResourceName,
    int PatternType,
    string Principal,
    string Host,
    int Operation,
    int Permission);

/// <summary>
/// Delegate to describe ACLs matching a filter.
/// </summary>
public delegate List<AclBindingDto> DescribeAclsDelegate(AclBindingDto? filter);

/// <summary>
/// Delegate to create ACLs.
/// </summary>
public delegate List<int> CreateAclsDelegate(List<AclBindingDto> acls);

/// <summary>
/// Delegate to delete ACLs matching filters.
/// </summary>
public delegate List<(List<AclBindingDto> MatchingAcls, int ErrorCode)> DeleteAclsDelegate(List<AclBindingDto> filters);

/// <summary>
/// gRPC SecurityService implementation.
/// </summary>
public class SecurityServiceImpl : SecurityService.SecurityServiceBase
{
    private readonly DescribeAclsDelegate _describeAcls;
    private readonly CreateAclsDelegate _createAcls;
    private readonly DeleteAclsDelegate _deleteAcls;

    public SecurityServiceImpl(
        DescribeAclsDelegate describeAcls,
        CreateAclsDelegate createAcls,
        DeleteAclsDelegate deleteAcls)
    {
        _describeAcls = describeAcls;
        _createAcls = createAcls;
        _deleteAcls = deleteAcls;
    }

    public override Task<DescribeAclsResponse> DescribeAcls(DescribeAclsRequest request, ServerCallContext context)
    {
        AclBindingDto? filter = null;
        if (request.Filter != null)
        {
            filter = new AclBindingDto(
                (int)request.Filter.ResourceType,
                request.Filter.ResourceName,
                (int)request.Filter.PatternType,
                request.Filter.Principal,
                request.Filter.Host,
                (int)request.Filter.Operation,
                (int)request.Filter.Permission);
        }

        var acls = _describeAcls(filter);

        var response = new DescribeAclsResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        foreach (var acl in acls)
        {
            response.Acls.Add(MapToProtoAclBinding(acl));
        }

        return Task.FromResult(response);
    }

    public override Task<CreateAclsResponse> CreateAcls(CreateAclsRequest request, ServerCallContext context)
    {
        var acls = request.Acls
            .Select(a => new AclBindingDto(
                (int)a.ResourceType,
                a.ResourceName,
                (int)a.PatternType,
                a.Principal,
                a.Host,
                (int)a.Operation,
                (int)a.Permission))
            .ToList();

        var errorCodes = _createAcls(acls);

        var response = new CreateAclsResponse();
        foreach (var errorCode in errorCodes)
        {
            response.Results.Add(new AclCreationResult
            {
                Status = new ResponseStatus { ErrorCode = MapErrorCode(errorCode) }
            });
        }

        return Task.FromResult(response);
    }

    public override Task<DeleteAclsResponse> DeleteAcls(DeleteAclsRequest request, ServerCallContext context)
    {
        var filters = request.Filters
            .Select(f => new AclBindingDto(
                (int)f.ResourceType,
                f.ResourceName,
                (int)f.PatternType,
                f.Principal,
                f.Host,
                (int)f.Operation,
                (int)f.Permission))
            .ToList();

        var results = _deleteAcls(filters);

        var response = new DeleteAclsResponse();
        foreach (var (matchingAcls, errorCode) in results)
        {
            var deletionResult = new AclDeletionResult
            {
                Status = new ResponseStatus { ErrorCode = MapErrorCode(errorCode) }
            };

            foreach (var acl in matchingAcls)
            {
                deletionResult.MatchingAcls.Add(MapToProtoAclBinding(acl));
            }

            response.Results.Add(deletionResult);
        }

        return Task.FromResult(response);
    }

    private static AclBinding MapToProtoAclBinding(AclBindingDto acl) => new()
    {
        ResourceType = (AclResourceType)acl.ResourceType,
        ResourceName = acl.ResourceName,
        PatternType = (AclPatternType)acl.PatternType,
        Principal = acl.Principal,
        Host = acl.Host,
        Operation = (AclOperation)acl.Operation,
        Permission = (AclPermission)acl.Permission
    };

    private static ErrorCode MapErrorCode(int errorCode) => errorCode switch
    {
        0 => ErrorCode.None,
        _ => ErrorCode.Unknown
    };
}
