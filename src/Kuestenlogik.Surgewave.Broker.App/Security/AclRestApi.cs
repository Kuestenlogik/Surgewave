namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// REST API endpoints for ACL management.
/// </summary>
public static class AclRestApi
{
    public static IEndpointRouteBuilder MapSurgewaveAcl(this IEndpointRouteBuilder app, AclAuthorizer authorizer)
    {
        var group = app.MapGroup("/admin/acls")
            .WithTags("ACL Management");

        group.MapGet("", () => ListAcls(authorizer, null, null, null))
            .WithName("ListAcls")
            .WithSummary("List all ACLs with optional filtering")
            .Produces<IReadOnlyList<AclEntryResponse>>();

        group.MapGet("/filter", (
            string? principal,
            AclResourceType? resourceType,
            string? resourceName,
            AclOperation? operation,
            AclPermission? permission) => ListAcls(authorizer, principal, resourceType, resourceName, operation, permission))
            .WithName("ListAclsFiltered")
            .WithSummary("List ACLs with filters")
            .Produces<IReadOnlyList<AclEntryResponse>>();

        group.MapPost("", (CreateAclRequest request) => CreateAcl(authorizer, request))
            .WithName("CreateAcl")
            .WithSummary("Create a new ACL entry")
            .Produces<AclEntryResponse>(StatusCodes.Status201Created)
            .ProducesValidationProblem();

        group.MapPost("/batch", (IReadOnlyList<CreateAclRequest> requests) => CreateAclsBatch(authorizer, requests))
            .WithName("CreateAclsBatch")
            .WithSummary("Create multiple ACL entries")
            .Produces<IReadOnlyList<AclEntryResponse>>(StatusCodes.Status201Created)
            .ProducesValidationProblem();

        group.MapDelete("", (
            string? principal,
            AclResourceType? resourceType,
            string? resourceName,
            AclOperation? operation,
            AclPermission? permission) => DeleteAcls(authorizer, principal, resourceType, resourceName, operation, permission))
            .WithName("DeleteAcls")
            .WithSummary("Delete ACLs matching the filter")
            .Produces<AclDeleteResult>();

        return app;
    }

    private static IResult ListAcls(
        AclAuthorizer authorizer,
        string? principal,
        AclResourceType? resourceType,
        string? resourceName,
        AclOperation? operation = null,
        AclPermission? permission = null)
    {
        var acls = authorizer.ListAcls(acl =>
        {
            if (principal != null && !acl.Principal.Equals(principal, StringComparison.OrdinalIgnoreCase))
                return false;
            if (resourceType.HasValue && acl.ResourceType != resourceType.Value)
                return false;
            if (resourceName != null && !acl.ResourceName.Equals(resourceName, StringComparison.OrdinalIgnoreCase))
                return false;
            if (operation.HasValue && acl.Operation != operation.Value)
                return false;
            if (permission.HasValue && acl.Permission != permission.Value)
                return false;
            return true;
        });

        var response = acls.Select(MapToResponse).ToList();
        return Results.Ok(response);
    }

    private static IResult CreateAcl(AclAuthorizer authorizer, CreateAclRequest request)
    {
        var acl = new AclEntry
        {
            Principal = request.Principal,
            Host = request.Host ?? "*",
            ResourceType = request.ResourceType,
            PatternType = request.PatternType ?? AclPatternType.Literal,
            ResourceName = request.ResourceName,
            Operation = request.Operation,
            Permission = request.Permission
        };

        var validationError = acl.Validate();
        if (validationError != null)
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["validation"] = [validationError]
            });
        }

        authorizer.AddAcl(acl);
        authorizer.SaveToFile();

        return Results.Created($"/admin/acls", MapToResponse(acl));
    }

    private static IResult CreateAclsBatch(AclAuthorizer authorizer, IReadOnlyList<CreateAclRequest> requests)
    {
        var acls = new List<AclEntry>();
        var errors = new Dictionary<string, string[]>();

        for (var i = 0; i < requests.Count; i++)
        {
            var request = requests[i];
            var acl = new AclEntry
            {
                Principal = request.Principal,
                Host = request.Host ?? "*",
                ResourceType = request.ResourceType,
                PatternType = request.PatternType ?? AclPatternType.Literal,
                ResourceName = request.ResourceName,
                Operation = request.Operation,
                Permission = request.Permission
            };

            var validationError = acl.Validate();
            if (validationError != null)
            {
                errors[$"[{i}]"] = [validationError];
            }
            else
            {
                acls.Add(acl);
            }
        }

        if (errors.Count > 0)
        {
            return Results.ValidationProblem(errors);
        }

        authorizer.AddAcls(acls);
        authorizer.SaveToFile();

        var response = acls.Select(MapToResponse).ToList();
        return Results.Created("/admin/acls", response);
    }

    private static IResult DeleteAcls(
        AclAuthorizer authorizer,
        string? principal,
        AclResourceType? resourceType,
        string? resourceName,
        AclOperation? operation,
        AclPermission? permission)
    {
        if (principal == null && resourceType == null && resourceName == null && operation == null && permission == null)
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["filter"] = ["At least one filter parameter is required to prevent accidental deletion of all ACLs"]
            });
        }

        var deletedCount = authorizer.RemoveAcls(acl =>
        {
            if (principal != null && !acl.Principal.Equals(principal, StringComparison.OrdinalIgnoreCase))
                return false;
            if (resourceType.HasValue && acl.ResourceType != resourceType.Value)
                return false;
            if (resourceName != null && !acl.ResourceName.Equals(resourceName, StringComparison.OrdinalIgnoreCase))
                return false;
            if (operation.HasValue && acl.Operation != operation.Value)
                return false;
            if (permission.HasValue && acl.Permission != permission.Value)
                return false;
            return true;
        });

        authorizer.SaveToFile();

        return Results.Ok(new AclDeleteResult(deletedCount));
    }

    private static AclEntryResponse MapToResponse(AclEntry acl) => new(
        acl.Principal,
        acl.Host,
        acl.ResourceType,
        acl.PatternType,
        acl.ResourceName,
        acl.Operation,
        acl.Permission);
}

/// <summary>
/// Request to create a new ACL entry.
/// </summary>
public sealed record CreateAclRequest(
    string Principal,
    AclResourceType ResourceType,
    string ResourceName,
    AclOperation Operation,
    AclPermission Permission,
    string? Host = "*",
    AclPatternType? PatternType = AclPatternType.Literal);

/// <summary>
/// Response representing an ACL entry.
/// </summary>
public sealed record AclEntryResponse(
    string Principal,
    string Host,
    AclResourceType ResourceType,
    AclPatternType PatternType,
    string ResourceName,
    AclOperation Operation,
    AclPermission Permission);

/// <summary>
/// Response for delete ACLs operation.
/// </summary>
public sealed record AclDeleteResult(int DeletedCount);
