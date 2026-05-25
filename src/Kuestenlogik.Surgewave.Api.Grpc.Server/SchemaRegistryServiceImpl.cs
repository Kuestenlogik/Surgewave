using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Schema info DTO.
/// </summary>
public record SchemaDto(
    int Id,
    int Version,
    string Subject,
    string SchemaString,
    int SchemaType,
    List<SchemaReferenceDto>? References);

/// <summary>
/// Schema reference DTO.
/// </summary>
public record SchemaReferenceDto(string Name, string Subject, int Version);

/// <summary>
/// Compatibility check result DTO.
/// </summary>
public record CompatibilityResultDto(bool IsCompatible, List<string>? Messages);

/// <summary>
/// Delegate to list subjects.
/// </summary>
public delegate List<string> ListSubjectsDelegate(bool includeDeleted);

/// <summary>
/// Delegate to get subject versions.
/// </summary>
public delegate List<int> GetSubjectVersionsDelegate(string subject, bool includeDeleted);

/// <summary>
/// Delegate to register a schema.
/// </summary>
public delegate SchemaDto RegisterSchemaDelegate(string subject, string schema, int schemaType, List<SchemaReferenceDto>? references);

/// <summary>
/// Delegate to get schema by ID.
/// </summary>
public delegate SchemaDto? GetSchemaByIdDelegate(int id);

/// <summary>
/// Delegate to get schema by version.
/// </summary>
public delegate SchemaDto? GetSchemaByVersionDelegate(string subject, int version);

/// <summary>
/// Delegate to delete a subject.
/// </summary>
public delegate List<int> DeleteSubjectDelegate(string subject, bool permanent);

/// <summary>
/// Delegate to delete a schema version.
/// </summary>
public delegate int? DeleteSchemaVersionDelegate(string subject, int version, bool permanent);

/// <summary>
/// Delegate to check compatibility.
/// </summary>
public delegate CompatibilityResultDto CheckCompatibilityDelegate(string subject, string schema, int schemaType, int? version, List<SchemaReferenceDto>? references);

/// <summary>
/// Delegate to get compatibility config.
/// </summary>
public delegate int GetCompatibilityConfigDelegate(string subject);

/// <summary>
/// Delegate to set compatibility config.
/// </summary>
public delegate int SetCompatibilityConfigDelegate(string subject, int level);

/// <summary>
/// Delegate to get supported schema types.
/// </summary>
public delegate List<string> GetSchemaTypesDelegate();

/// <summary>
/// gRPC SchemaRegistryService implementation.
/// </summary>
public class SchemaRegistryServiceImpl : SchemaRegistryService.SchemaRegistryServiceBase
{
    private readonly ListSubjectsDelegate _listSubjects;
    private readonly GetSubjectVersionsDelegate _getSubjectVersions;
    private readonly RegisterSchemaDelegate _registerSchema;
    private readonly GetSchemaByIdDelegate _getSchemaById;
    private readonly GetSchemaByVersionDelegate _getSchemaByVersion;
    private readonly DeleteSubjectDelegate _deleteSubject;
    private readonly DeleteSchemaVersionDelegate _deleteSchemaVersion;
    private readonly CheckCompatibilityDelegate _checkCompatibility;
    private readonly GetCompatibilityConfigDelegate _getCompatibilityConfig;
    private readonly SetCompatibilityConfigDelegate _setCompatibilityConfig;
    private readonly GetSchemaTypesDelegate _getSchemaTypes;

    public SchemaRegistryServiceImpl(
        ListSubjectsDelegate listSubjects,
        GetSubjectVersionsDelegate getSubjectVersions,
        RegisterSchemaDelegate registerSchema,
        GetSchemaByIdDelegate getSchemaById,
        GetSchemaByVersionDelegate getSchemaByVersion,
        DeleteSubjectDelegate deleteSubject,
        DeleteSchemaVersionDelegate deleteSchemaVersion,
        CheckCompatibilityDelegate checkCompatibility,
        GetCompatibilityConfigDelegate getCompatibilityConfig,
        SetCompatibilityConfigDelegate setCompatibilityConfig,
        GetSchemaTypesDelegate getSchemaTypes)
    {
        _listSubjects = listSubjects;
        _getSubjectVersions = getSubjectVersions;
        _registerSchema = registerSchema;
        _getSchemaById = getSchemaById;
        _getSchemaByVersion = getSchemaByVersion;
        _deleteSubject = deleteSubject;
        _deleteSchemaVersion = deleteSchemaVersion;
        _checkCompatibility = checkCompatibility;
        _getCompatibilityConfig = getCompatibilityConfig;
        _setCompatibilityConfig = setCompatibilityConfig;
        _getSchemaTypes = getSchemaTypes;
    }

    public override Task<ListSubjectsResponse> ListSubjects(ListSubjectsRequest request, ServerCallContext context)
    {
        var subjects = _listSubjects(request.IncludeDeleted);

        var response = new ListSubjectsResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };
        response.Subjects.AddRange(subjects);

        return Task.FromResult(response);
    }

    public override Task<GetSubjectVersionsResponse> GetSubjectVersions(GetSubjectVersionsRequest request, ServerCallContext context)
    {
        var versions = _getSubjectVersions(request.Subject, request.IncludeDeleted);

        var response = new GetSubjectVersionsResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };
        response.Versions.AddRange(versions);

        return Task.FromResult(response);
    }

    public override Task<RegisterSchemaResponse> RegisterSchema(RegisterSchemaRequest request, ServerCallContext context)
    {
        var references = request.References.Count > 0
            ? request.References.Select(r => new SchemaReferenceDto(r.Name, r.Subject, r.Version)).ToList()
            : null;

        var schema = _registerSchema(
            request.Subject,
            request.Schema,
            (int)request.SchemaType,
            references);

        return Task.FromResult(new RegisterSchemaResponse
        {
            Id = schema.Id,
            Version = schema.Version,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        });
    }

    public override Task<GetSchemaByIdResponse> GetSchemaById(GetSchemaByIdRequest request, ServerCallContext context)
    {
        var schema = _getSchemaById(request.Id);

        if (schema == null)
        {
            return Task.FromResult(new GetSchemaByIdResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = $"Schema with ID {request.Id} not found"
                }
            });
        }

        var response = new GetSchemaByIdResponse
        {
            Schema = schema.SchemaString,
            SchemaType = (SchemaType)schema.SchemaType,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        if (schema.References != null)
        {
            foreach (var reference in schema.References)
            {
                response.References.Add(new SchemaReference
                {
                    Name = reference.Name,
                    Subject = reference.Subject,
                    Version = reference.Version
                });
            }
        }

        return Task.FromResult(response);
    }

    public override Task<GetSchemaByVersionResponse> GetSchemaByVersion(GetSchemaByVersionRequest request, ServerCallContext context)
    {
        var schema = _getSchemaByVersion(request.Subject, request.Version);

        if (schema == null)
        {
            return Task.FromResult(new GetSchemaByVersionResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = $"Schema for subject '{request.Subject}' version {request.Version} not found"
                }
            });
        }

        var response = new GetSchemaByVersionResponse
        {
            Id = schema.Id,
            Version = schema.Version,
            Subject = schema.Subject,
            Schema = schema.SchemaString,
            SchemaType = (SchemaType)schema.SchemaType,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        if (schema.References != null)
        {
            foreach (var reference in schema.References)
            {
                response.References.Add(new SchemaReference
                {
                    Name = reference.Name,
                    Subject = reference.Subject,
                    Version = reference.Version
                });
            }
        }

        return Task.FromResult(response);
    }

    public override Task<DeleteSubjectResponse> DeleteSubject(DeleteSubjectRequest request, ServerCallContext context)
    {
        var deletedVersions = _deleteSubject(request.Subject, request.Permanent);

        var response = new DeleteSubjectResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };
        response.DeletedVersions.AddRange(deletedVersions);

        return Task.FromResult(response);
    }

    public override Task<DeleteSchemaVersionResponse> DeleteSchemaVersion(DeleteSchemaVersionRequest request, ServerCallContext context)
    {
        var deletedVersion = _deleteSchemaVersion(request.Subject, request.Version, request.Permanent);

        if (deletedVersion == null)
        {
            return Task.FromResult(new DeleteSchemaVersionResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = $"Schema version {request.Version} not found for subject '{request.Subject}'"
                }
            });
        }

        return Task.FromResult(new DeleteSchemaVersionResponse
        {
            Version = deletedVersion.Value,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        });
    }

    public override Task<CheckCompatibilityResponse> CheckCompatibility(CheckCompatibilityRequest request, ServerCallContext context)
    {
        var references = request.References.Count > 0
            ? request.References.Select(r => new SchemaReferenceDto(r.Name, r.Subject, r.Version)).ToList()
            : null;

        var result = _checkCompatibility(
            request.Subject,
            request.Schema,
            (int)request.SchemaType,
            request.Version > 0 ? request.Version : null,
            references);

        var response = new CheckCompatibilityResponse
        {
            IsCompatible = result.IsCompatible,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        if (result.Messages != null)
        {
            response.Messages.AddRange(result.Messages);
        }

        return Task.FromResult(response);
    }

    public override Task<GetCompatibilityConfigResponse> GetCompatibilityConfig(GetCompatibilityConfigRequest request, ServerCallContext context)
    {
        var level = _getCompatibilityConfig(request.Subject);

        return Task.FromResult(new GetCompatibilityConfigResponse
        {
            Level = (CompatibilityLevel)level,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        });
    }

    public override Task<SetCompatibilityConfigResponse> SetCompatibilityConfig(SetCompatibilityConfigRequest request, ServerCallContext context)
    {
        var level = _setCompatibilityConfig(request.Subject, (int)request.Level);

        return Task.FromResult(new SetCompatibilityConfigResponse
        {
            Level = (CompatibilityLevel)level,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        });
    }

    public override Task<GetSchemaTypesResponse> GetSchemaTypes(GetSchemaTypesRequest request, ServerCallContext context)
    {
        var types = _getSchemaTypes();

        var response = new GetSchemaTypesResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };
        response.Types_.AddRange(types);

        return Task.FromResult(response);
    }
}
