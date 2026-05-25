using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Schema;
using Kuestenlogik.Surgewave.Schema.Registry;
using SchemaRegistry = Kuestenlogik.Surgewave.Schema.Registry;

namespace Kuestenlogik.Surgewave.Broker.Native.Operations.Schema;

#region List Subjects

public readonly record struct ListSubjectsRequest
{
    public required bool IncludeDeleted { get; init; }
}

public readonly record struct ListSubjectsResult
{
    public required IReadOnlyList<string> Subjects { get; init; }
}

public sealed class ListSubjectsOperation : IOperationHandler<ListSubjectsRequest, ListSubjectsResult>
{
    private readonly SchemaStore _schemaStore;

    public ListSubjectsOperation(SchemaStore schemaStore) => _schemaStore = schemaStore;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListSubjects;

    public ListSubjectsRequest ParseRequest(ref SurgewavePayloadReader reader)
        => new() { IncludeDeleted = reader.Remaining > 0 && reader.ReadUInt8() == 1 };

    public void ValidateRequest(in ListSubjectsRequest request) { }

    public Task<ListSubjectsResult> ExecuteAsync(ListSubjectsRequest request, CancellationToken cancellationToken)
    {
        var subjects = _schemaStore.GetSubjects(request.IncludeDeleted);
        return Task.FromResult(new ListSubjectsResult { Subjects = subjects });
    }

    public void WriteResponse(IPayloadWriter writer, in ListSubjectsResult response)
    {
        writer.WriteInt32(response.Subjects.Count);
        foreach (var subject in response.Subjects)
            writer.WriteString(subject);
    }
}

#endregion

#region Get Subject Versions

public readonly record struct GetSubjectVersionsRequest
{
    public required string Subject { get; init; }
    public required bool IncludeDeleted { get; init; }
}

public readonly record struct GetSubjectVersionsResult
{
    public required IReadOnlyList<int> Versions { get; init; }
}

public sealed class GetSubjectVersionsOperation : IOperationHandler<GetSubjectVersionsRequest, GetSubjectVersionsResult>
{
    private readonly SchemaStore _schemaStore;

    public GetSubjectVersionsOperation(SchemaStore schemaStore) => _schemaStore = schemaStore;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetSubjectVersions;

    public GetSubjectVersionsRequest ParseRequest(ref SurgewavePayloadReader reader)
        => new()
        {
            Subject = reader.ReadString() ?? string.Empty,
            IncludeDeleted = reader.Remaining > 0 && reader.ReadUInt8() == 1
        };

    public void ValidateRequest(in GetSubjectVersionsRequest request) { }

    public Task<GetSubjectVersionsResult> ExecuteAsync(GetSubjectVersionsRequest request, CancellationToken cancellationToken)
    {
        var versions = _schemaStore.GetVersions(request.Subject, request.IncludeDeleted);

        if (versions.Count == 0 && _schemaStore.GetLatestSchema(request.Subject) == null)
            throw new SurgewaveOperationException(SurgewaveErrorCode.SubjectNotFound, $"Subject '{request.Subject}' not found");

        return Task.FromResult(new GetSubjectVersionsResult { Versions = versions });
    }

    public void WriteResponse(IPayloadWriter writer, in GetSubjectVersionsResult response)
    {
        writer.WriteInt32(response.Versions.Count);
        foreach (var version in response.Versions)
            writer.WriteInt32(version);
    }
}

#endregion

#region Register Schema

public readonly record struct RegisterSchemaRequest
{
    public required string Subject { get; init; }
    public required string SchemaTypeString { get; init; }
    public required string SchemaString { get; init; }
}

public readonly record struct RegisterSchemaResult
{
    public required SchemaRegistrationPayload Response { get; init; }
}

public sealed class RegisterSchemaOperation : IOperationHandler<RegisterSchemaRequest, RegisterSchemaResult>
{
    private readonly SchemaStore _schemaStore;
    private readonly CompatibilityChecker _compatibilityChecker;

    public RegisterSchemaOperation(SchemaStore schemaStore, CompatibilityChecker compatibilityChecker)
    {
        _schemaStore = schemaStore;
        _compatibilityChecker = compatibilityChecker;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.RegisterSchema;

    public RegisterSchemaRequest ParseRequest(ref SurgewavePayloadReader reader)
        => new()
        {
            Subject = reader.ReadString() ?? string.Empty,
            SchemaTypeString = reader.ReadString() ?? "AVRO",
            SchemaString = reader.ReadString() ?? string.Empty
        };

    public void ValidateRequest(in RegisterSchemaRequest request) { }

    public Task<RegisterSchemaResult> ExecuteAsync(RegisterSchemaRequest request, CancellationToken cancellationToken)
    {
        if (!Enum.TryParse<SchemaType>(request.SchemaTypeString, true, out var schemaType))
            schemaType = SchemaType.Avro;

        var (isValid, error) = _compatibilityChecker.ValidateSchema(request.SchemaString, schemaType);
        if (!isValid)
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidSchema, error ?? "Invalid schema");

        var compatibility = _schemaStore.GetCompatibility(request.Subject);
        var existingSchemas = _schemaStore.GetSchemasForCompatibilityCheck(request.Subject, compatibility);
        if (existingSchemas.Count > 0)
        {
            var compatResult = _compatibilityChecker.CheckCompatibility(request.SchemaString, schemaType, existingSchemas, compatibility);
            if (!compatResult.IsCompatible)
            {
                var message = compatResult.Messages != null && compatResult.Messages.Count > 0
                    ? string.Join("; ", compatResult.Messages)
                    : "Schema is incompatible with existing version";
                throw new SurgewaveOperationException(SurgewaveErrorCode.IncompatibleSchema, message);
            }
        }

        var schema = _schemaStore.RegisterSchema(request.Subject, request.SchemaString, schemaType);

        return Task.FromResult(new RegisterSchemaResult
        {
            Response = new SchemaRegistrationPayload { SchemaId = schema.Id, Version = schema.Version }
        });
    }

    public void WriteResponse(IPayloadWriter writer, in RegisterSchemaResult response)
        => response.Response.WriteTo(writer);
}

#endregion

#region Get Schema By Id

public readonly record struct GetSchemaByIdRequest
{
    public required int SchemaId { get; init; }
}

public readonly record struct GetSchemaByIdResult
{
    public required SchemaPayload Response { get; init; }
}

public sealed class GetSchemaByIdOperation : IOperationHandler<GetSchemaByIdRequest, GetSchemaByIdResult>
{
    private readonly SchemaStore _schemaStore;

    public GetSchemaByIdOperation(SchemaStore schemaStore) => _schemaStore = schemaStore;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetSchemaById;

    public GetSchemaByIdRequest ParseRequest(ref SurgewavePayloadReader reader)
        => new() { SchemaId = reader.ReadInt32() };

    public void ValidateRequest(in GetSchemaByIdRequest request) { }

    public Task<GetSchemaByIdResult> ExecuteAsync(GetSchemaByIdRequest request, CancellationToken cancellationToken)
    {
        var schema = _schemaStore.GetSchemaById(request.SchemaId);
        if (schema == null)
            throw new SurgewaveOperationException(SurgewaveErrorCode.SchemaNotFound, $"Schema with ID {request.SchemaId} not found");

        return Task.FromResult(new GetSchemaByIdResult { Response = ConvertToPayload(schema) });
    }

    public void WriteResponse(IPayloadWriter writer, in GetSchemaByIdResult response)
        => response.Response.WriteTo(writer);

    private static SchemaPayload ConvertToPayload(SchemaRegistry.Schema schema)
    {
        IReadOnlyList<SchemaReferencePayload>? references = null;
        if (schema.References != null && schema.References.Count > 0)
        {
            var refList = new SchemaReferencePayload[schema.References.Count];
            for (int i = 0; i < schema.References.Count; i++)
            {
                var reference = schema.References[i];
                refList[i] = new SchemaReferencePayload
                {
                    Name = reference.Name,
                    Subject = reference.Subject,
                    Version = reference.Version
                };
            }
            references = refList;
        }

        return new SchemaPayload
        {
            Id = schema.Id,
            Subject = schema.Subject,
            Version = schema.Version,
            SchemaType = (byte)schema.SchemaType,
            SchemaString = schema.SchemaString,
            References = references
        };
    }
}

#endregion

#region Get Schema By Version

public readonly record struct GetSchemaByVersionRequest
{
    public required string Subject { get; init; }
    public required int Version { get; init; }
}

public readonly record struct GetSchemaByVersionResult
{
    public required SchemaPayload Response { get; init; }
}

public sealed class GetSchemaByVersionOperation : IOperationHandler<GetSchemaByVersionRequest, GetSchemaByVersionResult>
{
    private readonly SchemaStore _schemaStore;

    public GetSchemaByVersionOperation(SchemaStore schemaStore) => _schemaStore = schemaStore;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetSchemaByVersion;

    public GetSchemaByVersionRequest ParseRequest(ref SurgewavePayloadReader reader)
        => new()
        {
            Subject = reader.ReadString() ?? string.Empty,
            Version = reader.ReadInt32()
        };

    public void ValidateRequest(in GetSchemaByVersionRequest request) { }

    public Task<GetSchemaByVersionResult> ExecuteAsync(GetSchemaByVersionRequest request, CancellationToken cancellationToken)
    {
        SchemaRegistry.Schema? schema = request.Version == -1
            ? _schemaStore.GetLatestSchema(request.Subject)
            : _schemaStore.GetSchema(request.Subject, request.Version);

        if (schema == null)
        {
            var errorCode = request.Version == -1 ? SurgewaveErrorCode.SubjectNotFound : SurgewaveErrorCode.VersionNotFound;
            var message = request.Version == -1
                ? $"Subject '{request.Subject}' not found"
                : $"Version {request.Version} not found for subject '{request.Subject}'";
            throw new SurgewaveOperationException(errorCode, message);
        }

        return Task.FromResult(new GetSchemaByVersionResult { Response = ConvertToPayload(schema) });
    }

    public void WriteResponse(IPayloadWriter writer, in GetSchemaByVersionResult response)
        => response.Response.WriteTo(writer);

    private static SchemaPayload ConvertToPayload(SchemaRegistry.Schema schema)
    {
        IReadOnlyList<SchemaReferencePayload>? references = null;
        if (schema.References != null && schema.References.Count > 0)
        {
            var refList = new SchemaReferencePayload[schema.References.Count];
            for (int i = 0; i < schema.References.Count; i++)
            {
                var reference = schema.References[i];
                refList[i] = new SchemaReferencePayload
                {
                    Name = reference.Name,
                    Subject = reference.Subject,
                    Version = reference.Version
                };
            }
            references = refList;
        }

        return new SchemaPayload
        {
            Id = schema.Id,
            Subject = schema.Subject,
            Version = schema.Version,
            SchemaType = (byte)schema.SchemaType,
            SchemaString = schema.SchemaString,
            References = references
        };
    }
}

#endregion

#region Delete Subject

public readonly record struct DeleteSubjectRequest
{
    public required string Subject { get; init; }
    public required bool Permanent { get; init; }
}

public readonly record struct DeleteSubjectResult
{
    public required IReadOnlyList<int> DeletedVersions { get; init; }
}

public sealed class DeleteSubjectOperation : IOperationHandler<DeleteSubjectRequest, DeleteSubjectResult>
{
    private readonly SchemaStore _schemaStore;

    public DeleteSubjectOperation(SchemaStore schemaStore) => _schemaStore = schemaStore;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DeleteSubject;

    public DeleteSubjectRequest ParseRequest(ref SurgewavePayloadReader reader)
        => new()
        {
            Subject = reader.ReadString() ?? string.Empty,
            Permanent = reader.Remaining > 0 && reader.ReadUInt8() == 1
        };

    public void ValidateRequest(in DeleteSubjectRequest request) { }

    public Task<DeleteSubjectResult> ExecuteAsync(DeleteSubjectRequest request, CancellationToken cancellationToken)
    {
        var deletedVersions = _schemaStore.DeleteSubject(request.Subject, request.Permanent);

        if (deletedVersions.Count == 0)
            throw new SurgewaveOperationException(SurgewaveErrorCode.SubjectNotFound, $"Subject '{request.Subject}' not found");

        return Task.FromResult(new DeleteSubjectResult { DeletedVersions = deletedVersions });
    }

    public void WriteResponse(IPayloadWriter writer, in DeleteSubjectResult response)
    {
        writer.WriteInt32(response.DeletedVersions.Count);
        foreach (var version in response.DeletedVersions)
            writer.WriteInt32(version);
    }
}

#endregion

#region Delete Schema Version

public readonly record struct DeleteSchemaVersionRequest
{
    public required string Subject { get; init; }
    public required int Version { get; init; }
    public required bool Permanent { get; init; }
}

public readonly record struct DeleteSchemaVersionResult
{
    public required int DeletedVersion { get; init; }
}

public sealed class DeleteSchemaVersionOperation : IOperationHandler<DeleteSchemaVersionRequest, DeleteSchemaVersionResult>
{
    private readonly SchemaStore _schemaStore;

    public DeleteSchemaVersionOperation(SchemaStore schemaStore) => _schemaStore = schemaStore;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DeleteSchemaVersion;

    public DeleteSchemaVersionRequest ParseRequest(ref SurgewavePayloadReader reader)
        => new()
        {
            Subject = reader.ReadString() ?? string.Empty,
            Version = reader.ReadInt32(),
            Permanent = reader.Remaining > 0 && reader.ReadUInt8() == 1
        };

    public void ValidateRequest(in DeleteSchemaVersionRequest request) { }

    public Task<DeleteSchemaVersionResult> ExecuteAsync(DeleteSchemaVersionRequest request, CancellationToken cancellationToken)
    {
        var deletedVersion = _schemaStore.DeleteVersion(request.Subject, request.Version, request.Permanent);

        if (deletedVersion == null)
            throw new SurgewaveOperationException(SurgewaveErrorCode.VersionNotFound,
                $"Version {request.Version} not found for subject '{request.Subject}'");

        return Task.FromResult(new DeleteSchemaVersionResult { DeletedVersion = deletedVersion.Value });
    }

    public void WriteResponse(IPayloadWriter writer, in DeleteSchemaVersionResult response)
        => writer.WriteInt32(response.DeletedVersion);
}

#endregion

#region Check Compatibility

public readonly record struct CheckCompatibilityRequest
{
    public required string Subject { get; init; }
    public required string SchemaTypeString { get; init; }
    public required string SchemaString { get; init; }
    public required int VersionToCheck { get; init; }
}

public readonly record struct CheckCompatibilityResult
{
    public required CompatibilityResultPayload Response { get; init; }
}

public sealed class CheckCompatibilityOperation : IOperationHandler<CheckCompatibilityRequest, CheckCompatibilityResult>
{
    private readonly SchemaStore _schemaStore;
    private readonly CompatibilityChecker _compatibilityChecker;

    public CheckCompatibilityOperation(SchemaStore schemaStore, CompatibilityChecker compatibilityChecker)
    {
        _schemaStore = schemaStore;
        _compatibilityChecker = compatibilityChecker;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CheckCompatibility;

    public CheckCompatibilityRequest ParseRequest(ref SurgewavePayloadReader reader)
        => new()
        {
            Subject = reader.ReadString() ?? string.Empty,
            SchemaTypeString = reader.ReadString() ?? "AVRO",
            SchemaString = reader.ReadString() ?? string.Empty,
            VersionToCheck = reader.Remaining >= 4 ? reader.ReadInt32() : -1
        };

    public void ValidateRequest(in CheckCompatibilityRequest request) { }

    public Task<CheckCompatibilityResult> ExecuteAsync(CheckCompatibilityRequest request, CancellationToken cancellationToken)
    {
        if (!Enum.TryParse<SchemaType>(request.SchemaTypeString, true, out var schemaType))
            schemaType = SchemaType.Avro;

        var compatibility = _schemaStore.GetCompatibility(request.Subject);

        IReadOnlyList<SchemaRegistry.Schema> existingSchemas;
        if (request.VersionToCheck > 0)
        {
            var specific = _schemaStore.GetSchema(request.Subject, request.VersionToCheck);
            existingSchemas = specific != null ? [specific] : [];
        }
        else
        {
            existingSchemas = _schemaStore.GetSchemasForCompatibilityCheck(request.Subject, compatibility);
        }

        var result = _compatibilityChecker.CheckCompatibility(request.SchemaString, schemaType, existingSchemas, compatibility);

        return Task.FromResult(new CheckCompatibilityResult
        {
            Response = new CompatibilityResultPayload { IsCompatible = result.IsCompatible, Messages = result.Messages }
        });
    }

    public void WriteResponse(IPayloadWriter writer, in CheckCompatibilityResult response)
        => response.Response.WriteTo(writer);
}

#endregion

#region Get Compatibility Config

public readonly record struct GetCompatibilityConfigRequest
{
    public required string? Subject { get; init; }
}

public readonly record struct GetCompatibilityConfigResult
{
    public required string Compatibility { get; init; }
    public required bool IsGlobal { get; init; }
}

public sealed class GetCompatibilityConfigOperation : IOperationHandler<GetCompatibilityConfigRequest, GetCompatibilityConfigResult>
{
    private readonly SchemaStore _schemaStore;

    public GetCompatibilityConfigOperation(SchemaStore schemaStore) => _schemaStore = schemaStore;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetCompatibilityConfig;

    public GetCompatibilityConfigRequest ParseRequest(ref SurgewavePayloadReader reader)
        => new() { Subject = reader.Remaining > 0 ? reader.ReadString() : null };

    public void ValidateRequest(in GetCompatibilityConfigRequest request) { }

    public Task<GetCompatibilityConfigResult> ExecuteAsync(GetCompatibilityConfigRequest request, CancellationToken cancellationToken)
    {
        CompatibilityMode compatibility;
        bool isGlobal;

        if (string.IsNullOrEmpty(request.Subject))
        {
            compatibility = _schemaStore.GlobalCompatibility;
            isGlobal = true;
        }
        else
        {
            compatibility = _schemaStore.GetCompatibility(request.Subject);
            isGlobal = false;
        }

        return Task.FromResult(new GetCompatibilityConfigResult
        {
            Compatibility = compatibility.ToString().ToUpperInvariant(),
            IsGlobal = isGlobal
        });
    }

    public void WriteResponse(IPayloadWriter writer, in GetCompatibilityConfigResult response)
    {
        writer.WriteString(response.Compatibility);
        writer.WriteUInt8(response.IsGlobal ? (byte)1 : (byte)0);
    }
}

#endregion

#region Set Compatibility Config

public readonly record struct SetCompatibilityConfigRequest
{
    public required string? Subject { get; init; }
    public required string CompatibilityString { get; init; }
}

public readonly record struct SetCompatibilityConfigResult
{
    public required string Compatibility { get; init; }
}

public sealed class SetCompatibilityConfigOperation : IOperationHandler<SetCompatibilityConfigRequest, SetCompatibilityConfigResult>
{
    private readonly SchemaStore _schemaStore;

    public SetCompatibilityConfigOperation(SchemaStore schemaStore) => _schemaStore = schemaStore;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.SetCompatibilityConfig;

    public SetCompatibilityConfigRequest ParseRequest(ref SurgewavePayloadReader reader)
        => new()
        {
            Subject = reader.ReadString(),
            CompatibilityString = reader.ReadString() ?? "BACKWARD"
        };

    public void ValidateRequest(in SetCompatibilityConfigRequest request) { }

    public Task<SetCompatibilityConfigResult> ExecuteAsync(SetCompatibilityConfigRequest request, CancellationToken cancellationToken)
    {
        if (!Enum.TryParse<CompatibilityMode>(request.CompatibilityString, true, out var compatibility))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidConfig, $"Invalid compatibility mode: {request.CompatibilityString}");

        if (string.IsNullOrEmpty(request.Subject))
            _schemaStore.GlobalCompatibility = compatibility;
        else
            _schemaStore.SetCompatibility(request.Subject, compatibility);

        return Task.FromResult(new SetCompatibilityConfigResult { Compatibility = compatibility.ToString().ToUpperInvariant() });
    }

    public void WriteResponse(IPayloadWriter writer, in SetCompatibilityConfigResult response)
        => writer.WriteString(response.Compatibility);
}

#endregion

#region Get Schema Types

public readonly record struct GetSchemaTypesResult
{
    public required IReadOnlyList<string> Types { get; init; }
}

public sealed class GetSchemaTypesOperation : INoRequestOperationHandler<GetSchemaTypesResult>
{
    private readonly CompatibilityChecker _compatibilityChecker;

    public GetSchemaTypesOperation(CompatibilityChecker compatibilityChecker) => _compatibilityChecker = compatibilityChecker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetSchemaTypes;

    public Task<GetSchemaTypesResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        var types = _compatibilityChecker.GetSupportedTypes().ToList();
        return Task.FromResult(new GetSchemaTypesResult { Types = types });
    }

    public void WriteResponse(IPayloadWriter writer, in GetSchemaTypesResult response)
    {
        writer.WriteInt32(response.Types.Count);
        foreach (var type in response.Types)
            writer.WriteString(type);
    }
}

#endregion
