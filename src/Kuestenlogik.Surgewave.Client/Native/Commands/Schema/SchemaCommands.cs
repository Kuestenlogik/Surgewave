using System.Text;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Schema;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Schema;

/// <summary>
/// Command to list all schema subjects.
/// </summary>
public sealed class ListSubjectsCommand : ISurgewaveCommand<IReadOnlyList<string>>
{
    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListSubjects;
    public void WriteRequest(ref SurgewavePayloadWriter writer) { }
    public int EstimateRequestSize() => 0;

    public IReadOnlyList<string> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"ListSubjects failed: {header.ErrorCode}");

        var count = reader.ReadInt32();
        var subjects = new List<string>(count);
        for (int i = 0; i < count; i++)
            subjects.Add(reader.ReadString() ?? string.Empty);
        return subjects;
    }
}

/// <summary>
/// Command to delete a subject.
/// </summary>
public sealed class DeleteSubjectCommand : ISurgewaveCommand<IReadOnlyList<int>>
{
    private readonly string _subject;
    private readonly bool _permanent;

    public DeleteSubjectCommand(string subject, bool permanent)
    {
        _subject = subject;
        _permanent = permanent;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DeleteSubject;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_subject);
        writer.WriteUInt8(_permanent ? (byte)1 : (byte)0);
    }

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_subject ?? "") + 1;

    public IReadOnlyList<int> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"DeleteSubject failed: {header.ErrorCode}");

        var count = reader.ReadInt32();
        var deletedVersions = new List<int>(count);
        for (int i = 0; i < count; i++)
            deletedVersions.Add(reader.ReadInt32());
        return deletedVersions;
    }
}

/// <summary>
/// Command to register a new schema.
/// </summary>
public sealed class RegisterSchemaCommand : ISurgewaveCommand<SchemaRegistrationResult>
{
    private readonly string _subject;
    private readonly string _schemaType;
    private readonly string _schemaString;

    public RegisterSchemaCommand(string subject, string schemaString, string schemaType)
    {
        _subject = subject;
        _schemaString = schemaString;
        _schemaType = schemaType;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.RegisterSchema;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_subject);
        writer.WriteString(_schemaType);
        writer.WriteString(_schemaString);
    }

    public int EstimateRequestSize() =>
        2 + Encoding.UTF8.GetByteCount(_subject ?? "") +
        2 + Encoding.UTF8.GetByteCount(_schemaType ?? "") +
        2 + Encoding.UTF8.GetByteCount(_schemaString ?? "");

    public SchemaRegistrationResult ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"RegisterSchema failed: {header.ErrorCode}");

        var registrationPayload = SchemaRegistrationPayload.Read(ref reader);
        return new SchemaRegistrationResult(registrationPayload.SchemaId, registrationPayload.Version);
    }
}

/// <summary>
/// Command to get schema by global ID.
/// </summary>
public sealed class GetSchemaByIdCommand : ISurgewaveCommand<SchemaInfo?>
{
    private readonly int _schemaId;

    public GetSchemaByIdCommand(int schemaId) => _schemaId = schemaId;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetSchemaById;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => writer.WriteInt32(_schemaId);

    public int EstimateRequestSize() => 4;

    public SchemaInfo? ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode == SurgewaveErrorCode.SchemaNotFound)
            return null;

        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"GetSchemaById failed: {header.ErrorCode}");

        var schemaPayload = SchemaPayload.Read(ref reader);
        return new SchemaInfo
        {
            Id = schemaPayload.Id,
            Subject = schemaPayload.Subject,
            Version = schemaPayload.Version,
            SchemaType = ConvertSchemaType(schemaPayload.SchemaType),
            SchemaString = schemaPayload.SchemaString
        };
    }

    private static string ConvertSchemaType(byte schemaType) => schemaType switch
    {
        0 => "AVRO", 1 => "JSON", 2 => "PROTOBUF", 3 => "FLATBUFFERS", _ => "AVRO"
    };
}

/// <summary>
/// Command to get schema by subject and version.
/// </summary>
public sealed class GetSchemaByVersionCommand : ISurgewaveCommand<SchemaInfo?>
{
    private readonly string _subject;
    private readonly int _version;

    public GetSchemaByVersionCommand(string subject, int version)
    {
        _subject = subject;
        _version = version;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetSchemaByVersion;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_subject);
        writer.WriteInt32(_version);
    }

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_subject ?? "") + 4;

    public SchemaInfo? ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode == SurgewaveErrorCode.SchemaNotFound || header.ErrorCode == SurgewaveErrorCode.SubjectNotFound)
            return null;

        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"GetSchemaByVersion failed: {header.ErrorCode}");

        var schemaPayload = SchemaPayload.Read(ref reader);
        return new SchemaInfo
        {
            Id = schemaPayload.Id,
            Subject = schemaPayload.Subject,
            Version = schemaPayload.Version,
            SchemaType = ConvertSchemaType(schemaPayload.SchemaType),
            SchemaString = schemaPayload.SchemaString
        };
    }

    private static string ConvertSchemaType(byte schemaType) => schemaType switch
    {
        0 => "AVRO", 1 => "JSON", 2 => "PROTOBUF", 3 => "FLATBUFFERS", _ => "AVRO"
    };
}

/// <summary>
/// Command to get versions for a subject.
/// </summary>
public sealed class GetSubjectVersionsCommand : ISurgewaveCommand<IReadOnlyList<int>>
{
    private readonly string _subject;

    public GetSubjectVersionsCommand(string subject) => _subject = subject;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetSubjectVersions;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => writer.WriteString(_subject);

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_subject ?? "");

    public IReadOnlyList<int> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"GetSubjectVersions failed: {header.ErrorCode}");

        var count = reader.ReadInt32();
        var versions = new List<int>(count);
        for (int i = 0; i < count; i++)
            versions.Add(reader.ReadInt32());
        return versions;
    }
}

/// <summary>
/// Command to check schema compatibility.
/// </summary>
public sealed class CheckCompatibilityCommand : ISurgewaveCommand<CompatibilityCheckResult>
{
    private readonly string _subject;
    private readonly string _schemaType;
    private readonly string _schemaString;
    private readonly int _version;

    public CheckCompatibilityCommand(string subject, string schemaString, string schemaType, int? version)
    {
        _subject = subject;
        _schemaString = schemaString;
        _schemaType = schemaType;
        _version = version ?? -1;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CheckCompatibility;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_subject);
        writer.WriteString(_schemaType);
        writer.WriteString(_schemaString);
        writer.WriteInt32(_version);
    }

    public int EstimateRequestSize() =>
        2 + Encoding.UTF8.GetByteCount(_subject ?? "") +
        2 + Encoding.UTF8.GetByteCount(_schemaType ?? "") +
        2 + Encoding.UTF8.GetByteCount(_schemaString ?? "") +
        4;

    public CompatibilityCheckResult ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"CheckCompatibility failed: {header.ErrorCode}");

        var compatibilityPayload = CompatibilityResultPayload.Read(ref reader);
        return new CompatibilityCheckResult(
            compatibilityPayload.IsCompatible,
            compatibilityPayload.Messages ?? []);
    }
}

/// <summary>
/// Command to get compatibility configuration.
/// </summary>
public sealed class GetCompatibilityConfigCommand : ISurgewaveCommand<string>
{
    private readonly string? _subject;

    public GetCompatibilityConfigCommand(string? subject) => _subject = subject;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetCompatibilityConfig;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => writer.WriteString(_subject);

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_subject ?? "");

    public string ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"GetCompatibilityConfig failed: {header.ErrorCode}");

        return reader.ReadString() ?? "BACKWARD";
    }
}

/// <summary>
/// Command to set compatibility configuration.
/// </summary>
public sealed class SetCompatibilityConfigCommand : ISurgewaveCommand<SurgewaveErrorCode>
{
    private readonly string? _subject;
    private readonly string _compatibility;

    public SetCompatibilityConfigCommand(string compatibility, string? subject)
    {
        _compatibility = compatibility;
        _subject = subject;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.SetCompatibilityConfig;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_subject);
        writer.WriteString(_compatibility);
    }

    public int EstimateRequestSize() =>
        2 + Encoding.UTF8.GetByteCount(_subject ?? "") +
        2 + Encoding.UTF8.GetByteCount(_compatibility ?? "");

    public SurgewaveErrorCode ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
        => header.ErrorCode;
}

/// <summary>
/// Command to get supported schema types.
/// </summary>
public sealed class GetSchemaTypesCommand : ISurgewaveCommand<IReadOnlyList<string>>
{
    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetSchemaTypes;
    public void WriteRequest(ref SurgewavePayloadWriter writer) { }
    public int EstimateRequestSize() => 0;

    public IReadOnlyList<string> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"GetSchemaTypes failed: {header.ErrorCode}");

        var count = reader.ReadInt32();
        var types = new List<string>(count);
        for (int i = 0; i < count; i++)
            types.Add(reader.ReadString() ?? string.Empty);
        return types;
    }
}
