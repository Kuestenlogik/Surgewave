using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Schema;

/// <summary>
/// Wire format for schema information (GetSchemaById, GetSchemaByVersion responses).
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct SchemaPayload
{
    public int Id { get; init; }
    public string Subject { get; init; }
    public int Version { get; init; }
    public byte SchemaType { get; init; }
    public string SchemaString { get; init; }
    public IReadOnlyList<SchemaReferencePayload>? References { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static SchemaPayload Read(ref SurgewavePayloadReader reader)
    {
        var id = reader.ReadInt32();
        var subject = reader.ReadString() ?? string.Empty;
        var version = reader.ReadInt32();
        var schemaType = reader.ReadUInt8();
        var schemaString = reader.ReadString() ?? string.Empty;

        // Read references
        var refCount = reader.ReadInt32();
        IReadOnlyList<SchemaReferencePayload>? references = null;
        if (refCount > 0)
        {
            var refList = new SchemaReferencePayload[refCount];
            for (int i = 0; i < refCount; i++)
            {
                refList[i] = SchemaReferencePayload.Read(ref reader);
            }
            references = refList;
        }

        return new SchemaPayload
        {
            Id = id,
            Subject = subject,
            Version = version,
            SchemaType = schemaType,
            SchemaString = schemaString,
            References = references
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Id);
        writer.WriteString(Subject);
        writer.WriteInt32(Version);
        writer.WriteUInt8(SchemaType);
        writer.WriteString(SchemaString);

        // Write references
        if (References != null && References.Count > 0)
        {
            writer.WriteInt32(References.Count);
            foreach (var reference in References)
            {
                reference.Write(ref writer);
            }
        }
        else
        {
            writer.WriteInt32(0);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Id);
        writer.WriteString(Subject);
        writer.WriteInt32(Version);
        writer.WriteUInt8(SchemaType);
        writer.WriteString(SchemaString);

        // Write references
        if (References != null && References.Count > 0)
        {
            writer.WriteInt32(References.Count);
            foreach (var reference in References)
            {
                reference.WriteTo(writer);
            }
        }
        else
        {
            writer.WriteInt32(0);
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        int size =
            4 +                                                             // Id
            2 + System.Text.Encoding.UTF8.GetByteCount(Subject ?? "") +     // Subject (length prefix + bytes)
            4 +                                                             // Version
            1 +                                                             // SchemaType
            2 + System.Text.Encoding.UTF8.GetByteCount(SchemaString ?? "") +// SchemaString (length prefix + bytes)
            4;                                                              // Reference count

        // Add reference sizes
        if (References != null && References.Count > 0)
        {
            foreach (var reference in References)
            {
                size += reference.EstimateSize();
            }
        }

        return size;
    }
}

/// <summary>
/// Wire format for schema reference (for PROTOBUF imports).
/// </summary>
public readonly record struct SchemaReferencePayload
{
    public string Name { get; init; }
    public string Subject { get; init; }
    public int Version { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static SchemaReferencePayload Read(ref SurgewavePayloadReader reader)
    {
        return new SchemaReferencePayload
        {
            Name = reader.ReadString() ?? string.Empty,
            Subject = reader.ReadString() ?? string.Empty,
            Version = reader.ReadInt32()
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteString(Subject);
        writer.WriteInt32(Version);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteString(Subject);
        writer.WriteInt32(Version);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Name ?? "") +      // Name (length prefix + bytes)
        2 + System.Text.Encoding.UTF8.GetByteCount(Subject ?? "") +   // Subject (length prefix + bytes)
        4;                                                            // Version
}
