using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry;

/// <summary>
/// Configuration for schema registry serializers.
/// </summary>
public class SchemaRegistrySerializerConfig
{
    /// <summary>
    /// The schema registry operations client.
    /// </summary>
    public ISchemaRegistryOperations SchemaRegistry { get; set; } = null!;

    /// <summary>
    /// The subject name strategy. Defaults to TopicName strategy.
    /// </summary>
    public ISubjectNameStrategy SubjectNameStrategy { get; set; } = TopicNameStrategy.Instance;

    /// <summary>
    /// Whether to automatically register schemas. Defaults to true.
    /// </summary>
    public bool AutoRegisterSchemas { get; set; } = true;

    /// <summary>
    /// Whether this is a key serializer (affects subject naming). Defaults to false (value serializer).
    /// </summary>
    public bool IsKey { get; set; }

    /// <summary>
    /// Schema cache timeout. Defaults to 5 minutes.
    /// </summary>
    public TimeSpan CacheTimeout { get; set; } = TimeSpan.FromMinutes(5);
}

/// <summary>
/// Wire format constants for Confluent-compatible schema registry serialization.
/// </summary>
public static class SchemaRegistryWireFormat
{
    /// <summary>
    /// Magic byte indicating Confluent schema registry wire format.
    /// </summary>
    public const byte MagicByte = 0x00;

    /// <summary>
    /// Size of the wire format header (1 magic byte + 4 schema ID bytes).
    /// </summary>
    public const int HeaderSize = 5;

    /// <summary>
    /// Write the wire format header to a buffer.
    /// </summary>
    public static void WriteHeader(Span<byte> buffer, int schemaId)
    {
        buffer[0] = MagicByte;
        // Write schema ID as big-endian
        buffer[1] = (byte)(schemaId >> 24);
        buffer[2] = (byte)(schemaId >> 16);
        buffer[3] = (byte)(schemaId >> 8);
        buffer[4] = (byte)schemaId;
    }

    /// <summary>
    /// Read the schema ID from a wire format header.
    /// </summary>
    public static int ReadSchemaId(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length < HeaderSize)
            throw new ArgumentException($"Buffer too small. Expected at least {HeaderSize} bytes, got {buffer.Length}");

        if (buffer[0] != MagicByte)
            throw new InvalidOperationException($"Invalid magic byte. Expected {MagicByte}, got {buffer[0]}");

        // Read schema ID as big-endian
        return (buffer[1] << 24) | (buffer[2] << 16) | (buffer[3] << 8) | buffer[4];
    }

    /// <summary>
    /// Get the payload portion of the buffer (after the header).
    /// </summary>
    public static ReadOnlySpan<byte> GetPayload(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length < HeaderSize)
            throw new ArgumentException($"Buffer too small. Expected at least {HeaderSize} bytes, got {buffer.Length}");

        return buffer[HeaderSize..];
    }
}
