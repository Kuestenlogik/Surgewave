namespace Kuestenlogik.Surgewave.Core.Serialization;

/// <summary>
/// Well-known content type identifiers for Surgewave messages.
/// Producers can set the "content-type" header to indicate message format.
/// </summary>
public static class ContentTypes
{
    /// <summary>JSON content type.</summary>
    public const string Json = "application/json";

    /// <summary>Protocol Buffers content type.</summary>
    public const string Protobuf = "application/x-protobuf";

    /// <summary>Apache Avro content type.</summary>
    public const string Avro = "application/avro";

    /// <summary>Hyperion binary serialization content type.</summary>
    public const string Hyperion = "application/x-hyperion";

    /// <summary>FlatBuffers content type.</summary>
    public const string FlatBuffers = "application/x-flatbuffers";

    /// <summary>MessagePack content type.</summary>
    public const string MessagePack = "application/x-msgpack";

    /// <summary>CBOR content type.</summary>
    public const string Cbor = "application/cbor";

    /// <summary>Bond content type.</summary>
    public const string Bond = "application/x-bond";

    /// <summary>Thrift content type.</summary>
    public const string Thrift = "application/x-thrift";

    /// <summary>MemoryPack content type.</summary>
    public const string MemoryPack = "application/x-memorypack";

    /// <summary>Cap'n Proto content type.</summary>
    public const string CapnProto = "application/x-capnproto";

    /// <summary>Microsoft Orleans content type.</summary>
    public const string Orleans = "application/x-orleans";

    /// <summary>Generic binary content type.</summary>
    public const string OctetStream = "application/octet-stream";

    /// <summary>Header key for content type on Surgewave/Kafka records.</summary>
    public const string HeaderKey = "content-type";
}
