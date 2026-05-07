namespace Kuestenlogik.Surgewave.Protocol.Native.Serialization;

/// <summary>
/// Supported field types for payload serialization.
/// </summary>
public enum FieldType
{
    Int8,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    String,
    NullableString,
    Bytes,
    Bool,
    Array,
    Nested
}
