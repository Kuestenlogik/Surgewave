using System.Text;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Serialization;

/// <summary>
/// Helper methods for common serialization operations.
/// Reduces duplication in payload implementations.
/// </summary>
public static class SerializationHelper
{
    /// <summary>
    /// Calculate size needed for a string field (2-byte length prefix + UTF8 bytes).
    /// </summary>
    public static int StringSize(string? value)
        => 2 + (value == null ? 0 : Encoding.UTF8.GetByteCount(value));

    /// <summary>
    /// Calculate size needed for a nullable string field.
    /// </summary>
    public static int NullableStringSize(string? value)
        => value == null ? 2 : 2 + Encoding.UTF8.GetByteCount(value);

    /// <summary>
    /// Calculate size needed for a bytes field (4-byte length prefix + bytes).
    /// </summary>
    public static int BytesSize(byte[]? value)
        => 4 + (value?.Length ?? 0);

    /// <summary>
    /// Calculate size needed for a bytes field with ReadOnlyMemory.
    /// </summary>
    public static int BytesSize(ReadOnlyMemory<byte> value)
        => 4 + value.Length;

    /// <summary>
    /// Calculate size needed for an array of serializable payloads.
    /// </summary>
    public static int ArraySize<T>(IReadOnlyList<T>? items) where T : ISerializablePayload<T>
    {
        if (items == null || items.Count == 0)
            return 4; // Just the count

        var size = 4; // Count prefix
        foreach (var item in items)
            size += item.EstimateSize();
        return size;
    }

    /// <summary>
    /// Calculate size needed for an array of strings.
    /// </summary>
    public static int StringArraySize(IReadOnlyList<string>? items)
    {
        if (items == null || items.Count == 0)
            return 4;

        var size = 4;
        foreach (var item in items)
            size += StringSize(item);
        return size;
    }

    /// <summary>
    /// Write an array of serializable payloads to ref struct writer.
    /// </summary>
    public static void WriteArray<T>(ref SurgewavePayloadWriter writer, IReadOnlyList<T>? items)
        where T : ISerializablePayload<T>
    {
        if (items == null || items.Count == 0)
        {
            writer.WriteInt32(0);
            return;
        }

        writer.WriteInt32(items.Count);
        foreach (var item in items)
            item.Write(ref writer);
    }

    /// <summary>
    /// Write an array of serializable payloads to interface writer.
    /// </summary>
    public static void WriteArrayTo<T>(IPayloadWriter writer, IReadOnlyList<T>? items)
        where T : ISerializablePayload<T>
    {
        if (items == null || items.Count == 0)
        {
            writer.WriteInt32(0);
            return;
        }

        writer.WriteInt32(items.Count);
        foreach (var item in items)
            item.WriteTo(writer);
    }

    /// <summary>
    /// Write an array of strings to ref struct writer.
    /// </summary>
    public static void WriteStringArray(ref SurgewavePayloadWriter writer, IReadOnlyList<string>? items)
    {
        if (items == null || items.Count == 0)
        {
            writer.WriteInt32(0);
            return;
        }

        writer.WriteInt32(items.Count);
        foreach (var item in items)
            writer.WriteString(item);
    }

    /// <summary>
    /// Write an array of strings to interface writer.
    /// </summary>
    public static void WriteStringArrayTo(IPayloadWriter writer, IReadOnlyList<string>? items)
    {
        if (items == null || items.Count == 0)
        {
            writer.WriteInt32(0);
            return;
        }

        writer.WriteInt32(items.Count);
        foreach (var item in items)
            writer.WriteString(item);
    }

    /// <summary>
    /// Read an array of serializable payloads.
    /// </summary>
    public static T[] ReadArray<T>(ref SurgewavePayloadReader reader) where T : ISerializablePayload<T>
    {
        var count = reader.ReadInt32();
        if (count <= 0)
            return [];

        var items = new T[count];
        for (var i = 0; i < count; i++)
            items[i] = T.Read(ref reader);
        return items;
    }

    /// <summary>
    /// Read an array of strings.
    /// </summary>
    public static string[] ReadStringArray(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        if (count <= 0)
            return [];

        var items = new string[count];
        for (var i = 0; i < count; i++)
            items[i] = reader.ReadString() ?? string.Empty;
        return items;
    }
}
