using System.Runtime.CompilerServices;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// ZigZag transform for signed integers — maps signed values to unsigned so small-magnitude
/// negatives stay small under varint encoding. This is the RecordBatch-v2 (storage log format)
/// field codec, shared by the storage/serializer path and the Kafka wire primitives; it lives
/// in Core so neither the native/storage paths nor the Kafka protocol own it exclusively.
/// </summary>
public static class ZigZag
{
    /// <summary>ZigZag-encode a signed 32-bit integer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Encode(int value) => (uint)((value << 1) ^ (value >> 31));

    /// <summary>ZigZag-decode to a signed 32-bit integer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Decode(uint value) => (int)((value >> 1) ^ (-(value & 1)));

    /// <summary>ZigZag-encode a signed 64-bit integer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Encode(long value) => (ulong)((value << 1) ^ (value >> 63));

    /// <summary>ZigZag-decode to a signed 64-bit integer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Decode(ulong value) => (long)(value >> 1) ^ (-(long)(value & 1));
}
