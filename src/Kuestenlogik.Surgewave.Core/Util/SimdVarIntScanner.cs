using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// SIMD-optimized VarInt scanning for fast record batch parsing.
/// Uses hardware intrinsics to find VarInt boundaries and decode multiple VarInts efficiently.
/// </summary>
public static class SimdVarIntScanner
{
    private static readonly bool UseAvx2 = Avx2.IsSupported;
    private static readonly bool UseSse2 = Sse2.IsSupported;

    /// <summary>
    /// Returns the active SIMD implementation.
    /// </summary>
    public static string Implementation =>
        UseAvx2 ? "AVX2" :
        UseSse2 ? "SSE2" :
        "Scalar";

    /// <summary>
    /// Skip a VarInt in the buffer and return its length without full decoding.
    /// Uses SIMD to find the terminator byte (bit 7 = 0) quickly.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int SkipVarInt(ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty) return 0;

        // Fast path: single byte (very common for small values)
        if ((buffer[0] & 0x80) == 0) return 1;

        // Fast path: two bytes
        if (buffer.Length >= 2 && (buffer[1] & 0x80) == 0) return 2;

        // Fast path: three bytes
        if (buffer.Length >= 3 && (buffer[2] & 0x80) == 0) return 3;

        // Use SIMD for longer VarInts (rare case)
        return SkipVarIntSlow(buffer);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int SkipVarIntSlow(ReadOnlySpan<byte> buffer)
    {
        for (int i = 3; i < Math.Min(buffer.Length, 10); i++)
        {
            if ((buffer[i] & 0x80) == 0) return i + 1;
        }
        return Math.Min(buffer.Length, 10);
    }

    /// <summary>
    /// Find the positions of VarInt terminators (bytes with bit 7 = 0) in a buffer.
    /// Returns a bitmask where each set bit indicates a terminator position.
    /// Uses AVX2 to scan 32 bytes at once.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint FindVarIntTerminators16(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length < 16 || !UseSse2)
        {
            return FindVarIntTerminatorsScalar(buffer, 16);
        }

        ref byte bufRef = ref MemoryMarshal.GetReference(buffer);
        var data = Vector128.LoadUnsafe(ref bufRef);

        // All bytes with bit 7 set have value >= 0x80
        // We want bytes with bit 7 = 0 (terminators)
        // Use PMOVMSKB to get mask of high bits, then invert
        var mask = Sse2.MoveMask(data);
        return (uint)(~mask & 0xFFFF);
    }

    /// <summary>
    /// Find VarInt terminators using AVX2 (32 bytes at once).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint FindVarIntTerminators32(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length < 32 || !UseAvx2)
        {
            return FindVarIntTerminatorsScalar(buffer, 32);
        }

        ref byte bufRef = ref MemoryMarshal.GetReference(buffer);
        var data = Vector256.LoadUnsafe(ref bufRef);

        // MoveMask gives us a bit for each byte where the high bit is set
        // Invert to get terminators (bit 7 = 0)
        var mask = Avx2.MoveMask(data);
        return (uint)~mask;
    }

    private static uint FindVarIntTerminatorsScalar(ReadOnlySpan<byte> buffer, int maxBits)
    {
        uint mask = 0;
        int len = Math.Min(buffer.Length, maxBits);
        for (int i = 0; i < len; i++)
        {
            if ((buffer[i] & 0x80) == 0)
            {
                mask |= (1u << i);
            }
        }
        return mask;
    }

    /// <summary>
    /// Batch decode multiple consecutive VarInts from a buffer.
    /// Returns the decoded values and total bytes consumed.
    /// Optimized for record batch parsing where we need multiple VarInts in sequence.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int BatchReadVarInts(ReadOnlySpan<byte> buffer, Span<int> values, out int bytesConsumed)
    {
        bytesConsumed = 0;
        int count = 0;
        int pos = 0;

        while (count < values.Length && pos < buffer.Length)
        {
            var (value, len) = ReadVarIntFast(buffer.Slice(pos));
            if (len == 0) break;

            values[count++] = value;
            pos += len;
        }

        bytesConsumed = pos;
        return count;
    }

    /// <summary>
    /// Fast VarInt reading with optimized branches for common cases.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (int value, int length) ReadVarIntFast(ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty) return (0, 0);

        byte b0 = buffer[0];
        if ((b0 & 0x80) == 0) return (b0, 1);

        if (buffer.Length < 2) return (0, 0);
        byte b1 = buffer[1];
        if ((b1 & 0x80) == 0) return ((b0 & 0x7F) | (b1 << 7), 2);

        if (buffer.Length < 3) return (0, 0);
        byte b2 = buffer[2];
        if ((b2 & 0x80) == 0) return ((b0 & 0x7F) | ((b1 & 0x7F) << 7) | (b2 << 14), 3);

        // Rare: 4+ byte VarInts
        return ReadVarIntFastSlow(buffer);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (int value, int length) ReadVarIntFastSlow(ReadOnlySpan<byte> buffer)
    {
        int value = (buffer[0] & 0x7F) | ((buffer[1] & 0x7F) << 7) | ((buffer[2] & 0x7F) << 14);

        if (buffer.Length < 4) return (0, 0);
        byte b3 = buffer[3];
        value |= (b3 & 0x7F) << 21;
        if ((b3 & 0x80) == 0) return (value, 4);

        if (buffer.Length < 5) return (0, 0);
        byte b4 = buffer[4];
        value |= (b4 & 0x0F) << 28; // Only 4 bits used from last byte for int32
        return (value, 5);
    }

    /// <summary>
    /// Scan a record batch's records section and return the offset of each record.
    /// This allows parallel/vectorized processing of records.
    /// </summary>
    public static int ScanRecordOffsets(ReadOnlySpan<byte> recordsData, Span<int> recordOffsets, int recordCount)
    {
        int pos = 0;
        int found = 0;

        while (found < recordCount && found < recordOffsets.Length && pos < recordsData.Length)
        {
            recordOffsets[found++] = pos;

            // Read record length (zigzag-encoded VarInt)
            var (recordLengthRaw, lenBytes) = ReadVarIntFast(recordsData.Slice(pos));
            if (lenBytes == 0) break;

            // Zigzag decode to get actual length
            int recordLength = (int)((uint)recordLengthRaw >> 1) ^ -(recordLengthRaw & 1);

            // Skip to next record
            pos += lenBytes + recordLength;
        }

        return found;
    }

    /// <summary>
    /// SIMD-optimized record offset scanning using terminator detection.
    /// Scans ahead to find VarInt boundaries, then processes them.
    /// </summary>
    public static int ScanRecordOffsetsSimd(ReadOnlySpan<byte> recordsData, Span<int> recordOffsets, int recordCount)
    {
        if (!UseSse2 || recordCount <= 4)
        {
            return ScanRecordOffsets(recordsData, recordOffsets, recordCount);
        }

        int pos = 0;
        int found = 0;

        // Process records
        while (found < recordCount && found < recordOffsets.Length && pos < recordsData.Length)
        {
            recordOffsets[found++] = pos;

            // For small remaining buffer, use scalar
            if (pos + 16 > recordsData.Length)
            {
                var (recordLengthRaw, lenBytes) = ReadVarIntFast(recordsData.Slice(pos));
                if (lenBytes == 0) break;
                int recordLength = (int)((uint)recordLengthRaw >> 1) ^ -(recordLengthRaw & 1);
                pos += lenBytes + recordLength;
                continue;
            }

            // Use SIMD to find the VarInt terminator
            var chunk = recordsData.Slice(pos);
            uint terminators = FindVarIntTerminators16(chunk);

            if (terminators == 0)
            {
                // No terminator in first 16 bytes - shouldn't happen for valid data
                // Fall back to scalar
                var (recordLengthRaw, lenBytes) = ReadVarIntFast(chunk);
                if (lenBytes == 0) break;
                int recordLength = (int)((uint)recordLengthRaw >> 1) ^ -(recordLengthRaw & 1);
                pos += lenBytes + recordLength;
                continue;
            }

            // Find first terminator position using TZCNT (trailing zero count)
            int firstTerminator = BitOperations.TrailingZeroCount(terminators);

            // Decode the VarInt up to the terminator
            int lenBytes2 = firstTerminator + 1;
            int recordLengthRaw2 = DecodeVarInt(chunk, lenBytes2);
            int recordLength2 = (int)((uint)recordLengthRaw2 >> 1) ^ -(recordLengthRaw2 & 1);

            pos += lenBytes2 + recordLength2;
        }

        return found;
    }

    /// <summary>
    /// Decode a VarInt of known length (used after SIMD boundary detection).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int DecodeVarInt(ReadOnlySpan<byte> buffer, int length)
    {
        return length switch
        {
            1 => buffer[0],
            2 => (buffer[0] & 0x7F) | (buffer[1] << 7),
            3 => (buffer[0] & 0x7F) | ((buffer[1] & 0x7F) << 7) | (buffer[2] << 14),
            4 => (buffer[0] & 0x7F) | ((buffer[1] & 0x7F) << 7) | ((buffer[2] & 0x7F) << 14) | (buffer[3] << 21),
            5 => (buffer[0] & 0x7F) | ((buffer[1] & 0x7F) << 7) | ((buffer[2] & 0x7F) << 14) | ((buffer[3] & 0x7F) << 21) | ((buffer[4] & 0x0F) << 28),
            _ => 0
        };
    }

    /// <summary>
    /// Optimized record field skipping - skips timestamp, offset, key length, key, value length, value, headers.
    /// Uses SIMD to quickly find VarInt boundaries.
    /// Returns the position after the record.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int SkipRecordFields(ReadOnlySpan<byte> recordData, int startPos)
    {
        int pos = startPos;

        // Skip attributes (1 byte)
        pos++;

        // Skip timestampDelta (zigzag varlong)
        pos += SkipVarInt(recordData.Slice(pos));

        // Skip offsetDelta (zigzag varint)
        pos += SkipVarInt(recordData.Slice(pos));

        // Key length and key
        var (keyLenRaw, keyLenBytes) = ReadVarIntFast(recordData.Slice(pos));
        pos += keyLenBytes;
        int keyLen = (int)((uint)keyLenRaw >> 1) ^ -(keyLenRaw & 1);
        if (keyLen > 0) pos += keyLen;

        // Value length and value
        var (valueLenRaw, valueLenBytes) = ReadVarIntFast(recordData.Slice(pos));
        pos += valueLenBytes;
        int valueLen = (int)((uint)valueLenRaw >> 1) ^ -(valueLenRaw & 1);
        if (valueLen > 0) pos += valueLen;

        // Headers count and headers
        var (headerCount, headerCountBytes) = ReadVarIntFast(recordData.Slice(pos));
        pos += headerCountBytes;
        for (int h = 0; h < headerCount; h++)
        {
            var (hKeyLen, hKeyLenBytes) = ReadVarIntFast(recordData.Slice(pos));
            pos += hKeyLenBytes;
            if (hKeyLen > 0) pos += hKeyLen;

            var (hValLen, hValLenBytes) = ReadVarIntFast(recordData.Slice(pos));
            pos += hValLenBytes;
            if (hValLen > 0) pos += hValLen;
        }

        return pos;
    }

    /// <summary>
    /// Extract key and value positions from a record efficiently.
    /// Returns positions and lengths that can be used to slice the data.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ExtractKeyValue(
        ReadOnlySpan<byte> recordData, int startPos,
        out int keyStart, out int keyLength,
        out int valueStart, out int valueLength)
    {
        int pos = startPos;

        // Skip attributes (1 byte)
        pos++;

        // Skip timestampDelta (zigzag varlong)
        pos += SkipVarInt(recordData.Slice(pos));

        // Skip offsetDelta (zigzag varint)
        pos += SkipVarInt(recordData.Slice(pos));

        // Key length and key
        var (keyLenRaw, keyLenBytes) = ReadVarIntFast(recordData.Slice(pos));
        pos += keyLenBytes;
        int keyLen = (int)((uint)keyLenRaw >> 1) ^ -(keyLenRaw & 1);
        keyStart = keyLen > 0 ? pos : -1;
        keyLength = keyLen > 0 ? keyLen : 0;
        if (keyLen > 0) pos += keyLen;

        // Value length and value
        var (valueLenRaw, valueLenBytes) = ReadVarIntFast(recordData.Slice(pos));
        pos += valueLenBytes;
        int valueLen = (int)((uint)valueLenRaw >> 1) ^ -(valueLenRaw & 1);
        valueStart = valueLen > 0 ? pos : -1;
        valueLength = valueLen > 0 ? valueLen : 0;
        if (valueLen > 0) pos += valueLen;

        // Skip headers
        var (headerCount, headerCountBytes) = ReadVarIntFast(recordData.Slice(pos));
        pos += headerCountBytes;
        for (int h = 0; h < headerCount; h++)
        {
            var (hKeyLen, hKeyLenBytes) = ReadVarIntFast(recordData.Slice(pos));
            pos += hKeyLenBytes;
            if (hKeyLen > 0) pos += hKeyLen;

            var (hValLen, hValLenBytes) = ReadVarIntFast(recordData.Slice(pos));
            pos += hValLenBytes;
            if (hValLen > 0) pos += hValLen;
        }

        return pos;
    }

    /// <summary>
    /// Count the number of VarInts in a buffer using SIMD.
    /// Useful for quickly determining record/field counts.
    /// </summary>
    public static int CountVarInts(ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty) return 0;

        int count = 0;
        int pos = 0;

        // Use SIMD for bulk scanning
        if (UseAvx2 && buffer.Length >= 32)
        {
            while (pos + 32 <= buffer.Length)
            {
                uint terminators = FindVarIntTerminators32(buffer.Slice(pos));
                count += BitOperations.PopCount(terminators);
                pos += 32;
            }
        }
        else if (UseSse2 && buffer.Length >= 16)
        {
            while (pos + 16 <= buffer.Length)
            {
                uint terminators = FindVarIntTerminators16(buffer.Slice(pos));
                count += BitOperations.PopCount(terminators);
                pos += 16;
            }
        }

        // Handle remaining bytes
        while (pos < buffer.Length)
        {
            if ((buffer[pos] & 0x80) == 0)
            {
                count++;
            }
            pos++;
        }

        return count;
    }
}
