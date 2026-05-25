using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// CRC32-C (Castagnoli) checksum implementation for Kafka RecordBatch validation.
/// Uses hardware intrinsics when available for optimal performance:
/// - SSE4.2 CRC32 instruction on x86/x64
/// - ARM CRC32 instruction on ARM64
/// Falls back to software table-based implementation on unsupported platforms.
/// </summary>
public static class Crc32C
{
    private static readonly uint[] Table;
    private static readonly bool UseSse42X64;
    private static readonly bool UseSse42;
    private static readonly bool UseArmCrc;

    static Crc32C()
    {
        // Check for hardware support
        UseSse42X64 = Sse42.X64.IsSupported;
        UseSse42 = Sse42.IsSupported && !UseSse42X64;
        UseArmCrc = Crc32.Arm64.IsSupported;

        // Build software lookup table (still needed for fallback and small trailing bytes)
        const uint poly = 0x82F63B78; // CRC32-C reversed polynomial

        Table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint crc = i;
            for (int j = 0; j < 8; j++)
            {
                if ((crc & 1) != 0)
                    crc = (crc >> 1) ^ poly;
                else
                    crc >>= 1;
            }
            Table[i] = crc;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Compute(Span<byte> data)
    {
        return Compute((ReadOnlySpan<byte>)data);
    }

    public static uint Compute(ReadOnlySpan<byte> data)
    {
        if (UseSse42X64)
        {
            return ComputeSse42X64(data);
        }
        else if (UseSse42)
        {
            return ComputeSse42(data);
        }
        else if (UseArmCrc)
        {
            return ComputeArmCrc64(data);
        }
        else
        {
            return ComputeSoftware(data);
        }
    }

    /// <summary>
    /// SSE4.2 implementation for x64 - processes 8 bytes at a time.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ComputeSse42X64(ReadOnlySpan<byte> data)
    {
        ulong crc = 0xFFFFFFFF;
        int offset = 0;
        int length = data.Length;

        // Process 8 bytes at a time using 64-bit CRC instruction
        ref byte dataRef = ref MemoryMarshal.GetReference(data);
        while (offset + 8 <= length)
        {
            ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref dataRef, offset));
            crc = Sse42.X64.Crc32(crc, value);
            offset += 8;
        }

        // Process 4 bytes if available
        uint crc32 = (uint)crc;
        if (offset + 4 <= length)
        {
            uint value = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref dataRef, offset));
            crc32 = Sse42.Crc32(crc32, value);
            offset += 4;
        }

        // Process 2 bytes if available
        if (offset + 2 <= length)
        {
            ushort value = Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref dataRef, offset));
            crc32 = Sse42.Crc32(crc32, value);
            offset += 2;
        }

        // Process remaining byte
        if (offset < length)
        {
            crc32 = Sse42.Crc32(crc32, Unsafe.Add(ref dataRef, offset));
        }

        return ~crc32;
    }

    /// <summary>
    /// SSE4.2 implementation for x86 (32-bit) - processes 4 bytes at a time.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ComputeSse42(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        int offset = 0;
        int length = data.Length;

        // Process 4 bytes at a time using 32-bit CRC instruction
        ref byte dataRef = ref MemoryMarshal.GetReference(data);
        while (offset + 4 <= length)
        {
            uint value = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref dataRef, offset));
            crc = Sse42.Crc32(crc, value);
            offset += 4;
        }

        // Process 2 bytes if available
        if (offset + 2 <= length)
        {
            ushort value = Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref dataRef, offset));
            crc = Sse42.Crc32(crc, value);
            offset += 2;
        }

        // Process remaining byte
        if (offset < length)
        {
            crc = Sse42.Crc32(crc, Unsafe.Add(ref dataRef, offset));
        }

        return ~crc;
    }

    /// <summary>
    /// ARM CRC32 implementation for ARM64 - processes 8 bytes at a time.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ComputeArmCrc64(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        int offset = 0;
        int length = data.Length;

        ref byte dataRef = ref MemoryMarshal.GetReference(data);

        // Process 8 bytes at a time using 64-bit CRC instruction
        while (offset + 8 <= length)
        {
            ulong value = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref dataRef, offset));
            crc = Crc32.Arm64.ComputeCrc32C(crc, value);
            offset += 8;
        }

        // Process 4 bytes if available
        if (offset + 4 <= length)
        {
            uint value = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref dataRef, offset));
            crc = Crc32.ComputeCrc32C(crc, value);
            offset += 4;
        }

        // Process 2 bytes if available
        if (offset + 2 <= length)
        {
            ushort value = Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref dataRef, offset));
            crc = Crc32.ComputeCrc32C(crc, value);
            offset += 2;
        }

        // Process remaining byte
        if (offset < length)
        {
            crc = Crc32.ComputeCrc32C(crc, Unsafe.Add(ref dataRef, offset));
        }

        return ~crc;
    }

    /// <summary>
    /// Software fallback using lookup table - byte by byte processing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ComputeSoftware(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;

        foreach (byte b in data)
        {
            byte index = (byte)((crc ^ b) & 0xFF);
            crc = (crc >> 8) ^ Table[index];
        }

        return ~crc;
    }

    /// <summary>
    /// Returns whether hardware CRC32 acceleration is available.
    /// </summary>
    public static bool IsHardwareAccelerated => UseSse42X64 || UseSse42 || UseArmCrc;

    /// <summary>
    /// Returns a string describing the active implementation.
    /// </summary>
    public static string Implementation =>
        UseSse42X64 ? "SSE4.2 (x64)" :
        UseSse42 ? "SSE4.2 (x86)" :
        UseArmCrc ? "ARM CRC32" :
        "Software";
}
