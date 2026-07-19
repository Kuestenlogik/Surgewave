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
///
/// On x64 above <see cref="InterleaveThreshold"/> bytes the SSE4.2 path switches to a 3-way
/// interleaved kernel (<see cref="ComputeSse42X64Interleaved"/>): the plain path is a single serial
/// <c>crc32</c> dependency chain (the instruction is ~3-cycle latency, 1/cycle throughput, so a serial
/// chain uses only ~1/3 of the unit), whereas running three independent sub-streams in lockstep and
/// recombining them with a precomputed carry-less "shift" reaches ~full rate on large buffers. The
/// recombine tables are built at startup FROM the real <c>crc32</c> instruction (advancing a state over
/// L zero bytes), so there is no hand-derived polynomial constant to get wrong; the interleaved result is
/// bit-identical to the serial chain for every input (guarded by a cross-implementation fuzz test).
/// </summary>
public static class Crc32C
{
    /// <summary>Sub-stream length (bytes) for the 3-way interleave; a multiple of 8 for the 8-byte crc32 step.</summary>
    private const int InterleaveL = 1024;
    /// <summary>Full interleaved block = three sub-streams.</summary>
    private const int InterleaveBlock = 3 * InterleaveL;
    /// <summary>Below this length the plain serial chain wins (fold setup/tail cost dominates); stay serial.</summary>
    private const int InterleaveThreshold = InterleaveBlock;

    private static readonly uint[] Table;
    private static readonly bool UseSse42X64;
    private static readonly bool UseSse42;
    private static readonly bool UseArmCrc;

    // Slice-by-4 "shift" tables: advance a 32-bit crc state by InterleaveL / 2*InterleaveL zero bytes in
    // O(1). Non-null only when UseSse42X64 (built from the real crc32 instruction). ShiftL[k][b] is the
    // contribution of byte b at position k, i.e. AdvanceZeros(b << (8*k), n) — linearity of the raw CRC
    // lets the four byte-contributions be XORed back together.
    private static readonly uint[][]? ShiftL;
    private static readonly uint[][]? Shift2L;

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

        // Precompute the interleave recombine tables from the real crc32 instruction.
        if (UseSse42X64)
        {
            ShiftL = BuildShiftTable(InterleaveL);
            Shift2L = BuildShiftTable(2 * InterleaveL);
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

    /// <summary>x64 SSE4.2 entry: interleave large buffers, otherwise the serial chain.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ComputeSse42X64(ReadOnlySpan<byte> data)
    {
        return data.Length >= InterleaveThreshold
            ? ComputeSse42X64Interleaved(data)
            : ComputeSse42X64Serial(data);
    }

    /// <summary>
    /// SSE4.2 serial chain for x64 - processes 8 bytes at a time. Single <c>crc32</c> dependency chain.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static uint ComputeSse42X64Serial(ReadOnlySpan<byte> data)
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

        return FinishTail(crc, ref dataRef, offset, length);
    }

    /// <summary>
    /// 3-way interleaved SSE4.2 CRC32C for x64. Three independent sub-stream chains break the serial
    /// <c>crc32</c> latency dependency; each full block is recombined via the precomputed shift tables.
    /// Bit-identical to <see cref="ComputeSse42X64Serial"/> for every input length.
    /// </summary>
    internal static uint ComputeSse42X64Interleaved(ReadOnlySpan<byte> data)
    {
        ref byte dataRef = ref MemoryMarshal.GetReference(data);
        int length = data.Length;
        int offset = 0;
        ulong crc = 0xFFFFFFFF; // running state; seeded into sub-stream A of the first block only

        uint[][] shiftL = ShiftL!;
        uint[][] shift2L = Shift2L!;

        while (offset + InterleaveBlock <= length)
        {
            // Three independent chains over [0,L) [L,2L) [2L,3L), advanced in lockstep so the CPU keeps
            // three crc32 instructions in flight and hides the ~3-cycle latency.
            ulong a = crc, b = 0, c = 0;
            int baseB = offset + InterleaveL;
            int baseC = offset + 2 * InterleaveL;
            for (int i = 0; i < InterleaveL; i += 8)
            {
                a = Sse42.X64.Crc32(a, Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref dataRef, offset + i)));
                b = Sse42.X64.Crc32(b, Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref dataRef, baseB + i)));
                c = Sse42.X64.Crc32(c, Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref dataRef, baseC + i)));
            }

            // Recombine: crc = shift(a, 2L) XOR shift(b, L) XOR c  (raw-CRC linearity).
            crc = ShiftBy(shift2L, (uint)a) ^ ShiftBy(shiftL, (uint)b) ^ (uint)c;
            offset += InterleaveBlock;
        }

        // Any 8-byte multiples still left before the < 8-byte tail, continued serially from the running crc.
        while (offset + 8 <= length)
        {
            crc = Sse42.X64.Crc32(crc, Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref dataRef, offset)));
            offset += 8;
        }

        return FinishTail(crc, ref dataRef, offset, length);
    }

    /// <summary>Process the trailing 4/2/1 bytes from a running crc and apply the final xor-out.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint FinishTail(ulong crc, ref byte dataRef, int offset, int length)
    {
        uint crc32 = (uint)crc;
        if (offset + 4 <= length)
        {
            uint value = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref dataRef, offset));
            crc32 = Sse42.Crc32(crc32, value);
            offset += 4;
        }
        if (offset + 2 <= length)
        {
            ushort value = Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref dataRef, offset));
            crc32 = Sse42.Crc32(crc32, value);
            offset += 2;
        }
        if (offset < length)
        {
            crc32 = Sse42.Crc32(crc32, Unsafe.Add(ref dataRef, offset));
        }
        return ~crc32;
    }

    /// <summary>Advance a raw crc state by <paramref name="zeroBytes"/> zero bytes via the real crc32 instruction.</summary>
    private static uint AdvanceZeros(uint state, int zeroBytes)
    {
        ulong crc = state;
        int i = 0;
        for (; i + 8 <= zeroBytes; i += 8)
            crc = Sse42.X64.Crc32(crc, 0UL);
        uint c = (uint)crc;
        for (; i < zeroBytes; i++)
            c = Sse42.Crc32(c, (byte)0);
        return c;
    }

    /// <summary>
    /// Build a slice-by-4 table for "advance a crc state by <paramref name="zeroBytes"/> zero bytes":
    /// <c>table[k][b] = AdvanceZeros(b &lt;&lt; (8*k), zeroBytes)</c>. Derived from the real instruction, so no
    /// hand-derived polynomial constant is involved.
    /// </summary>
    private static uint[][] BuildShiftTable(int zeroBytes)
    {
        var table = new uint[4][];
        for (int k = 0; k < 4; k++)
        {
            var col = new uint[256];
            int shift = 8 * k;
            for (int b = 0; b < 256; b++)
                col[b] = AdvanceZeros((uint)b << shift, zeroBytes);
            table[k] = col;
        }
        return table;
    }

    /// <summary>Apply a precomputed shift table to a 32-bit crc state (raw-CRC linear, slice-by-4).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ShiftBy(uint[][] table, uint c)
    {
        return table[0][c & 0xFF]
             ^ table[1][(c >> 8) & 0xFF]
             ^ table[2][(c >> 16) & 0xFF]
             ^ table[3][(c >> 24) & 0xFF];
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
    internal static uint ComputeSoftware(ReadOnlySpan<byte> data)
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
