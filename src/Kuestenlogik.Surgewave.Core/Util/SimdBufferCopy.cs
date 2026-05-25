using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// SIMD-optimized buffer operations for high-performance memory copying.
/// Uses AVX2/SSE2 for bulk copies when beneficial.
/// </summary>
public static class SimdBufferCopy
{
    private static readonly bool UseAvx2 = Avx2.IsSupported;
    private static readonly bool UseSse2 = Sse2.IsSupported;

    /// <summary>
    /// Threshold above which SIMD copying provides benefit.
    /// Below this, Buffer.MemoryCopy is typically faster due to overhead.
    /// </summary>
    private const int SimdThreshold = 256;

    /// <summary>
    /// Copy bytes from source to destination using SIMD when beneficial.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Copy(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (source.Length <= SimdThreshold || !UseAvx2)
        {
            source.CopyTo(destination);
            return;
        }

        CopyAvx2(ref MemoryMarshal.GetReference(source),
                 ref MemoryMarshal.GetReference(destination),
                 source.Length);
    }

    /// <summary>
    /// Copy bytes with explicit length. Useful when spans aren't readily available.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Copy(ref byte source, ref byte destination, int length)
    {
        if (length <= SimdThreshold)
        {
            Unsafe.CopyBlockUnaligned(ref destination, ref source, (uint)length);
            return;
        }

        if (UseAvx2)
        {
            CopyAvx2(ref source, ref destination, length);
        }
        else if (UseSse2)
        {
            CopySse2(ref source, ref destination, length);
        }
        else
        {
            Unsafe.CopyBlockUnaligned(ref destination, ref source, (uint)length);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CopyAvx2(ref byte source, ref byte destination, int length)
    {
        int offset = 0;

        // Copy 256 bytes at a time (8 x 32-byte vectors) for cache line efficiency
        while (offset + 256 <= length)
        {
            var v0 = Vector256.LoadUnsafe(ref Unsafe.Add(ref source, offset));
            var v1 = Vector256.LoadUnsafe(ref Unsafe.Add(ref source, offset + 32));
            var v2 = Vector256.LoadUnsafe(ref Unsafe.Add(ref source, offset + 64));
            var v3 = Vector256.LoadUnsafe(ref Unsafe.Add(ref source, offset + 96));
            var v4 = Vector256.LoadUnsafe(ref Unsafe.Add(ref source, offset + 128));
            var v5 = Vector256.LoadUnsafe(ref Unsafe.Add(ref source, offset + 160));
            var v6 = Vector256.LoadUnsafe(ref Unsafe.Add(ref source, offset + 192));
            var v7 = Vector256.LoadUnsafe(ref Unsafe.Add(ref source, offset + 224));

            v0.StoreUnsafe(ref Unsafe.Add(ref destination, offset));
            v1.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 32));
            v2.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 64));
            v3.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 96));
            v4.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 128));
            v5.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 160));
            v6.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 192));
            v7.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 224));

            offset += 256;
        }

        // Copy 32 bytes at a time
        while (offset + 32 <= length)
        {
            var v = Vector256.LoadUnsafe(ref Unsafe.Add(ref source, offset));
            v.StoreUnsafe(ref Unsafe.Add(ref destination, offset));
            offset += 32;
        }

        // Copy remaining bytes
        if (offset < length)
        {
            Unsafe.CopyBlockUnaligned(
                ref Unsafe.Add(ref destination, offset),
                ref Unsafe.Add(ref source, offset),
                (uint)(length - offset));
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CopySse2(ref byte source, ref byte destination, int length)
    {
        int offset = 0;

        // Copy 128 bytes at a time (8 x 16-byte vectors)
        while (offset + 128 <= length)
        {
            var v0 = Vector128.LoadUnsafe(ref Unsafe.Add(ref source, offset));
            var v1 = Vector128.LoadUnsafe(ref Unsafe.Add(ref source, offset + 16));
            var v2 = Vector128.LoadUnsafe(ref Unsafe.Add(ref source, offset + 32));
            var v3 = Vector128.LoadUnsafe(ref Unsafe.Add(ref source, offset + 48));
            var v4 = Vector128.LoadUnsafe(ref Unsafe.Add(ref source, offset + 64));
            var v5 = Vector128.LoadUnsafe(ref Unsafe.Add(ref source, offset + 80));
            var v6 = Vector128.LoadUnsafe(ref Unsafe.Add(ref source, offset + 96));
            var v7 = Vector128.LoadUnsafe(ref Unsafe.Add(ref source, offset + 112));

            v0.StoreUnsafe(ref Unsafe.Add(ref destination, offset));
            v1.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 16));
            v2.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 32));
            v3.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 48));
            v4.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 64));
            v5.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 80));
            v6.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 96));
            v7.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 112));

            offset += 128;
        }

        // Copy 16 bytes at a time
        while (offset + 16 <= length)
        {
            var v = Vector128.LoadUnsafe(ref Unsafe.Add(ref source, offset));
            v.StoreUnsafe(ref Unsafe.Add(ref destination, offset));
            offset += 16;
        }

        // Copy remaining bytes
        if (offset < length)
        {
            Unsafe.CopyBlockUnaligned(
                ref Unsafe.Add(ref destination, offset),
                ref Unsafe.Add(ref source, offset),
                (uint)(length - offset));
        }
    }

    /// <summary>
    /// Fill a buffer with a specific byte value using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Fill(Span<byte> destination, byte value)
    {
        if (destination.Length <= SimdThreshold || !UseAvx2)
        {
            destination.Fill(value);
            return;
        }

        FillAvx2(ref MemoryMarshal.GetReference(destination), destination.Length, value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void FillAvx2(ref byte destination, int length, byte value)
    {
        var fillVec = Vector256.Create(value);
        int offset = 0;

        // Fill 256 bytes at a time
        while (offset + 256 <= length)
        {
            fillVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset));
            fillVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 32));
            fillVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 64));
            fillVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 96));
            fillVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 128));
            fillVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 160));
            fillVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 192));
            fillVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 224));
            offset += 256;
        }

        // Fill 32 bytes at a time
        while (offset + 32 <= length)
        {
            fillVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset));
            offset += 32;
        }

        // Fill remaining bytes
        while (offset < length)
        {
            Unsafe.Add(ref destination, offset) = value;
            offset++;
        }
    }

    /// <summary>
    /// Zero a buffer using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Zero(Span<byte> destination)
    {
        if (destination.Length <= SimdThreshold || !UseAvx2)
        {
            destination.Clear();
            return;
        }

        ZeroAvx2(ref MemoryMarshal.GetReference(destination), destination.Length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ZeroAvx2(ref byte destination, int length)
    {
        var zeroVec = Vector256<byte>.Zero;
        int offset = 0;

        // Zero 256 bytes at a time
        while (offset + 256 <= length)
        {
            zeroVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset));
            zeroVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 32));
            zeroVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 64));
            zeroVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 96));
            zeroVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 128));
            zeroVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 160));
            zeroVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 192));
            zeroVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset + 224));
            offset += 256;
        }

        // Zero 32 bytes at a time
        while (offset + 32 <= length)
        {
            zeroVec.StoreUnsafe(ref Unsafe.Add(ref destination, offset));
            offset += 32;
        }

        // Zero remaining bytes
        while (offset < length)
        {
            Unsafe.Add(ref destination, offset) = 0;
            offset++;
        }
    }
}
