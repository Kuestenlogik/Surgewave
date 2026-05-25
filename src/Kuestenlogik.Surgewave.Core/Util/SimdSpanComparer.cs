using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// SIMD-optimized ReadOnlySpan comparison using AVX2/SSE2 intrinsics.
/// Optimized for comparing header keys and other byte sequences in hot paths.
/// </summary>
public static class SimdSpanComparer
{
    private static readonly bool UseAvx2 = Avx2.IsSupported;
    private static readonly bool UseSse2 = Sse2.IsSupported;

    /// <summary>
    /// Fast SIMD-optimized equality comparison for two byte spans.
    /// Uses AVX2 for 32+ bytes, SSE2 for 16+ bytes, or scalar for smaller spans.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool SequenceEqual(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        if (left.Length != right.Length)
            return false;

        if (left.Length == 0)
            return true;

        // For very short spans, use built-in (already optimized)
        if (left.Length < 8)
            return left.SequenceEqual(right);

        return EqualsSimd(ref MemoryMarshal.GetReference(left),
                          ref MemoryMarshal.GetReference(right),
                          left.Length);
    }

    /// <summary>
    /// Fast SIMD-optimized comparison for spans that are known to be the same length.
    /// Skips length check for performance in tight loops.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool SequenceEqualUnsafe(ref byte left, ref byte right, int length)
    {
        if (length == 0)
            return true;

        return EqualsSimd(ref left, ref right, length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsSimd(ref byte left, ref byte right, int length)
    {
        if (UseAvx2 && length >= 32)
        {
            return EqualsAvx2(ref left, ref right, length);
        }
        else if (UseSse2 && length >= 16)
        {
            return EqualsSse2(ref left, ref right, length);
        }
        else
        {
            return EqualsScalar(ref left, ref right, length);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsAvx2(ref byte left, ref byte right, int length)
    {
        int offset = 0;

        // Process 32 bytes at a time
        while (offset + 32 <= length)
        {
            var leftVec = Vector256.LoadUnsafe(ref Unsafe.Add(ref left, offset));
            var rightVec = Vector256.LoadUnsafe(ref Unsafe.Add(ref right, offset));

            var cmp = Avx2.CompareEqual(leftVec, rightVec);
            int mask = Avx2.MoveMask(cmp);

            if (mask != unchecked((int)0xFFFFFFFF))
            {
                return false;
            }

            offset += 32;
        }

        // Process remaining 16 bytes with SSE2
        if (offset + 16 <= length)
        {
            var leftVec = Vector128.LoadUnsafe(ref Unsafe.Add(ref left, offset));
            var rightVec = Vector128.LoadUnsafe(ref Unsafe.Add(ref right, offset));

            var cmp = Sse2.CompareEqual(leftVec, rightVec);
            int mask = Sse2.MoveMask(cmp);

            if (mask != 0xFFFF)
            {
                return false;
            }

            offset += 16;
        }

        // Process remaining bytes
        return EqualsScalar(ref Unsafe.Add(ref left, offset), ref Unsafe.Add(ref right, offset), length - offset);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsSse2(ref byte left, ref byte right, int length)
    {
        int offset = 0;

        // Process 16 bytes at a time
        while (offset + 16 <= length)
        {
            var leftVec = Vector128.LoadUnsafe(ref Unsafe.Add(ref left, offset));
            var rightVec = Vector128.LoadUnsafe(ref Unsafe.Add(ref right, offset));

            var cmp = Sse2.CompareEqual(leftVec, rightVec);
            int mask = Sse2.MoveMask(cmp);

            if (mask != 0xFFFF)
            {
                return false;
            }

            offset += 16;
        }

        // Process remaining bytes
        return EqualsScalar(ref Unsafe.Add(ref left, offset), ref Unsafe.Add(ref right, offset), length - offset);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsScalar(ref byte left, ref byte right, int length)
    {
        // Use unaligned long comparisons for 8+ bytes
        int offset = 0;

        while (offset + 8 <= length)
        {
            if (Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref left, offset)) !=
                Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref right, offset)))
            {
                return false;
            }
            offset += 8;
        }

        // Compare remaining 4 bytes
        if (offset + 4 <= length)
        {
            if (Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref left, offset)) !=
                Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref right, offset)))
            {
                return false;
            }
            offset += 4;
        }

        // Compare remaining bytes
        while (offset < length)
        {
            if (Unsafe.Add(ref left, offset) != Unsafe.Add(ref right, offset))
            {
                return false;
            }
            offset++;
        }

        return true;
    }

    /// <summary>
    /// Find the first occurrence of a byte pattern in a span using SIMD.
    /// Returns -1 if not found.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int IndexOf(ReadOnlySpan<byte> span, ReadOnlySpan<byte> pattern)
    {
        if (pattern.IsEmpty)
            return 0;

        if (pattern.Length > span.Length)
            return -1;

        if (pattern.Length == 1)
            return span.IndexOf(pattern[0]);

        // For small patterns, use built-in IndexOf (already optimized)
        if (pattern.Length <= 4 || !UseSse2)
            return span.IndexOf(pattern);

        return IndexOfSimd(ref MemoryMarshal.GetReference(span), span.Length,
                           ref MemoryMarshal.GetReference(pattern), pattern.Length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int IndexOfSimd(ref byte span, int spanLength, ref byte pattern, int patternLength)
    {
        byte firstByte = pattern;
        int searchEnd = spanLength - patternLength;

        // Create vector with first byte replicated
        var firstByteVec = Vector128.Create(firstByte);

        int i = 0;

        // SIMD search for first byte
        while (i + 16 <= searchEnd)
        {
            var block = Vector128.LoadUnsafe(ref Unsafe.Add(ref span, i));
            var cmp = Sse2.CompareEqual(block, firstByteVec);
            int mask = Sse2.MoveMask(cmp);

            while (mask != 0)
            {
                int offset = BitOperations.TrailingZeroCount(mask);
                int pos = i + offset;

                if (pos <= searchEnd &&
                    SequenceEqualUnsafe(ref Unsafe.Add(ref span, pos), ref pattern, patternLength))
                {
                    return pos;
                }

                mask &= mask - 1; // Clear lowest bit
            }

            i += 16;
        }

        // Scalar fallback for remaining bytes
        while (i <= searchEnd)
        {
            if (Unsafe.Add(ref span, i) == firstByte &&
                SequenceEqualUnsafe(ref Unsafe.Add(ref span, i), ref pattern, patternLength))
            {
                return i;
            }
            i++;
        }

        return -1;
    }
}
