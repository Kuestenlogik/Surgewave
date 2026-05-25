using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// SIMD-optimized byte array comparer for use in dictionaries and hash sets.
/// Uses hardware intrinsics when available for fast equality checks and hashing.
/// </summary>
public sealed class SimdByteArrayComparer : IEqualityComparer<byte[]>
{
    /// <summary>
    /// Singleton instance for reuse.
    /// </summary>
    public static readonly SimdByteArrayComparer Instance = new();

    private static readonly bool UseAvx2 = Avx2.IsSupported;
    private static readonly bool UseSse2 = Sse2.IsSupported;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;
        if (x.Length != y.Length) return false;
        if (x.Length == 0) return true;

        return EqualsSimd(x, y);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsSimd(byte[] x, byte[] y)
    {
        int length = x.Length;

        if (UseAvx2 && length >= 32)
        {
            return EqualsAvx2(x, y);
        }
        else if (UseSse2 && length >= 16)
        {
            return EqualsSse2(x, y);
        }
        else
        {
            return EqualsScalar(x, y);
        }
    }

    /// <summary>
    /// AVX2 implementation - compares 32 bytes at a time.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsAvx2(byte[] x, byte[] y)
    {
        int length = x.Length;
        int offset = 0;

        ref byte xRef = ref MemoryMarshal.GetArrayDataReference(x);
        ref byte yRef = ref MemoryMarshal.GetArrayDataReference(y);

        // Process 32 bytes at a time
        while (offset + 32 <= length)
        {
            var xVec = Vector256.LoadUnsafe(ref Unsafe.Add(ref xRef, offset));
            var yVec = Vector256.LoadUnsafe(ref Unsafe.Add(ref yRef, offset));

            // Compare and get mask - all 1s if equal
            var cmp = Avx2.CompareEqual(xVec, yVec);
            int mask = Avx2.MoveMask(cmp);

            // If not all bytes equal, arrays differ
            if (mask != unchecked((int)0xFFFFFFFF))
            {
                return false;
            }

            offset += 32;
        }

        // Process remaining 16 bytes with SSE2 if possible
        if (offset + 16 <= length)
        {
            var xVec = Vector128.LoadUnsafe(ref Unsafe.Add(ref xRef, offset));
            var yVec = Vector128.LoadUnsafe(ref Unsafe.Add(ref yRef, offset));

            var cmp = Sse2.CompareEqual(xVec, yVec);
            int mask = Sse2.MoveMask(cmp);

            if (mask != 0xFFFF)
            {
                return false;
            }

            offset += 16;
        }

        // Process remaining bytes
        while (offset < length)
        {
            if (Unsafe.Add(ref xRef, offset) != Unsafe.Add(ref yRef, offset))
            {
                return false;
            }
            offset++;
        }

        return true;
    }

    /// <summary>
    /// SSE2 implementation - compares 16 bytes at a time.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsSse2(byte[] x, byte[] y)
    {
        int length = x.Length;
        int offset = 0;

        ref byte xRef = ref MemoryMarshal.GetArrayDataReference(x);
        ref byte yRef = ref MemoryMarshal.GetArrayDataReference(y);

        // Process 16 bytes at a time
        while (offset + 16 <= length)
        {
            var xVec = Vector128.LoadUnsafe(ref Unsafe.Add(ref xRef, offset));
            var yVec = Vector128.LoadUnsafe(ref Unsafe.Add(ref yRef, offset));

            var cmp = Sse2.CompareEqual(xVec, yVec);
            int mask = Sse2.MoveMask(cmp);

            if (mask != 0xFFFF)
            {
                return false;
            }

            offset += 16;
        }

        // Process remaining bytes
        while (offset < length)
        {
            if (Unsafe.Add(ref xRef, offset) != Unsafe.Add(ref yRef, offset))
            {
                return false;
            }
            offset++;
        }

        return true;
    }

    /// <summary>
    /// Scalar fallback using 64-bit comparisons.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsScalar(byte[] x, byte[] y)
    {
        int length = x.Length;
        int offset = 0;

        ref byte xRef = ref MemoryMarshal.GetArrayDataReference(x);
        ref byte yRef = ref MemoryMarshal.GetArrayDataReference(y);

        // Process 8 bytes at a time using ulong comparison
        while (offset + 8 <= length)
        {
            ulong xVal = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref xRef, offset));
            ulong yVal = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref yRef, offset));

            if (xVal != yVal)
            {
                return false;
            }

            offset += 8;
        }

        // Process remaining bytes
        while (offset < length)
        {
            if (Unsafe.Add(ref xRef, offset) != Unsafe.Add(ref yRef, offset))
            {
                return false;
            }
            offset++;
        }

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetHashCode(byte[] obj)
    {
        if (obj is null || obj.Length == 0) return 0;

        if (UseAvx2 && obj.Length >= 32)
        {
            return GetHashCodeAvx2(obj);
        }
        else if (UseSse2 && obj.Length >= 16)
        {
            return GetHashCodeSse2(obj);
        }
        else
        {
            return GetHashCodeScalar(obj);
        }
    }

    /// <summary>
    /// AVX2 hash using XOR-based accumulation.
    /// Uses a simple but fast approach: XOR all 32-byte chunks together,
    /// then fold down to 32 bits with final mixing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetHashCodeAvx2(byte[] data)
    {
        int length = data.Length;
        int offset = 0;

        ref byte dataRef = ref MemoryMarshal.GetArrayDataReference(data);

        // Initialize hash vectors with prime-based seeds
        var hash1 = Vector256.Create(0x1B873593u, 0xCC9E2D51u, 0x85EBCA6Bu, 0xC2B2AE35u,
                                     0xE6546B64u, 0x239B961Bu, 0xAB0E9789u, 0x38B34AE5u);
        var hash2 = Vector256.Create(0x9E3779B9u, 0x7F4A7C15u, 0xBB67AE85u, 0x3C6EF372u,
                                     0xA54FF53Au, 0x510E527Fu, 0x9B05688Cu, 0x1F83D9ABu);

        // Process 32 bytes at a time with alternating XOR for better distribution
        while (offset + 32 <= length)
        {
            var chunk = Vector256.LoadUnsafe(ref Unsafe.Add(ref dataRef, offset)).AsUInt32();
            hash1 = Avx2.Xor(hash1, chunk);
            hash2 = Avx2.Xor(hash2, Avx2.Shuffle(chunk.AsByte(), Vector256.Create(
                (byte)3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12,
                19, 18, 17, 16, 23, 22, 21, 20, 27, 26, 25, 24, 31, 30, 29, 28)).AsUInt32());
            offset += 32;
        }

        // Combine the two hash vectors
        var combined = Avx2.Xor(hash1, hash2);

        // Fold 256-bit to 128-bit
        var lo = combined.GetLower();
        var hi = combined.GetUpper();
        var hash128 = Sse2.Xor(lo, hi);

        // Fold 128-bit to 64-bit
        ulong h64 = hash128.GetElement(0) ^ hash128.GetElement(1) ^
                    ((ulong)hash128.GetElement(2) << 16) ^ ((ulong)hash128.GetElement(3) << 24);

        // Process remaining bytes with scalar
        while (offset < length)
        {
            h64 ^= Unsafe.Add(ref dataRef, offset);
            h64 *= 0x5BD1E995u;
            offset++;
        }

        // Final mix
        h64 ^= h64 >> 33;
        h64 *= 0xFF51AFD7ED558CCDuL;
        h64 ^= h64 >> 33;

        return (int)h64;
    }

    /// <summary>
    /// SSE2 hash implementation - processes 16 bytes at a time.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetHashCodeSse2(byte[] data)
    {
        int length = data.Length;
        int offset = 0;

        ref byte dataRef = ref MemoryMarshal.GetArrayDataReference(data);

        // Initialize hash vector with prime-based seed
        var hash = Vector128.Create(0x1B873593u, 0xCC9E2D51u, 0x85EBCA6Bu, 0xC2B2AE35u);

        // Process 16 bytes at a time with XOR accumulation
        while (offset + 16 <= length)
        {
            var chunk = Vector128.LoadUnsafe(ref Unsafe.Add(ref dataRef, offset)).AsUInt32();
            hash = Sse2.Xor(hash, chunk);
            // Use shift-based mixing (SSE2 compatible)
            hash = Sse2.Xor(hash, Sse2.ShiftRightLogical(chunk, 16));
            offset += 16;
        }

        // Fold 128-bit to 64-bit
        ulong h64 = hash.GetElement(0) ^ hash.GetElement(1) ^
                    ((ulong)hash.GetElement(2) << 16) ^ ((ulong)hash.GetElement(3) << 24);

        // Process remaining bytes
        while (offset < length)
        {
            h64 ^= Unsafe.Add(ref dataRef, offset);
            h64 *= 0x5BD1E995u;
            offset++;
        }

        // Final mix
        h64 ^= h64 >> 33;
        h64 *= 0xFF51AFD7ED558CCDuL;
        h64 ^= h64 >> 33;

        return (int)h64;
    }

    /// <summary>
    /// Scalar hash using 64-bit operations - optimized for small arrays.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetHashCodeScalar(byte[] data)
    {
        int length = data.Length;
        int offset = 0;

        ref byte dataRef = ref MemoryMarshal.GetArrayDataReference(data);

        ulong hash = 0x9E3779B97F4A7C15uL; // Golden ratio prime

        // Process 8 bytes at a time
        while (offset + 8 <= length)
        {
            ulong chunk = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref dataRef, offset));
            hash ^= chunk;
            hash *= 0x5BD1E995u;
            hash ^= hash >> 47;
            offset += 8;
        }

        // Process remaining bytes
        while (offset < length)
        {
            hash ^= Unsafe.Add(ref dataRef, offset);
            hash *= 0x5BD1E995u;
            offset++;
        }

        // Final mix
        hash ^= hash >> 33;
        hash *= 0xFF51AFD7ED558CCDuL;
        hash ^= hash >> 33;

        return (int)hash;
    }

    /// <summary>
    /// Returns a string describing the active implementation.
    /// </summary>
    public static string Implementation =>
        UseAvx2 ? "AVX2" :
        UseSse2 ? "SSE2" :
        "Scalar";

    /// <summary>
    /// Returns whether hardware acceleration is available.
    /// </summary>
    public static bool IsHardwareAccelerated => UseAvx2 || UseSse2;
}
