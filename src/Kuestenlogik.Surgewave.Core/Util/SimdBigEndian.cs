using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// SIMD-optimized big-endian byte-swap operations for batch processing.
/// Uses SSSE3 PSHUFB / AVX2 VPSHUFB to reverse byte order in multiple values simultaneously.
/// </summary>
public static class SimdBigEndian
{
    private static readonly bool UseSsse3 = Ssse3.IsSupported;
    private static readonly bool UseAvx2 = Avx2.IsSupported;

    /// <summary>
    /// Minimum batch size to use SIMD operations. Below this threshold, scalar operations are used.
    /// -1 = disabled (always use scalar), 0 = auto (always use SIMD when available), >0 = minimum batch size.
    /// Default is 4.
    /// </summary>
    public static int MinBatchSize { get; set; } = 4;

    /// <summary>
    /// Returns the active SIMD implementation.
    /// </summary>
    public static string Implementation =>
        MinBatchSize < 0 ? "Disabled" :
        UseAvx2 ? "AVX2" :
        UseSsse3 ? "SSSE3" :
        "Scalar";

    /// <summary>
    /// Whether hardware acceleration is available and enabled.
    /// </summary>
    public static bool IsHardwareAccelerated => UseSsse3 && MinBatchSize >= 0;

    // Shuffle masks for byte reversal within different element sizes
    // SSSE3 (16 bytes) - reverse bytes within 2x Int64
    private static readonly Vector128<byte> ShuffleMask64_128 = Vector128.Create(
        (byte)7, 6, 5, 4, 3, 2, 1, 0,  // First Int64
        15, 14, 13, 12, 11, 10, 9, 8   // Second Int64
    );

    // SSSE3 (16 bytes) - reverse bytes within 4x Int32
    private static readonly Vector128<byte> ShuffleMask32_128 = Vector128.Create(
        (byte)3, 2, 1, 0,   // First Int32
        7, 6, 5, 4,         // Second Int32
        11, 10, 9, 8,       // Third Int32
        15, 14, 13, 12      // Fourth Int32
    );

    // SSSE3 (16 bytes) - reverse bytes within 8x Int16
    private static readonly Vector128<byte> ShuffleMask16_128 = Vector128.Create(
        (byte)1, 0,   // First Int16
        3, 2,         // Second Int16
        5, 4,         // Third Int16
        7, 6,         // Fourth Int16
        9, 8,         // Fifth Int16
        11, 10,       // Sixth Int16
        13, 12,       // Seventh Int16
        15, 14        // Eighth Int16
    );

    // AVX2 (32 bytes) - reverse bytes within 4x Int64
    private static readonly Vector256<byte> ShuffleMask64_256 = Vector256.Create(
        (byte)7, 6, 5, 4, 3, 2, 1, 0,      // First Int64 (lower lane)
        15, 14, 13, 12, 11, 10, 9, 8,      // Second Int64 (lower lane)
        7, 6, 5, 4, 3, 2, 1, 0,            // Third Int64 (upper lane, same pattern)
        15, 14, 13, 12, 11, 10, 9, 8       // Fourth Int64 (upper lane)
    );

    // AVX2 (32 bytes) - reverse bytes within 8x Int32
    private static readonly Vector256<byte> ShuffleMask32_256 = Vector256.Create(
        (byte)3, 2, 1, 0,     // First Int32
        7, 6, 5, 4,           // Second Int32
        11, 10, 9, 8,         // Third Int32
        15, 14, 13, 12,       // Fourth Int32
        3, 2, 1, 0,           // Fifth Int32 (upper lane)
        7, 6, 5, 4,           // Sixth Int32
        11, 10, 9, 8,         // Seventh Int32
        15, 14, 13, 12        // Eighth Int32
    );

    /// <summary>
    /// Write multiple Int64 values to a buffer in big-endian format using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt64sBigEndian(Span<byte> destination, ReadOnlySpan<long> values)
    {
        if (values.IsEmpty) return;

        var pos = 0;
        var destPos = 0;
        var count = values.Length;

        // Use scalar for small batches or when SIMD is disabled (-1)
        if (MinBatchSize < 0 || (MinBatchSize > 0 && count < MinBatchSize))
        {
            while (count > 0)
            {
                BinaryPrimitives.WriteInt64BigEndian(destination.Slice(destPos, 8), values[pos]);
                pos++;
                destPos += 8;
                count--;
            }
            return;
        }

        // AVX2 path: process 4 Int64s at a time (32 bytes)
        if (UseAvx2 && count >= 4)
        {
            ref byte destRef = ref MemoryMarshal.GetReference(destination);
            ref long srcRef = ref MemoryMarshal.GetReference(values);

            while (count >= 4)
            {
                // Load 4 Int64s
                var data = Vector256.LoadUnsafe(ref Unsafe.As<long, byte>(ref Unsafe.Add(ref srcRef, pos)));

                // Shuffle bytes to reverse within each Int64
                var shuffled = Avx2.Shuffle(data, ShuffleMask64_256);

                // Store result
                shuffled.StoreUnsafe(ref Unsafe.Add(ref destRef, destPos));

                pos += 4;
                destPos += 32;
                count -= 4;
            }
        }

        // SSSE3 path: process 2 Int64s at a time (16 bytes)
        if (UseSsse3 && count >= 2)
        {
            ref byte destRef = ref MemoryMarshal.GetReference(destination);
            ref long srcRef = ref MemoryMarshal.GetReference(values);

            while (count >= 2)
            {
                var data = Vector128.LoadUnsafe(ref Unsafe.As<long, byte>(ref Unsafe.Add(ref srcRef, pos)));
                var shuffled = Ssse3.Shuffle(data, ShuffleMask64_128);
                shuffled.StoreUnsafe(ref Unsafe.Add(ref destRef, destPos));

                pos += 2;
                destPos += 16;
                count -= 2;
            }
        }

        // Scalar fallback for remaining values
        while (count > 0)
        {
            BinaryPrimitives.WriteInt64BigEndian(destination.Slice(destPos, 8), values[pos]);
            pos++;
            destPos += 8;
            count--;
        }
    }

    /// <summary>
    /// Write multiple Int32 values to a buffer in big-endian format using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt32sBigEndian(Span<byte> destination, ReadOnlySpan<int> values)
    {
        if (values.IsEmpty) return;

        var pos = 0;
        var destPos = 0;
        var count = values.Length;

        // Use scalar for small batches or when SIMD is disabled (-1)
        if (MinBatchSize < 0 || (MinBatchSize > 0 && count < MinBatchSize))
        {
            while (count > 0)
            {
                BinaryPrimitives.WriteInt32BigEndian(destination.Slice(destPos, 4), values[pos]);
                pos++;
                destPos += 4;
                count--;
            }
            return;
        }

        // AVX2 path: process 8 Int32s at a time (32 bytes)
        if (UseAvx2 && count >= 8)
        {
            ref byte destRef = ref MemoryMarshal.GetReference(destination);
            ref int srcRef = ref MemoryMarshal.GetReference(values);

            while (count >= 8)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.As<int, byte>(ref Unsafe.Add(ref srcRef, pos)));
                var shuffled = Avx2.Shuffle(data, ShuffleMask32_256);
                shuffled.StoreUnsafe(ref Unsafe.Add(ref destRef, destPos));

                pos += 8;
                destPos += 32;
                count -= 8;
            }
        }

        // SSSE3 path: process 4 Int32s at a time (16 bytes)
        if (UseSsse3 && count >= 4)
        {
            ref byte destRef = ref MemoryMarshal.GetReference(destination);
            ref int srcRef = ref MemoryMarshal.GetReference(values);

            while (count >= 4)
            {
                var data = Vector128.LoadUnsafe(ref Unsafe.As<int, byte>(ref Unsafe.Add(ref srcRef, pos)));
                var shuffled = Ssse3.Shuffle(data, ShuffleMask32_128);
                shuffled.StoreUnsafe(ref Unsafe.Add(ref destRef, destPos));

                pos += 4;
                destPos += 16;
                count -= 4;
            }
        }

        // Scalar fallback for remaining values
        while (count > 0)
        {
            BinaryPrimitives.WriteInt32BigEndian(destination.Slice(destPos, 4), values[pos]);
            pos++;
            destPos += 4;
            count--;
        }
    }

    /// <summary>
    /// Write multiple Int16 values to a buffer in big-endian format using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt16sBigEndian(Span<byte> destination, ReadOnlySpan<short> values)
    {
        if (values.IsEmpty) return;

        var pos = 0;
        var destPos = 0;
        var count = values.Length;

        // Use scalar for small batches or when SIMD is disabled (-1)
        if (MinBatchSize < 0 || (MinBatchSize > 0 && count < MinBatchSize))
        {
            while (count > 0)
            {
                BinaryPrimitives.WriteInt16BigEndian(destination.Slice(destPos, 2), values[pos]);
                pos++;
                destPos += 2;
                count--;
            }
            return;
        }

        // SSSE3 path: process 8 Int16s at a time (16 bytes)
        if (UseSsse3 && count >= 8)
        {
            ref byte destRef = ref MemoryMarshal.GetReference(destination);
            ref short srcRef = ref MemoryMarshal.GetReference(values);

            while (count >= 8)
            {
                var data = Vector128.LoadUnsafe(ref Unsafe.As<short, byte>(ref Unsafe.Add(ref srcRef, pos)));
                var shuffled = Ssse3.Shuffle(data, ShuffleMask16_128);
                shuffled.StoreUnsafe(ref Unsafe.Add(ref destRef, destPos));

                pos += 8;
                destPos += 16;
                count -= 8;
            }
        }

        // Scalar fallback for remaining values
        while (count > 0)
        {
            BinaryPrimitives.WriteInt16BigEndian(destination.Slice(destPos, 2), values[pos]);
            pos++;
            destPos += 2;
            count--;
        }
    }

    /// <summary>
    /// Read multiple Int64 values from a big-endian buffer using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReadInt64sBigEndian(ReadOnlySpan<byte> source, Span<long> values)
    {
        if (values.IsEmpty) return;

        var srcPos = 0;
        var pos = 0;
        var count = values.Length;

        // Use scalar for small batches or when SIMD is disabled (-1)
        if (MinBatchSize < 0 || (MinBatchSize > 0 && count < MinBatchSize))
        {
            while (count > 0)
            {
                values[pos] = BinaryPrimitives.ReadInt64BigEndian(source.Slice(srcPos, 8));
                srcPos += 8;
                pos++;
                count--;
            }
            return;
        }

        // AVX2 path: process 4 Int64s at a time (32 bytes)
        if (UseAvx2 && count >= 4)
        {
            ref byte srcRef = ref MemoryMarshal.GetReference(source);
            ref long destRef = ref MemoryMarshal.GetReference(values);

            while (count >= 4)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref srcRef, srcPos));
                var shuffled = Avx2.Shuffle(data, ShuffleMask64_256);
                shuffled.StoreUnsafe(ref Unsafe.As<long, byte>(ref Unsafe.Add(ref destRef, pos)));

                srcPos += 32;
                pos += 4;
                count -= 4;
            }
        }

        // SSSE3 path: process 2 Int64s at a time (16 bytes)
        if (UseSsse3 && count >= 2)
        {
            ref byte srcRef = ref MemoryMarshal.GetReference(source);
            ref long destRef = ref MemoryMarshal.GetReference(values);

            while (count >= 2)
            {
                var data = Vector128.LoadUnsafe(ref Unsafe.Add(ref srcRef, srcPos));
                var shuffled = Ssse3.Shuffle(data, ShuffleMask64_128);
                shuffled.StoreUnsafe(ref Unsafe.As<long, byte>(ref Unsafe.Add(ref destRef, pos)));

                srcPos += 16;
                pos += 2;
                count -= 2;
            }
        }

        // Scalar fallback for remaining values
        while (count > 0)
        {
            values[pos] = BinaryPrimitives.ReadInt64BigEndian(source.Slice(srcPos, 8));
            srcPos += 8;
            pos++;
            count--;
        }
    }

    /// <summary>
    /// Read multiple Int32 values from a big-endian buffer using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReadInt32sBigEndian(ReadOnlySpan<byte> source, Span<int> values)
    {
        if (values.IsEmpty) return;

        var srcPos = 0;
        var pos = 0;
        var count = values.Length;

        // Use scalar for small batches or when SIMD is disabled (-1)
        if (MinBatchSize < 0 || (MinBatchSize > 0 && count < MinBatchSize))
        {
            while (count > 0)
            {
                values[pos] = BinaryPrimitives.ReadInt32BigEndian(source.Slice(srcPos, 4));
                srcPos += 4;
                pos++;
                count--;
            }
            return;
        }

        // AVX2 path: process 8 Int32s at a time (32 bytes)
        if (UseAvx2 && count >= 8)
        {
            ref byte srcRef = ref MemoryMarshal.GetReference(source);
            ref int destRef = ref MemoryMarshal.GetReference(values);

            while (count >= 8)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref srcRef, srcPos));
                var shuffled = Avx2.Shuffle(data, ShuffleMask32_256);
                shuffled.StoreUnsafe(ref Unsafe.As<int, byte>(ref Unsafe.Add(ref destRef, pos)));

                srcPos += 32;
                pos += 8;
                count -= 8;
            }
        }

        // SSSE3 path: process 4 Int32s at a time (16 bytes)
        if (UseSsse3 && count >= 4)
        {
            ref byte srcRef = ref MemoryMarshal.GetReference(source);
            ref int destRef = ref MemoryMarshal.GetReference(values);

            while (count >= 4)
            {
                var data = Vector128.LoadUnsafe(ref Unsafe.Add(ref srcRef, srcPos));
                var shuffled = Ssse3.Shuffle(data, ShuffleMask32_128);
                shuffled.StoreUnsafe(ref Unsafe.As<int, byte>(ref Unsafe.Add(ref destRef, pos)));

                srcPos += 16;
                pos += 4;
                count -= 4;
            }
        }

        // Scalar fallback for remaining values
        while (count > 0)
        {
            values[pos] = BinaryPrimitives.ReadInt32BigEndian(source.Slice(srcPos, 4));
            srcPos += 4;
            pos++;
            count--;
        }
    }

    /// <summary>
    /// Read multiple Int16 values from a big-endian buffer using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReadInt16sBigEndian(ReadOnlySpan<byte> source, Span<short> values)
    {
        if (values.IsEmpty) return;

        var srcPos = 0;
        var pos = 0;
        var count = values.Length;

        // Use scalar for small batches or when SIMD is disabled (-1)
        if (MinBatchSize < 0 || (MinBatchSize > 0 && count < MinBatchSize))
        {
            while (count > 0)
            {
                values[pos] = BinaryPrimitives.ReadInt16BigEndian(source.Slice(srcPos, 2));
                srcPos += 2;
                pos++;
                count--;
            }
            return;
        }

        // SSSE3 path: process 8 Int16s at a time (16 bytes)
        if (UseSsse3 && count >= 8)
        {
            ref byte srcRef = ref MemoryMarshal.GetReference(source);
            ref short destRef = ref MemoryMarshal.GetReference(values);

            while (count >= 8)
            {
                var data = Vector128.LoadUnsafe(ref Unsafe.Add(ref srcRef, srcPos));
                var shuffled = Ssse3.Shuffle(data, ShuffleMask16_128);
                shuffled.StoreUnsafe(ref Unsafe.As<short, byte>(ref Unsafe.Add(ref destRef, pos)));

                srcPos += 16;
                pos += 8;
                count -= 8;
            }
        }

        // Scalar fallback for remaining values
        while (count > 0)
        {
            values[pos] = BinaryPrimitives.ReadInt16BigEndian(source.Slice(srcPos, 2));
            srcPos += 2;
            pos++;
            count--;
        }
    }

    /// <summary>
    /// Write 2 consecutive Int64 values in big-endian format (common pattern: offset + timestamp).
    /// Optimized path using SSSE3 when available.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Write2Int64sBigEndian(Span<byte> destination, long value1, long value2)
    {
        if (UseSsse3)
        {
            Span<long> values = stackalloc long[2] { value1, value2 };
            ref byte destRef = ref MemoryMarshal.GetReference(destination);
            ref long srcRef = ref MemoryMarshal.GetReference(values);

            var data = Vector128.LoadUnsafe(ref Unsafe.As<long, byte>(ref srcRef));
            var shuffled = Ssse3.Shuffle(data, ShuffleMask64_128);
            shuffled.StoreUnsafe(ref destRef);
        }
        else
        {
            BinaryPrimitives.WriteInt64BigEndian(destination, value1);
            BinaryPrimitives.WriteInt64BigEndian(destination.Slice(8), value2);
        }
    }

    /// <summary>
    /// Write 4 consecutive Int32 values in big-endian format.
    /// Optimized path using SSSE3 when available.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Write4Int32sBigEndian(Span<byte> destination, int value1, int value2, int value3, int value4)
    {
        if (UseSsse3)
        {
            Span<int> values = stackalloc int[4] { value1, value2, value3, value4 };
            ref byte destRef = ref MemoryMarshal.GetReference(destination);
            ref int srcRef = ref MemoryMarshal.GetReference(values);

            var data = Vector128.LoadUnsafe(ref Unsafe.As<int, byte>(ref srcRef));
            var shuffled = Ssse3.Shuffle(data, ShuffleMask32_128);
            shuffled.StoreUnsafe(ref destRef);
        }
        else
        {
            BinaryPrimitives.WriteInt32BigEndian(destination, value1);
            BinaryPrimitives.WriteInt32BigEndian(destination.Slice(4), value2);
            BinaryPrimitives.WriteInt32BigEndian(destination.Slice(8), value3);
            BinaryPrimitives.WriteInt32BigEndian(destination.Slice(12), value4);
        }
    }

    /// <summary>
    /// Read 2 consecutive Int64 values from big-endian buffer.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (long value1, long value2) Read2Int64sBigEndian(ReadOnlySpan<byte> source)
    {
        if (UseSsse3)
        {
            Span<long> values = stackalloc long[2];
            ref byte srcRef = ref MemoryMarshal.GetReference(source);
            ref long destRef = ref MemoryMarshal.GetReference(values);

            var data = Vector128.LoadUnsafe(ref srcRef);
            var shuffled = Ssse3.Shuffle(data, ShuffleMask64_128);
            shuffled.StoreUnsafe(ref Unsafe.As<long, byte>(ref destRef));

            return (values[0], values[1]);
        }
        else
        {
            return (
                BinaryPrimitives.ReadInt64BigEndian(source),
                BinaryPrimitives.ReadInt64BigEndian(source.Slice(8))
            );
        }
    }

    /// <summary>
    /// Read 4 consecutive Int32 values from big-endian buffer.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (int value1, int value2, int value3, int value4) Read4Int32sBigEndian(ReadOnlySpan<byte> source)
    {
        if (UseSsse3)
        {
            Span<int> values = stackalloc int[4];
            ref byte srcRef = ref MemoryMarshal.GetReference(source);
            ref int destRef = ref MemoryMarshal.GetReference(values);

            var data = Vector128.LoadUnsafe(ref srcRef);
            var shuffled = Ssse3.Shuffle(data, ShuffleMask32_128);
            shuffled.StoreUnsafe(ref Unsafe.As<int, byte>(ref destRef));

            return (values[0], values[1], values[2], values[3]);
        }
        else
        {
            return (
                BinaryPrimitives.ReadInt32BigEndian(source),
                BinaryPrimitives.ReadInt32BigEndian(source.Slice(4)),
                BinaryPrimitives.ReadInt32BigEndian(source.Slice(8)),
                BinaryPrimitives.ReadInt32BigEndian(source.Slice(12))
            );
        }
    }
}
