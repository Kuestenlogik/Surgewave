using System.Runtime.Intrinsics.X86;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// #85 S2 merge gate: the 3-way interleaved x64 CRC32C kernel MUST be bit-identical to both the serial
/// SSE4.2 chain and the software table implementation for every input length and alignment. The existing
/// golden-vector tests all use small inputs that take the serial path, so this cross-implementation fuzz
/// is the ONLY coverage of the interleaved kernel — a wrong recombine would corrupt every large batch's
/// CRC while still looking random. <see cref="Crc32C.ComputeSoftware"/> (a different algorithm family:
/// reflected table, poly 0x82F63B78) is the independent oracle; there is no framework CRC32C to compare
/// against (System.IO.Hashing.Crc32 is ISO-HDLC, a different polynomial).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class Crc32CInterleavedFuzzTests
{
    [Fact]
    public void Interleaved_IsBitIdentical_ToSoftwareAndSerial_AcrossAllLengthsAndOffsets()
    {
        if (!Sse42.X64.IsSupported)
            return; // interleaved kernel exists only on x64 SSE4.2 hardware (CI ubuntu-x64 exercises it)

        // Deterministic pseudo-random payload; the +16 lets us slice at unaligned base offsets.
        var rng = new Random(20260719);
        var buf = new byte[8192 + 16];
        rng.NextBytes(buf);

        // Fully dense over 0..8192 covers the interleave threshold (3072 = 3*L), the 3L block boundary,
        // and every len%8 tail split; unaligned offsets exercise the ReadUnaligned sub-stream reads.
        int[] offsets = [0, 1, 2, 3, 7];
        for (var len = 0; len <= 8192; len++)
        {
            foreach (var off in offsets)
            {
                var span = buf.AsSpan(off, len);
                var software = Crc32C.ComputeSoftware(span);
                var serial = Crc32C.ComputeSse42X64Serial(span);
                var interleaved = Crc32C.ComputeSse42X64Interleaved(span);

                Assert.True(serial == software,
                    $"serial != software at len={len} off={off}: {serial:X8} vs {software:X8}");
                Assert.True(interleaved == software,
                    $"interleaved != software at len={len} off={off}: {interleaved:X8} vs {software:X8}");
            }
        }
    }

    [Fact]
    public void PublicCompute_MatchesSoftware_ForLargeBuffers()
    {
        if (!Sse42.X64.IsSupported)
            return; // x64 SSE4.2 only

        // The public dispatch takes the interleaved path above the threshold; pin it against software at
        // a few realistic large/compressed-batch sizes and awkward remainders.
        var rng = new Random(424242);
        foreach (var len in new[] { 3071, 3072, 3073, 4096, 6144, 10000, 65536, 65537 })
        {
            var data = new byte[len];
            rng.NextBytes(data);
            Assert.Equal(Crc32C.ComputeSoftware(data), Crc32C.Compute(data));
        }
    }
}
