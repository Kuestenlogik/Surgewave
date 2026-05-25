using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Protocol.Kafka;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// Benchmarks for SIMD-optimized VarInt scanning operations.
/// Tests VarInt counting, record offset scanning, and batch parsing.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Unit", "Simd", "Protocol")]
public class VarIntScannerBenchmarks
{
    private byte[] _singleByteVarInts = null!;
    private byte[] _mixedVarInts = null!;
    private byte[] _recordsData = null!;
    private int[] _recordOffsets = null!;
    private int _recordCount;

    [GlobalSetup]
    public void Setup()
    {
        Console.WriteLine($"SIMD VarInt Implementation: {SimdVarIntScanner.Implementation}");

        // Create array of single-byte VarInts (0-127, most common case)
        _singleByteVarInts = new byte[1024];
        for (int i = 0; i < _singleByteVarInts.Length; i++)
        {
            _singleByteVarInts[i] = (byte)(i % 128);
        }

        // Create array of mixed VarInts (1-3 bytes each)
        using var ms = new MemoryStream();
        var random = new Random(42);
        for (int i = 0; i < 500; i++)
        {
            int value = random.Next(0, 2097152); // Up to 3-byte VarInt
            var buffer = new byte[5];
            int len = KafkaProtocolPrimitives.WriteVarInt(buffer, value);
            ms.Write(buffer, 0, len);
        }
        _mixedVarInts = ms.ToArray();

        // Create simulated record batch records data
        // Each record: length(varint) + attributes(1) + timestampDelta(varint) + offsetDelta(varint) +
        //              keyLen(varint) + key + valueLen(varint) + value + headerCount(varint)
        _recordCount = 100;
        _recordOffsets = new int[_recordCount];
        using var recordsMs = new MemoryStream();
        var recordBuffer = new byte[256];

        for (int i = 0; i < _recordCount; i++)
        {
            int recordStart = (int)recordsMs.Position;

            // Build record content first
            using var recordContent = new MemoryStream();

            // attributes (1 byte)
            recordContent.WriteByte(0);

            // timestampDelta (zigzag varlong) - small values
            var tsBuffer = new byte[10];
            int tsLen = KafkaProtocolPrimitives.WriteVarLong(tsBuffer, (long)KafkaProtocolPrimitives.ZigzagEncode((long)(i * 10)));
            recordContent.Write(tsBuffer, 0, tsLen);

            // offsetDelta (zigzag varint)
            var offsetBuffer = new byte[5];
            int offsetLen = KafkaProtocolPrimitives.WriteVarInt(offsetBuffer, (int)KafkaProtocolPrimitives.ZigzagEncode(i));
            recordContent.Write(offsetBuffer, 0, offsetLen);

            // keyLen (zigzag) + key
            int keyLen = 16;
            var keyLenBuffer = new byte[5];
            int keyLenBytes = KafkaProtocolPrimitives.WriteVarInt(keyLenBuffer, (int)KafkaProtocolPrimitives.ZigzagEncode(keyLen));
            recordContent.Write(keyLenBuffer, 0, keyLenBytes);
            recordContent.Write(new byte[keyLen], 0, keyLen);

            // valueLen (zigzag) + value
            int valueLen = 64;
            var valueLenBuffer = new byte[5];
            int valueLenBytes = KafkaProtocolPrimitives.WriteVarInt(valueLenBuffer, (int)KafkaProtocolPrimitives.ZigzagEncode(valueLen));
            recordContent.Write(valueLenBuffer, 0, valueLenBytes);
            recordContent.Write(new byte[valueLen], 0, valueLen);

            // headerCount = 0
            recordContent.WriteByte(0);

            // Write record length prefix (zigzag encoded)
            int recordLength = (int)recordContent.Length;
            var lengthBuffer = new byte[5];
            int lengthBytes = KafkaProtocolPrimitives.WriteVarInt(lengthBuffer, (int)KafkaProtocolPrimitives.ZigzagEncode(recordLength));
            recordsMs.Write(lengthBuffer, 0, lengthBytes);

            // Write record content
            recordContent.Position = 0;
            recordContent.CopyTo(recordsMs);
        }

        _recordsData = recordsMs.ToArray();
        Console.WriteLine($"Records data size: {_recordsData.Length} bytes, {_recordCount} records");
    }

    // === VarInt Counting Benchmarks ===

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Count")]
    public int CountVarInts_Scalar()
    {
        int count = 0;
        for (int i = 0; i < _singleByteVarInts.Length; i++)
        {
            if ((_singleByteVarInts[i] & 0x80) == 0)
            {
                count++;
            }
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("Count")]
    public int CountVarInts_SIMD()
    {
        return SimdVarIntScanner.CountVarInts(_singleByteVarInts);
    }

    [Benchmark]
    [BenchmarkCategory("Count", "Mixed")]
    public int CountVarInts_Mixed_Scalar()
    {
        int count = 0;
        for (int i = 0; i < _mixedVarInts.Length; i++)
        {
            if ((_mixedVarInts[i] & 0x80) == 0)
            {
                count++;
            }
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("Count", "Mixed")]
    public int CountVarInts_Mixed_SIMD()
    {
        return SimdVarIntScanner.CountVarInts(_mixedVarInts);
    }

    // === VarInt Reading Benchmarks ===

    [Benchmark]
    [BenchmarkCategory("Read")]
    public int ReadVarInt_Scalar()
    {
        int pos = 0;
        int sum = 0;
        while (pos < _mixedVarInts.Length)
        {
            var (value, bytesRead) = KafkaProtocolPrimitives.ReadVarInt(_mixedVarInts.AsSpan(pos));
            sum += value;
            pos += bytesRead;
        }
        return sum;
    }

    [Benchmark]
    [BenchmarkCategory("Read")]
    public int ReadVarInt_SIMD()
    {
        int pos = 0;
        int sum = 0;
        while (pos < _mixedVarInts.Length)
        {
            var (value, length) = SimdVarIntScanner.ReadVarIntFast(_mixedVarInts.AsSpan(pos));
            if (length == 0) break;
            sum += value;
            pos += length;
        }
        return sum;
    }

    // === Record Offset Scanning Benchmarks ===

    [Benchmark]
    [BenchmarkCategory("Scan")]
    public int ScanRecordOffsets_Scalar()
    {
        return SimdVarIntScanner.ScanRecordOffsets(_recordsData, _recordOffsets, _recordCount);
    }

    [Benchmark]
    [BenchmarkCategory("Scan")]
    public int ScanRecordOffsets_SIMD()
    {
        return SimdVarIntScanner.ScanRecordOffsetsSimd(_recordsData, _recordOffsets, _recordCount);
    }

    // === Batch VarInt Reading Benchmarks ===

    private int[] _batchValues = new int[100];

    [Benchmark]
    [BenchmarkCategory("Batch")]
    public int BatchReadVarInts()
    {
        return SimdVarIntScanner.BatchReadVarInts(_mixedVarInts, _batchValues, out _);
    }

    // === Skip VarInt Benchmarks ===

    [Benchmark]
    [BenchmarkCategory("Skip")]
    public int SkipVarInts_Scalar()
    {
        int pos = 0;
        int count = 0;
        while (pos < _mixedVarInts.Length)
        {
            // Scalar skip
            int len = 0;
            while (pos + len < _mixedVarInts.Length && (_mixedVarInts[pos + len] & 0x80) != 0)
            {
                len++;
            }
            len++; // Include terminator
            pos += len;
            count++;
        }
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("Skip")]
    public int SkipVarInts_SIMD()
    {
        int pos = 0;
        int count = 0;
        while (pos < _mixedVarInts.Length)
        {
            int len = SimdVarIntScanner.SkipVarInt(_mixedVarInts.AsSpan(pos));
            if (len == 0) break;
            pos += len;
            count++;
        }
        return count;
    }

    // === Terminator Finding Benchmarks ===

    [Benchmark]
    [BenchmarkCategory("Terminators")]
    public uint FindTerminators16_SIMD()
    {
        uint sum = 0;
        for (int i = 0; i + 16 <= _mixedVarInts.Length; i += 16)
        {
            sum += SimdVarIntScanner.FindVarIntTerminators16(_mixedVarInts.AsSpan(i, 16));
        }
        return sum;
    }

    [Benchmark]
    [BenchmarkCategory("Terminators")]
    public uint FindTerminators32_SIMD()
    {
        uint sum = 0;
        for (int i = 0; i + 32 <= _mixedVarInts.Length; i += 32)
        {
            sum += SimdVarIntScanner.FindVarIntTerminators32(_mixedVarInts.AsSpan(i, 32));
        }
        return sum;
    }
}
