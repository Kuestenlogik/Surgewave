using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// Benchmarks for compression codecs (None, GZIP, LZ4, ZSTD)
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Unit", "Compression")]
public class CompressionBenchmarks
{
    private byte[] _smallData = null!;
    private byte[] _mediumData = null!;
    private byte[] _largeData = null!;

    private byte[] _compressedGzipSmall = null!;
    private byte[] _compressedLz4Small = null!;
    private byte[] _compressedZstdSmall = null!;

    private byte[] _compressedGzipMedium = null!;
    private byte[] _compressedLz4Medium = null!;
    private byte[] _compressedZstdMedium = null!;

    [GlobalSetup]
    public void Setup()
    {
        // Create test data with some repetition (more compressible)
        _smallData = CreateCompressibleData(1024);        // 1KB
        _mediumData = CreateCompressibleData(64 * 1024);  // 64KB
        _largeData = CreateCompressibleData(1024 * 1024); // 1MB

        // Pre-compress for decompression benchmarks
        _compressedGzipSmall = CompressionCodec.Compress(_smallData, KafkaConstants.Compression.Gzip);
        _compressedLz4Small = CompressionCodec.Compress(_smallData, KafkaConstants.Compression.Lz4);
        _compressedZstdSmall = CompressionCodec.Compress(_smallData, KafkaConstants.Compression.Zstd);

        _compressedGzipMedium = CompressionCodec.Compress(_mediumData, KafkaConstants.Compression.Gzip);
        _compressedLz4Medium = CompressionCodec.Compress(_mediumData, KafkaConstants.Compression.Lz4);
        _compressedZstdMedium = CompressionCodec.Compress(_mediumData, KafkaConstants.Compression.Zstd);
    }

    private static byte[] CreateCompressibleData(int size)
    {
        var data = new byte[size];
        var random = new Random(42); // Fixed seed for reproducibility

        // Create semi-compressible data (mix of random and repeated patterns)
        for (int i = 0; i < size; i++)
        {
            if (i % 16 < 8)
            {
                data[i] = (byte)(i % 256); // Repeating pattern
            }
            else
            {
                data[i] = (byte)random.Next(256); // Random data
            }
        }

        return data;
    }

    // GZIP compression (baseline for comparison)
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Compress", "Small")]
    public byte[] Compress_Gzip_1KB() => CompressionCodec.Compress(_smallData, KafkaConstants.Compression.Gzip);

    [Benchmark]
    [BenchmarkCategory("Compress", "Medium")]
    public byte[] Compress_Gzip_64KB() => CompressionCodec.Compress(_mediumData, KafkaConstants.Compression.Gzip);

    // LZ4 compression
    [Benchmark]
    [BenchmarkCategory("Compress", "Small")]
    public byte[] Compress_LZ4_1KB() => CompressionCodec.Compress(_smallData, KafkaConstants.Compression.Lz4);

    [Benchmark]
    [BenchmarkCategory("Compress", "Medium")]
    public byte[] Compress_LZ4_64KB() => CompressionCodec.Compress(_mediumData, KafkaConstants.Compression.Lz4);

    [Benchmark]
    [BenchmarkCategory("Compress", "Large")]
    public byte[] Compress_LZ4_1MB() => CompressionCodec.Compress(_largeData, KafkaConstants.Compression.Lz4);

    // ZSTD compression
    [Benchmark]
    [BenchmarkCategory("Compress", "Small")]
    public byte[] Compress_Zstd_1KB() => CompressionCodec.Compress(_smallData, KafkaConstants.Compression.Zstd);

    [Benchmark]
    [BenchmarkCategory("Compress", "Medium")]
    public byte[] Compress_Zstd_64KB() => CompressionCodec.Compress(_mediumData, KafkaConstants.Compression.Zstd);

    [Benchmark]
    [BenchmarkCategory("Compress", "Large")]
    public byte[] Compress_Zstd_1MB() => CompressionCodec.Compress(_largeData, KafkaConstants.Compression.Zstd);

    // Decompression benchmarks
    [Benchmark]
    [BenchmarkCategory("Decompress", "Small")]
    public byte[] Decompress_Gzip_1KB() => CompressionCodec.Decompress(_compressedGzipSmall, KafkaConstants.Compression.Gzip);

    [Benchmark]
    [BenchmarkCategory("Decompress", "Small")]
    public byte[] Decompress_LZ4_1KB() => CompressionCodec.Decompress(_compressedLz4Small, KafkaConstants.Compression.Lz4);

    [Benchmark]
    [BenchmarkCategory("Decompress", "Small")]
    public byte[] Decompress_Zstd_1KB() => CompressionCodec.Decompress(_compressedZstdSmall, KafkaConstants.Compression.Zstd);

    [Benchmark]
    [BenchmarkCategory("Decompress", "Medium")]
    public byte[] Decompress_Gzip_64KB() => CompressionCodec.Decompress(_compressedGzipMedium, KafkaConstants.Compression.Gzip);

    [Benchmark]
    [BenchmarkCategory("Decompress", "Medium")]
    public byte[] Decompress_LZ4_64KB() => CompressionCodec.Decompress(_compressedLz4Medium, KafkaConstants.Compression.Lz4);

    [Benchmark]
    [BenchmarkCategory("Decompress", "Medium")]
    public byte[] Decompress_Zstd_64KB() => CompressionCodec.Decompress(_compressedZstdMedium, KafkaConstants.Compression.Zstd);
}
