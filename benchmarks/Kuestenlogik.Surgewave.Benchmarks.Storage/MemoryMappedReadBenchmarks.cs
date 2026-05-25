using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;

namespace Kuestenlogik.Surgewave.Benchmarks.Storage;

/// <summary>
/// Benchmarks comparing standard FileStream reads vs Memory-Mapped reads.
/// This measures the performance difference between the two I/O approaches.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Storage")]
public class MemoryMappedReadBenchmarks : IDisposable
{
    private string _tempDirectory = null!;
    private string _logFilePath = null!;
    private FileStorageEngine _engine = null!;
    private StorageEngineSegmentAdapter _segment = null!;
    private MemoryMappedLogReader _mmapReader = null!;
    private long _fileSize;

    [Params(4096, 65536, 1048576)] // 4KB, 64KB, 1MB
    public int ReadSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), "surgewave-mmap-benchmarks", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDirectory);

        // Create and populate segment, then close it
        using (var engine = new FileStorageEngine(_tempDirectory, baseOffset: 0, createNew: true))
        using (var segment = new StorageEngineSegmentAdapter(engine))
        {
            var recordBatch = CreateRecordBatch(1024, 10);  // 1KB batches
            for (int i = 0; i < 2000; i++)  // ~2MB of data
            {
                segment.AppendBatchAsync(recordBatch).AsTask().Wait();
            }
            segment.FlushAsync().AsTask().Wait();
        }

        _logFilePath = Path.Combine(_tempDirectory, "00000000000000000000.log");
        _fileSize = new FileInfo(_logFilePath).Length;

        // Reopen segment for read tests (with FileShare.Read)
        _engine = new FileStorageEngine(_tempDirectory, baseOffset: 0, createNew: false);
        _segment = new StorageEngineSegmentAdapter(_engine);

        // Create memory-mapped reader
        _mmapReader = new MemoryMappedLogReader(_logFilePath);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        Dispose();
    }

    public void Dispose()
    {
        _mmapReader?.Dispose();
        _segment?.Dispose();
        _engine?.Dispose();
        try
        {
            if (Directory.Exists(_tempDirectory))
            {
                Directory.Delete(_tempDirectory, recursive: true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Standard FileStream read approach (traditional I/O)
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Read")]
    public byte[] StandardFileStreamRead()
    {
        using var fs = new FileStream(_logFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var buffer = new byte[ReadSize];
        var bytesToRead = Math.Min(ReadSize, (int)_fileSize);
        fs.ReadExactly(buffer, 0, bytesToRead);
        return buffer;
    }

    /// <summary>
    /// Memory-mapped file read approach (zero-copy potential)
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Read")]
    public List<byte[]> MemoryMappedRead()
    {
        return _mmapReader.ReadBatches(0, ReadSize);
    }

    /// <summary>
    /// LogSegment's async read approach
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Read")]
    public List<byte[]> LogSegmentAsyncRead()
    {
        return _segment.ReadBatchesAsync(0, ReadSize).AsTask().Result;
    }

    private static byte[] CreateRecordBatch(int valueSize, int recordCount)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var valueData = new byte[valueSize];
        Random.Shared.NextBytes(valueData);

        WriteBigEndian(writer, 0L);
        var batchLengthPos = stream.Position;
        WriteBigEndian(writer, 0);
        var batchDataStart = stream.Position;

        WriteBigEndian(writer, 0);
        writer.Write((byte)2);
        WriteBigEndian(writer, 0u);
        WriteBigEndian(writer, (short)0);
        WriteBigEndian(writer, recordCount - 1);
        WriteBigEndian(writer, timestamp);
        WriteBigEndian(writer, timestamp);
        WriteBigEndian(writer, -1L);
        WriteBigEndian(writer, (short)-1);
        WriteBigEndian(writer, -1);
        WriteBigEndian(writer, recordCount);

        for (int i = 0; i < recordCount; i++)
        {
            WriteRecord(writer, valueData, i);
        }

        var batchLength = (int)(stream.Position - batchDataStart);
        var endPos = stream.Position;
        stream.Position = batchLengthPos;
        WriteBigEndian(writer, batchLength);
        stream.Position = endPos;

        return stream.ToArray();
    }

    private static void WriteRecord(BinaryWriter writer, byte[] value, int offsetDelta)
    {
        using var recordStream = new MemoryStream();
        using var recordWriter = new BinaryWriter(recordStream);

        recordWriter.Write((byte)0);
        WriteVarInt(recordWriter, 0);
        WriteVarInt(recordWriter, offsetDelta);
        WriteVarInt(recordWriter, -1);
        WriteVarInt(recordWriter, value.Length);
        recordWriter.Write(value);
        WriteVarInt(recordWriter, 0);

        var recordBytes = recordStream.ToArray();
        WriteVarInt(writer, recordBytes.Length);
        writer.Write(recordBytes);
    }

    private static void WriteBigEndian(BinaryWriter writer, short value)
    {
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, int value)
    {
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, long value)
    {
        writer.Write((byte)(value >> 56));
        writer.Write((byte)(value >> 48));
        writer.Write((byte)(value >> 40));
        writer.Write((byte)(value >> 32));
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteBigEndian(BinaryWriter writer, uint value)
    {
        writer.Write((byte)(value >> 24));
        writer.Write((byte)(value >> 16));
        writer.Write((byte)(value >> 8));
        writer.Write((byte)value);
    }

    private static void WriteVarInt(BinaryWriter writer, int value)
    {
        var v = (uint)((value << 1) ^ (value >> 31));
        while ((v & ~0x7F) != 0)
        {
            writer.Write((byte)((v & 0x7F) | 0x80));
            v >>= 7;
        }
        writer.Write((byte)v);
    }
}
