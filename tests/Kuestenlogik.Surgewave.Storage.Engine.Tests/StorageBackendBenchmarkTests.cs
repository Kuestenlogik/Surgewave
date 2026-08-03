using System.Diagnostics;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Engine.Tests;

/// <summary>
/// Cross-backend workload coverage: every storage backend has to survive the same append, read and
/// mixed workloads and leave a consistent log behind. Throughput and latency are printed for
/// diagnostics.
///
/// <para><b>No absolute performance is asserted here, on purpose.</b> These tests run in CI as part
/// of the solution's unit tests, and a floor like "at least 100 ops/sec" measures the agent as much
/// as the code: this file's assertions failed locally at 64–82 ops/sec on a developer machine that
/// had been running benchmarks for hours, with the storage code byte-identical to a green run
/// earlier the same day. A test that fails for reasons unrelated to the change teaches people to
/// re-run until it passes, which is worse than not having it.</para>
///
/// <para>Throughput regressions are gated where the measurement is controlled: the benchmark suite
/// under <c>benchmarks/</c>, run as a paired A/B against a baseline, not as a unit test.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class StorageBackendBenchmarkTests : IDisposable
{
    private readonly string _tempDir;
    private readonly ITestOutputHelper _output;

    public StorageBackendBenchmarkTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-benchmark-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { }
        GC.SuppressFinalize(this);
    }

    [Theory]
    [InlineData(StorageBackend.File)]
    [InlineData(StorageBackend.ZeroCopyWal)]
    [InlineData(StorageBackend.ZeroCopyMemory)]
    [InlineData(StorageBackend.Memory)]
    public async Task Benchmark_AppendThroughput(StorageBackend backend)
    {
        // Arrange
        var factory = CreateFactory(backend);
        var tp = new TopicPartition { Topic = $"bench-append-{backend}", Partition = 0 };
        var dir = Path.Combine(_tempDir, backend.ToString());
        Directory.CreateDirectory(dir);

        using var log = new PartitionLog(dir, tp, factory);

        var batchCount = 1000;
        var recordsPerBatch = 10;
        var batches = Enumerable.Range(0, batchCount)
            .Select(i => CreateTestBatch(baseOffset: i * recordsPerBatch, recordCount: recordsPerBatch))
            .ToArray();

        // Warmup
        for (int i = 0; i < 10; i++)
        {
            await log.AppendBatchAsync(batches[i]);
        }

        // Measure
        var sw = Stopwatch.StartNew();
        for (int i = 10; i < batchCount; i++)
        {
            await log.AppendBatchAsync(batches[i]);
        }
        sw.Stop();

        var appendedBatches = batchCount - 10;
        var appendedRecords = appendedBatches * recordsPerBatch;
        var throughput = appendedRecords / sw.Elapsed.TotalSeconds;
        var latencyUs = sw.Elapsed.TotalMicroseconds / appendedBatches;

        _output.WriteLine($"[{backend}] Append: {appendedRecords:N0} records in {sw.ElapsedMilliseconds}ms");
        _output.WriteLine($"[{backend}] Throughput: {throughput:N0} records/sec");
        _output.WriteLine($"[{backend}] Avg latency: {latencyUs:F1} µs/batch");

        // What this test can verify is that the workload ran to completion and left the log
        // consistent — every appended record accounted for. The throughput is printed for
        // diagnostics but deliberately not asserted; see the note on the class.
        Assert.Equal(batchCount * recordsPerBatch, log.NextOffset);
    }

    [Theory]
    [InlineData(StorageBackend.File)]
    [InlineData(StorageBackend.ZeroCopyWal)]
    [InlineData(StorageBackend.ZeroCopyMemory)]
    [InlineData(StorageBackend.Memory)]
    public async Task Benchmark_ReadThroughput(StorageBackend backend)
    {
        // Arrange
        var factory = CreateFactory(backend);
        var tp = new TopicPartition { Topic = $"bench-read-{backend}", Partition = 0 };
        var dir = Path.Combine(_tempDir, backend.ToString(), "read");
        Directory.CreateDirectory(dir);

        using var log = new PartitionLog(dir, tp, factory);

        // Write test data
        var batchCount = 500;
        var recordsPerBatch = 10;
        for (int i = 0; i < batchCount; i++)
        {
            var batch = CreateTestBatch(baseOffset: i * recordsPerBatch, recordCount: recordsPerBatch);
            await log.AppendBatchAsync(batch);
        }

        // Warmup reads
        for (int i = 0; i < 5; i++)
        {
            await log.ReadBatchesAsync(0, maxBytes: 64 * 1024);
        }

        // Measure sequential reads
        var sw = Stopwatch.StartNew();
        var readIterations = 100;
        var totalBytesRead = 0L;
        var totalBatchesRead = 0;

        for (int i = 0; i < readIterations; i++)
        {
            var offset = (i * 50) % (batchCount * recordsPerBatch);
            var batches = await log.ReadBatchesAsync(offset, maxBytes: 64 * 1024);
            totalBatchesRead += batches.Count;
            totalBytesRead += batches.Sum(b => b.Length);
        }
        sw.Stop();

        var throughputMB = (totalBytesRead / (1024.0 * 1024.0)) / sw.Elapsed.TotalSeconds;
        var latencyUs = sw.Elapsed.TotalMicroseconds / readIterations;

        _output.WriteLine($"[{backend}] Read: {totalBatchesRead} batches, {totalBytesRead:N0} bytes in {sw.ElapsedMilliseconds}ms");
        _output.WriteLine($"[{backend}] Throughput: {throughputMB:F2} MB/sec");
        _output.WriteLine($"[{backend}] Avg latency: {latencyUs:F1} µs/read");

        // Basic sanity check
        Assert.True(totalBatchesRead > 0, "Should have read some batches");
    }

    [Theory]
    [InlineData(StorageBackend.File)]
    [InlineData(StorageBackend.ZeroCopyWal)]
    [InlineData(StorageBackend.ZeroCopyMemory)]
    [InlineData(StorageBackend.Memory)]
    public async Task Benchmark_MixedWorkload(StorageBackend backend)
    {
        // Arrange
        var factory = CreateFactory(backend);
        var tp = new TopicPartition { Topic = $"bench-mixed-{backend}", Partition = 0 };
        var dir = Path.Combine(_tempDir, backend.ToString(), "mixed");
        Directory.CreateDirectory(dir);

        using var log = new PartitionLog(dir, tp, factory);

        var iterations = 500;
        var recordsPerBatch = 5;

        // Measure mixed append + read workload
        var sw = Stopwatch.StartNew();
        var totalReads = 0;

        for (int i = 0; i < iterations; i++)
        {
            // Append
            var batch = CreateTestBatch(baseOffset: i * recordsPerBatch, recordCount: recordsPerBatch);
            await log.AppendBatchAsync(batch);

            // Read (every 5th iteration)
            if (i > 0 && i % 5 == 0)
            {
                var readOffset = Math.Max(0, (i - 10) * recordsPerBatch);
                var batches = await log.ReadBatchesAsync(readOffset, maxBytes: 32 * 1024);
                totalReads += batches.Count;
            }
        }
        sw.Stop();

        var opsPerSec = iterations / sw.Elapsed.TotalSeconds;

        _output.WriteLine($"[{backend}] Mixed: {iterations} appends, {totalReads} reads in {sw.ElapsedMilliseconds}ms");
        _output.WriteLine($"[{backend}] Throughput: {opsPerSec:N0} ops/sec");

        // Same as above: assert the workload's outcome, not the machine's speed. Every iteration
        // appended, and the reads in between returned data.
        Assert.Equal(iterations * recordsPerBatch, log.NextOffset);
        Assert.True(totalReads > 0, "the mixed workload never read anything back");
    }

    [Fact]
    public async Task Benchmark_MessageThroughput_AllBackends()
    {
        // Core storage backends (Arrow excluded - requires flush before read)
        var backendConfigs = new (string Name, ILogSegmentFactory Factory)[]
        {
            ("File", FileLogSegmentFactory.Create(useMmap: false)),
            ("ZeroCopyFile", FileLogSegmentFactory.Create(useMmap: true)),
            ("ZeroCopyMemory", ZeroCopyMemoryLogSegmentFactory.Create()),
            ("Memory", new MemoryLogSegmentFactory())
        };

        var messageSizes = new[] { 100, 1000, 10000 }; // 100B, 1KB, 10KB messages

        _output.WriteLine("");
        _output.WriteLine("=== Message Throughput Comparison (messages/sec) ===");
        _output.WriteLine("");

        foreach (var messageSize in messageSizes)
        {
            _output.WriteLine($"Message Size: {messageSize} bytes");
            _output.WriteLine(string.Format("{0,-20} {1,15} {2,15} {3,15}", "Backend", "Write msg/s", "Read msg/s", "MB/s Write"));
            _output.WriteLine(new string('-', 68));

            foreach (var (name, factory) in backendConfigs)
            {
                var tp = new TopicPartition { Topic = $"msg-bench-{name}-{messageSize}", Partition = 0 };
                var dir = Path.Combine(_tempDir, $"msg-{name}-{messageSize}");
                Directory.CreateDirectory(dir);

                using var log = new PartitionLog(dir, tp, factory);

                // Prepare batches with specified message size
                var messageCount = 10000;
                var messagesPerBatch = 10;
                var batchCount = messageCount / messagesPerBatch;
                var batches = new List<byte[]>();

                for (int i = 0; i < batchCount; i++)
                {
                    var batch = CreateTestBatchWithSize(
                        baseOffset: i * messagesPerBatch,
                        recordCount: messagesPerBatch,
                        valueSize: messageSize);
                    batches.Add(batch);
                }

                // Warmup
                for (int i = 0; i < Math.Min(10, batchCount); i++)
                {
                    await log.AppendBatchAsync(batches[i]);
                }

                // Write benchmark
                var swWrite = Stopwatch.StartNew();
                for (int i = 10; i < batchCount; i++)
                {
                    await log.AppendBatchAsync(batches[i]);
                }
                swWrite.Stop();

                var writtenMessages = (batchCount - 10) * messagesPerBatch;
                var writeMessagesPerSec = writtenMessages / swWrite.Elapsed.TotalSeconds;
                var writeMBPerSec = (writtenMessages * messageSize / (1024.0 * 1024.0)) / swWrite.Elapsed.TotalSeconds;

                // Read benchmark
                var swRead = Stopwatch.StartNew();
                var totalReadMessages = 0;
                var readIterations = 100;
                for (int i = 0; i < readIterations; i++)
                {
                    var offset = (i * 100) % messageCount;
                    var readBatches = await log.ReadBatchesAsync(offset, maxBytes: 256 * 1024);
                    totalReadMessages += readBatches.Count * messagesPerBatch;
                }
                swRead.Stop();

                var readMessagesPerSec = totalReadMessages / swRead.Elapsed.TotalSeconds;

                _output.WriteLine(string.Format("{0,-20} {1,15:N0} {2,15:N0} {3,15:F1}",
                    name, writeMessagesPerSec, readMessagesPerSec, writeMBPerSec));
            }
            _output.WriteLine("");
        }

        Assert.True(true); // Test passes if it runs without error
    }

    [Fact]
    public async Task Benchmark_CompareAllBackends_Summary()
    {
        var backends = new[] { StorageBackend.File, StorageBackend.ZeroCopyWal, StorageBackend.ZeroCopyMemory, StorageBackend.Memory };
        var results = new Dictionary<StorageBackend, (double appendThroughput, double readThroughput)>();

        foreach (var backend in backends)
        {
            var factory = CreateFactory(backend);
            var tp = new TopicPartition { Topic = $"bench-compare-{backend}", Partition = 0 };
            var dir = Path.Combine(_tempDir, $"compare-{backend}");
            Directory.CreateDirectory(dir);

            using var log = new PartitionLog(dir, tp, factory);

            // Append benchmark
            var batchCount = 500;
            var recordsPerBatch = 10;
            var batches = Enumerable.Range(0, batchCount)
                .Select(i => CreateTestBatch(baseOffset: i * recordsPerBatch, recordCount: recordsPerBatch))
                .ToArray();

            var swAppend = Stopwatch.StartNew();
            for (int i = 0; i < batchCount; i++)
            {
                await log.AppendBatchAsync(batches[i]);
            }
            swAppend.Stop();
            var appendThroughput = (batchCount * recordsPerBatch) / swAppend.Elapsed.TotalSeconds;

            // Read benchmark
            var swRead = Stopwatch.StartNew();
            var readIterations = 50;
            for (int i = 0; i < readIterations; i++)
            {
                await log.ReadBatchesAsync(i * 100, maxBytes: 64 * 1024);
            }
            swRead.Stop();
            var readThroughput = readIterations / swRead.Elapsed.TotalSeconds;

            results[backend] = (appendThroughput, readThroughput);
        }

        // Output summary
        _output.WriteLine("");
        _output.WriteLine("=== Storage Backend Comparison ===");
        _output.WriteLine(string.Format("{0,-20} {1,15} {2,15}", "Backend", "Append (rec/s)", "Read (ops/s)"));
        _output.WriteLine(new string('-', 52));

        foreach (var (backend, (append, read)) in results.OrderByDescending(r => r.Value.appendThroughput))
        {
            _output.WriteLine(string.Format("{0,-20} {1,15:N0} {2,15:N0}", backend, append, read));
        }

        // All backends should work
        Assert.Equal(4, results.Count);
    }

    /// <summary>
    /// Create a Kafka RecordBatch with specified message value size.
    /// </summary>
    private static byte[] CreateTestBatchWithSize(long baseOffset, int recordCount, int valueSize)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var valueData = new byte[valueSize];
        System.Security.Cryptography.RandomNumberGenerator.Fill(valueData);

        // Base Offset (8 bytes, big-endian)
        WriteBigEndian(writer, baseOffset);

        // Placeholder for batch length
        var batchLengthPos = stream.Position;
        WriteBigEndian(writer, 0);

        var batchDataStart = stream.Position;

        // Partition Leader Epoch
        WriteBigEndian(writer, 0);
        // Magic
        writer.Write((byte)2);
        // CRC placeholder
        WriteBigEndian(writer, 0u);
        // Attributes
        WriteBigEndian(writer, (short)0);
        // Last Offset Delta
        WriteBigEndian(writer, recordCount - 1);
        // Base Timestamp
        WriteBigEndian(writer, timestamp);
        // Max Timestamp
        WriteBigEndian(writer, timestamp);
        // Producer ID
        WriteBigEndian(writer, -1L);
        // Producer Epoch
        WriteBigEndian(writer, (short)-1);
        // Base Sequence
        WriteBigEndian(writer, -1);
        // Record Count
        WriteBigEndian(writer, recordCount);

        // Write records
        for (int i = 0; i < recordCount; i++)
        {
            WriteRecord(writer, valueData, i);
        }

        // Update batch length
        var batchLength = (int)(stream.Position - batchDataStart);
        var endPos = stream.Position;
        stream.Position = batchLengthPos;
        WriteBigEndian(writer, batchLength);
        stream.Position = endPos;

        return stream.ToArray();
    }

    /// <summary>
    /// Create a minimal valid Kafka RecordBatch for testing.
    /// </summary>
    private static byte[] CreateTestBatch(long baseOffset, int recordCount)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var valueData = new byte[100];
        System.Security.Cryptography.RandomNumberGenerator.Fill(valueData);

        // Base Offset (8 bytes, big-endian)
        WriteBigEndian(writer, baseOffset);

        // Placeholder for batch length
        var batchLengthPos = stream.Position;
        WriteBigEndian(writer, 0);

        var batchDataStart = stream.Position;

        // Partition Leader Epoch
        WriteBigEndian(writer, 0);
        // Magic
        writer.Write((byte)2);
        // CRC placeholder
        WriteBigEndian(writer, 0u);
        // Attributes
        WriteBigEndian(writer, (short)0);
        // Last Offset Delta
        WriteBigEndian(writer, recordCount - 1);
        // Base Timestamp
        WriteBigEndian(writer, timestamp);
        // Max Timestamp
        WriteBigEndian(writer, timestamp);
        // Producer ID
        WriteBigEndian(writer, -1L);
        // Producer Epoch
        WriteBigEndian(writer, (short)-1);
        // Base Sequence
        WriteBigEndian(writer, -1);
        // Record Count
        WriteBigEndian(writer, recordCount);

        // Write records
        for (int i = 0; i < recordCount; i++)
        {
            WriteRecord(writer, valueData, i);
        }

        // Update batch length
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

        recordWriter.Write((byte)0);           // Attributes
        WriteVarInt(recordWriter, 0);          // Timestamp delta
        WriteVarInt(recordWriter, offsetDelta); // Offset delta
        WriteVarInt(recordWriter, -1);         // Key length (null)
        WriteVarInt(recordWriter, value.Length);
        recordWriter.Write(value);
        WriteVarInt(recordWriter, 0);          // Headers count

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

    private static ILogSegmentFactory CreateFactory(StorageBackend backend)
    {
        return backend switch
        {
            StorageBackend.File => FileLogSegmentFactory.Create(useMmap: false),
            StorageBackend.Memory => new MemoryLogSegmentFactory(),
            StorageBackend.ZeroCopyWal => FileLogSegmentFactory.Create(useMmap: true),
            StorageBackend.ZeroCopyMemory => ZeroCopyMemoryLogSegmentFactory.Create(),
            _ => throw new ArgumentOutOfRangeException(nameof(backend), backend, null)
        };
    }
}
