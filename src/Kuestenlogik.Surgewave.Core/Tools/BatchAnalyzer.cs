using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Core.Tools;

/// <summary>
/// Diagnostic tool to analyze RecordBatch format and CRC checksums
/// </summary>
public static class BatchAnalyzer
{
    public static void AnalyzeBatch(byte[] batch)
    {
        if (batch.Length < 61)
        {
            Console.WriteLine($"ERROR: Batch too small ({batch.Length} bytes, need at least 61)");
            return;
        }

        // Parse header
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(batch.AsSpan(0, 8));
        var batchLength = BinaryPrimitives.ReadInt32BigEndian(batch.AsSpan(8, 4));
        var partitionLeaderEpoch = BinaryPrimitives.ReadInt32BigEndian(batch.AsSpan(12, 4));
        var magic = batch[16];
        var storedCrc = BinaryPrimitives.ReadUInt32BigEndian(batch.AsSpan(17, 4));

        Console.WriteLine($"=== RecordBatch Analysis ===");
        Console.WriteLine($"Total batch size: {batch.Length} bytes");
        Console.WriteLine($"BaseOffset: {baseOffset}");
        Console.WriteLine($"BatchLength: {batchLength} (expected total: {12 + batchLength})");
        Console.WriteLine($"PartitionLeaderEpoch: {partitionLeaderEpoch}");
        Console.WriteLine($"Magic: {magic}");
        Console.WriteLine($"Stored CRC: 0x{storedCrc:X8}");

        // Calculate CRC
        var crcData = batch.AsSpan(21);
        var calculatedCrc = Crc32C.Compute(crcData);
        Console.WriteLine($"Calculated CRC: 0x{calculatedCrc:X8}");

        if (storedCrc == calculatedCrc)
        {
            Console.WriteLine("✓ CRC MATCHES");
        }
        else
        {
            Console.WriteLine("✗ CRC MISMATCH!");
        }

        // Show first 80 bytes as hex
        var hexLen = Math.Min(80, batch.Length);
        var hex = string.Join("-", batch.Take(hexLen).Select(b => b.ToString("X2")));
        Console.WriteLine($"\nFirst {hexLen} bytes:");
        Console.WriteLine(hex);

        // Parse attributes and record count if possible
        if (batch.Length >= 61)
        {
            var attributes = BinaryPrimitives.ReadInt16BigEndian(batch.AsSpan(21, 2));
            var lastOffsetDelta = BinaryPrimitives.ReadInt32BigEndian(batch.AsSpan(23, 4));
            var baseTimestamp = BinaryPrimitives.ReadInt64BigEndian(batch.AsSpan(27, 8));
            var maxTimestamp = BinaryPrimitives.ReadInt64BigEndian(batch.AsSpan(35, 8));
            var producerId = BinaryPrimitives.ReadInt64BigEndian(batch.AsSpan(43, 8));
            var producerEpoch = BinaryPrimitives.ReadInt16BigEndian(batch.AsSpan(51, 2));
            var baseSequence = BinaryPrimitives.ReadInt32BigEndian(batch.AsSpan(53, 4));
            var recordCount = BinaryPrimitives.ReadInt32BigEndian(batch.AsSpan(57, 4));

            Console.WriteLine($"\nAttributes: 0x{attributes:X4}");
            Console.WriteLine($"LastOffsetDelta: {lastOffsetDelta}");
            Console.WriteLine($"BaseTimestamp: {baseTimestamp}");
            Console.WriteLine($"MaxTimestamp: {maxTimestamp}");
            Console.WriteLine($"ProducerId: {producerId}");
            Console.WriteLine($"ProducerEpoch: {producerEpoch}");
            Console.WriteLine($"BaseSequence: {baseSequence}");
            Console.WriteLine($"RecordCount: {recordCount}");
        }
    }

    public static void AnalyzeLogFile(string logFilePath)
    {
        if (!File.Exists(logFilePath))
        {
            Console.WriteLine($"ERROR: File not found: {logFilePath}");
            return;
        }

        var allBytes = File.ReadAllBytes(logFilePath);
        Console.WriteLine($"=== Analyzing Log File ===");
        Console.WriteLine($"File: {logFilePath}");
        Console.WriteLine($"Total size: {allBytes.Length} bytes\n");

        int batchNum = 0;
        int position = 0;

        while (position < allBytes.Length)
        {
            if (position + 12 > allBytes.Length)
            {
                Console.WriteLine($"ERROR: Not enough bytes for next batch header at position {position}");
                break;
            }

            var batchLength = BinaryPrimitives.ReadInt32BigEndian(allBytes.AsSpan(position + 8, 4));
            var totalBatchSize = 12 + batchLength;

            if (position + totalBatchSize > allBytes.Length)
            {
                Console.WriteLine($"ERROR: Batch {batchNum} declares size {totalBatchSize} but only {allBytes.Length - position} bytes remaining");
                break;
            }

            Console.WriteLine($"\n--- Batch {batchNum} (position {position}) ---");
            var batch = allBytes.AsSpan(position, totalBatchSize).ToArray();
            AnalyzeBatch(batch);

            position += totalBatchSize;
            batchNum++;
        }

        Console.WriteLine($"\n=== Summary ===");
        Console.WriteLine($"Total batches analyzed: {batchNum}");
        Console.WriteLine($"Total bytes consumed: {position}");
    }
}
