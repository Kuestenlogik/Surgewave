using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Broker.AdaptiveCompression;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

[Trait("Category", TestCategories.Unit)]
public sealed class AdaptiveCompressionServiceTests : IAsyncLifetime, IDisposable
{
    private readonly string _testDirectory;
    private readonly LogManager _logManager;

    public AdaptiveCompressionServiceTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"surgewave-adaptive-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);
        _logManager = new LogManager(_testDirectory, new MemoryLogSegmentFactory());
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Fact]
    public void Config_Defaults_AreSafe()
    {
        var config = new AdaptiveCompressionConfig();
        Assert.False(config.Enabled);
        Assert.Equal(30, config.ScanIntervalSeconds);
        Assert.Equal(1024 * 1024, config.MaxScanBytesPerPartition);
        Assert.Equal(100, config.SampleEveryNthRecord);
        Assert.Equal(50, config.MinSampleCount);
        Assert.Equal("auto", AdaptiveCompressionConfig.AutoMarker);
    }

    [Fact]
    public async Task ScanOnce_IgnoresTopicsWithoutAutoMarker()
    {
        // Topic has compression.type=lz4 — service should not touch it.
        await _logManager.CreateTopicAsync(
            "explicit-topic",
            partitionCount: 1,
            config: new Dictionary<string, string> { ["compression.type"] = "lz4" });

        var service = CreateService(samplerMinSamples: 1, sampleEveryN: 1);

        await service.ScanOnceAsync(CancellationToken.None);

        Assert.Equal(0, service.ActiveSamplerCount);
        var meta = _logManager.GetTopicMetadata("explicit-topic")!;
        Assert.Equal("lz4", meta.Config["compression.type"]);
    }

    [Fact]
    public async Task ScanOnce_CreatesSamplerForAutoTopic()
    {
        await _logManager.CreateTopicAsync(
            "auto-topic",
            partitionCount: 1,
            config: new Dictionary<string, string> { ["compression.type"] = "auto" });

        // Topic exists but has no batches yet → sampler is created but no decision.
        var service = CreateService(samplerMinSamples: 50, sampleEveryN: 100);

        await service.ScanOnceAsync(CancellationToken.None);

        Assert.Equal(1, service.ActiveSamplerCount);
        Assert.Equal("auto", _logManager.GetTopicMetadata("auto-topic")!.Config["compression.type"]);
    }

    [Fact]
    public async Task ScanOnce_WithEnoughBatches_WritesDecidedCodecBack()
    {
        await _logManager.CreateTopicAsync(
            "compressible-topic",
            partitionCount: 1,
            config: new Dictionary<string, string> { ["compression.type"] = "auto" });

        // Append highly compressible payloads so any real codec beats 'none'.
        // Repeating-string records are the textbook case where lz4/zstd win.
        var topicPartition = new TopicPartition { Topic = "compressible-topic", Partition = 0 };
        for (var i = 0; i < 5; i++)
        {
            var batch = CreateRecordBatchWithPayload(
                Encoding.UTF8.GetBytes(new string('A', 2048)));
            await _logManager.AppendBatchAsync(topicPartition, batch);
        }

        // sample-every-1 + min-sample-count=1 → decision after the first batch
        // observed.
        var service = CreateService(samplerMinSamples: 1, sampleEveryN: 1);

        await service.ScanOnceAsync(CancellationToken.None);

        var resolved = _logManager.GetTopicMetadata("compressible-topic")!.Config["compression.type"];
        Assert.NotEqual("auto", resolved);
        Assert.Contains(resolved, new[] { "none", "snappy", "lz4", "zstd" });

        // After decision, the sampler is evicted.
        Assert.Equal(0, service.ActiveSamplerCount);
    }

    [Fact]
    public async Task ScanOnce_DecidedTopic_DoesNotResample()
    {
        await _logManager.CreateTopicAsync(
            "auto-topic",
            partitionCount: 1,
            config: new Dictionary<string, string> { ["compression.type"] = "auto" });

        var topicPartition = new TopicPartition { Topic = "auto-topic", Partition = 0 };
        await _logManager.AppendBatchAsync(
            topicPartition,
            CreateRecordBatchWithPayload(Encoding.UTF8.GetBytes(new string('B', 1024))));

        var service = CreateService(samplerMinSamples: 1, sampleEveryN: 1);

        await service.ScanOnceAsync(CancellationToken.None);
        var firstDecision = _logManager.GetTopicMetadata("auto-topic")!.Config["compression.type"];
        Assert.NotEqual("auto", firstDecision);

        // Second scan: topic no longer 'auto' → service should be a no-op.
        await service.ScanOnceAsync(CancellationToken.None);
        Assert.Equal(0, service.ActiveSamplerCount);
        Assert.Equal(firstDecision, _logManager.GetTopicMetadata("auto-topic")!.Config["compression.type"]);
    }

    private AdaptiveCompressionService CreateService(int samplerMinSamples, int sampleEveryN) =>
        new(
            new AdaptiveCompressionConfig
            {
                Enabled = true,
                ScanIntervalSeconds = 30,
                MaxScanBytesPerPartition = 1024 * 1024,
                MinSampleCount = samplerMinSamples,
                SampleEveryNthRecord = sampleEveryN
            },
            _logManager,
            NullLogger<AdaptiveCompressionService>.Instance);

    /// <summary>
    /// Build a minimal RecordBatch with an uncompressed (compression-type=0)
    /// payload after the header. Just enough for
    /// <see cref="AdaptiveCompressionService"/> to slice off the header and
    /// hand <paramref name="payload"/> to the sampler.
    /// </summary>
    private static byte[] CreateRecordBatchWithPayload(byte[] payload)
    {
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + payload.Length];

        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0);                // baseOffset
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12); // batchLength
        // Attributes (offset 21, 2 bytes) defaults to 0 → compression.type = none.
        BinaryPrimitives.WriteInt64BigEndian(
            batch.AsSpan(KafkaConstants.RecordBatch.BaseTimestampOffset, 8),
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);               // recordCount = 1

        payload.CopyTo(batch, KafkaConstants.RecordBatch.HeaderSize);
        return batch;
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try
        {
            if (Directory.Exists(_testDirectory))
                Directory.Delete(_testDirectory, recursive: true);
        }
        catch
        {
            // ignore cleanup
        }
    }
}
