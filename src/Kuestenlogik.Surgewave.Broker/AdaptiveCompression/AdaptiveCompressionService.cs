using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.AdaptiveCompression;

/// <summary>
/// Background service that resolves <c>compression.type=auto</c> topic configs
/// to a concrete codec.
///
/// <para>
/// Every <see cref="AdaptiveCompressionConfig.ScanIntervalSeconds"/> the service
/// enumerates every topic whose <c>compression.type</c> equals
/// <see cref="AdaptiveCompressionConfig.AutoMarker"/>, reads up to
/// <see cref="AdaptiveCompressionConfig.MaxScanBytesPerPartition"/> from each
/// partition, decompresses the record sections and feeds them into a per-topic
/// <see cref="AdaptiveCompressionSampler"/>. Once a sampler has accumulated
/// enough samples for a decision, the service writes the recommended codec name
/// back to the topic config via
/// <see cref="LogManager.UpdateTopicConfig(string, Dictionary{string,string}, IEnumerable{string})"/>
/// and evicts the sampler — the topic is now "decided" and never re-sampled
/// unless an operator manually flips it back to <c>auto</c>.
/// </para>
///
/// <para>
/// The Produce hot path is deliberately untouched: sampling runs out-of-band so
/// adaptive compression carries zero per-record overhead, at the cost of a
/// scan-interval delay before a fresh topic settles on a codec.
/// </para>
/// </summary>
public sealed class AdaptiveCompressionService : BackgroundService
{
    private const string CompressionTypeKey = "compression.type";

    private readonly AdaptiveCompressionConfig _config;
    private readonly LogManager _logManager;
    private readonly ILogger<AdaptiveCompressionService> _logger;

    private readonly ConcurrentDictionary<string, AdaptiveCompressionSampler> _samplers = new();

    public AdaptiveCompressionService(
        AdaptiveCompressionConfig config,
        LogManager logManager,
        ILogger<AdaptiveCompressionService> logger)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logManager = logManager ?? throw new ArgumentNullException(nameof(logManager));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>Number of topics that currently have an active sampler. Exposed
    /// for tests and the (future) <c>/api/adaptive-compression</c> endpoint.</summary>
    public int ActiveSamplerCount => _samplers.Count;

    /// <inheritdoc/>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            _logger.LogDebug("Adaptive compression is disabled");
            return;
        }

        _logger.LogInformation(
            "Adaptive compression started (interval={Interval}s, maxScanBytes={MaxBytes}/partition)",
            _config.ScanIntervalSeconds, _config.MaxScanBytesPerPartition);

        var interval = TimeSpan.FromSeconds(_config.ScanIntervalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, stoppingToken).ConfigureAwait(false);
                await ScanOnceAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during adaptive-compression scan cycle");
            }
        }

        _logger.LogInformation("Adaptive compression stopped");
    }

    /// <summary>One scan cycle — visible for tests so they can drive the service
    /// without waiting on the interval timer.</summary>
    internal async Task ScanOnceAsync(CancellationToken cancellationToken)
    {
        foreach (var topic in _logManager.ListTopics())
        {
            if (cancellationToken.IsCancellationRequested) return;

            if (!IsAutoTopic(topic))
            {
                // Topic stopped being 'auto' (operator change, or we just decided
                // for it) — drop any sampler state we held.
                _samplers.TryRemove(topic.Name, out _);
                continue;
            }

            var sampler = _samplers.GetOrAdd(topic.Name, _ => CreateSampler());
            await SampleTopicAsync(topic, sampler, cancellationToken).ConfigureAwait(false);

            if (sampler.TryDecide() is { } decision)
            {
                ApplyDecision(topic, decision);
            }
        }
    }

    private static bool IsAutoTopic(TopicMetadata topic) =>
        topic.Config.TryGetValue(CompressionTypeKey, out var value)
        && string.Equals(value, AdaptiveCompressionConfig.AutoMarker, StringComparison.OrdinalIgnoreCase);

    private AdaptiveCompressionSampler CreateSampler() =>
        new(new CompressionSamplerOptions
        {
            SampleEveryNthRecord = _config.SampleEveryNthRecord,
            MinSampleCount = _config.MinSampleCount
        });

    private async Task SampleTopicAsync(
        TopicMetadata topic,
        AdaptiveCompressionSampler sampler,
        CancellationToken cancellationToken)
    {
        for (var partition = 0; partition < topic.PartitionCount; partition++)
        {
            if (cancellationToken.IsCancellationRequested) return;

            var topicPartition = new TopicPartition { Topic = topic.Name, Partition = partition };
            var log = _logManager.GetLog(topicPartition);
            if (log is null) continue;

            // Start from the latest data — adaptive decisions should reflect
            // current workload, not the topic's entire history.
            var endOffset = log.NextOffset;
            if (endOffset <= 0) continue;

            // Walk backwards a reasonable amount; ReadBatchesAsync clamps us at
            // MaxScanBytesPerPartition anyway, so this is a soft lower bound.
            var startOffset = Math.Max(0, endOffset - 1024);

            List<byte[]> batches;
            try
            {
                batches = await _logManager.ReadBatchesAsync(
                    topicPartition,
                    startOffset,
                    _config.MaxScanBytesPerPartition,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Adaptive compression: failed to read batches from {Topic}-{Partition}",
                    topic.Name, partition);
                continue;
            }

            foreach (var batch in batches)
            {
                ObserveBatch(sampler, batch);
            }
        }
    }

    private static void ObserveBatch(AdaptiveCompressionSampler sampler, byte[] batch)
    {
        if (batch.Length <= KafkaConstants.RecordBatch.HeaderSize) return;

        var compressionType = CompressionCodec.GetCompressionTypeFromBatch(batch);
        var recordsSection = batch.AsSpan(KafkaConstants.RecordBatch.HeaderSize);

        if (compressionType == KafkaConstants.Compression.None)
        {
            sampler.Observe(recordsSection);
            return;
        }

        byte[] decompressed;
        try
        {
            decompressed = CompressionCodec.Decompress(recordsSection.ToArray(), compressionType);
        }
        catch
        {
            // Malformed batch or unsupported codec — skip silently; sampler will
            // converge on the remaining good samples.
            return;
        }

        sampler.Observe(decompressed);
    }

    private void ApplyDecision(TopicMetadata topic, CompressionDecision decision)
    {
        var codecName = CompressionCodec.GetCompressionName(decision.Codec).ToLowerInvariant();

        var updates = new Dictionary<string, string>
        {
            [CompressionTypeKey] = codecName
        };

        try
        {
            _logManager.UpdateTopicConfig(topic.Name, updates);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Adaptive compression: failed to write back '{Codec}' for topic '{Topic}'",
                LogSanitizer.Sanitize(codecName),
                LogSanitizer.Sanitize(topic.Name));
            return;
        }

        _samplers.TryRemove(topic.Name, out _);

        _logger.LogInformation(
            "Adaptive compression: topic '{Topic}' resolved compression.type=auto -> {Codec} ({Reason})",
            LogSanitizer.Sanitize(topic.Name),
            LogSanitizer.Sanitize(codecName),
            LogSanitizer.Sanitize(decision.Reason));
    }
}
