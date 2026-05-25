using System.Diagnostics;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Heuristic that watches an uncompressed-record-bytes stream from one topic
/// and picks the best compression codec for that topic's data shape. Built
/// for the <c>compression.type=auto</c> Surgewave extension (G20 on the
/// roadmap) — backwards compatible: a topic with an explicit
/// <c>compression.type</c> value never reaches this code path.
/// </summary>
/// <remarks>
/// <para>
/// Sampling is opt-in per call. Production hot paths call <see cref="Observe"/>
/// for every record but only the every-<c>SampleEveryNthRecord</c>-th call
/// actually runs the four compressors; the rest update only a cheap
/// uncompressed-bytes counter. Keeps the steady-state overhead at a single
/// branch + an interlocked add.
/// </para>
/// <para>
/// After <see cref="CompressionSamplerOptions.MinSampleCount"/> samples are
/// recorded, <see cref="TryDecide"/> returns the winning codec. The score is
/// "uncompressed bytes saved per millisecond of compress time" — biased
/// towards lz4 / snappy on the millisecond axis and towards zstd on the
/// bytes axis, mirroring the typical Kafka-on-Surgewave operator preference
/// (cheap CPU, throughput-first). Codecs that grow the payload past raw
/// (rare but happens on already-compressed data) are filtered out before
/// scoring.
/// </para>
/// <para>
/// The sampler is fully thread-safe — concurrent producers on a single topic
/// share one instance.
/// </para>
/// </remarks>
public sealed class AdaptiveCompressionSampler
{
    private readonly CompressionSamplerOptions _options;
    private readonly int[] _candidateCodecs;
    private readonly CompressionStats[] _stats;
    private long _observed;
    private long _sampled;

    public AdaptiveCompressionSampler(CompressionSamplerOptions? options = null)
    {
        _options = options ?? CompressionSamplerOptions.Default;
        _candidateCodecs = _options.CandidateCodecs.ToArray();
        _stats = new CompressionStats[_candidateCodecs.Length];
        for (var i = 0; i < _stats.Length; i++)
        {
            _stats[i] = new CompressionStats(_candidateCodecs[i]);
        }
    }

    /// <summary>Total number of <see cref="Observe"/> calls so far.</summary>
    public long Observed => Interlocked.Read(ref _observed);

    /// <summary>Number of those calls that actually ran the codec sweep.</summary>
    public long Sampled => Interlocked.Read(ref _sampled);

    /// <summary>
    /// Record one uncompressed record-batch payload. Cheap on the common
    /// path (only every Nth call runs the compressors).
    /// </summary>
    public void Observe(ReadOnlySpan<byte> uncompressedBytes)
    {
        if (uncompressedBytes.IsEmpty)
        {
            return;
        }

        var count = Interlocked.Increment(ref _observed);
        if (count % _options.SampleEveryNthRecord != 0)
        {
            return;
        }

        Interlocked.Increment(ref _sampled);

        // Snapshot the span into a heap buffer; CompressionCodec.Compress
        // takes byte[] today. Cheap for the 1-in-N path.
        var snapshot = uncompressedBytes.ToArray();

        for (var i = 0; i < _candidateCodecs.Length; i++)
        {
            var codec = _candidateCodecs[i];
            var start = Stopwatch.GetTimestamp();
            byte[] compressed;
            try
            {
                compressed = CompressionCodec.Compress(snapshot, codec);
            }
            catch
            {
                // Codec failure on this codec → mark it as unusable for this
                // sample but keep going on the others.
                continue;
            }
            var elapsedTicks = Stopwatch.GetTimestamp() - start;

            _stats[i].Add(snapshot.Length, compressed.Length, elapsedTicks);
        }
    }

    /// <summary>
    /// Return the recommended codec, or <c>null</c> if not enough samples
    /// have accumulated yet (<see cref="CompressionSamplerOptions.MinSampleCount"/>).
    /// </summary>
    public CompressionDecision? TryDecide()
    {
        if (Sampled < _options.MinSampleCount)
        {
            return null;
        }

        ScoredCandidate? best = null;
        var perCodec = new List<CodecStatsSnapshot>(_candidateCodecs.Length);

        foreach (var stats in _stats)
        {
            var snapshot = stats.Snapshot();
            perCodec.Add(snapshot);
            if (snapshot.SampleCount == 0)
            {
                continue;
            }
            if (snapshot.CompressedBytes >= snapshot.UncompressedBytes)
            {
                // codec grew the payload — exclude from scoring
                continue;
            }

            var bytesSaved = snapshot.UncompressedBytes - snapshot.CompressedBytes;
            var elapsedMs = TicksToMs(snapshot.CompressTicks);
            var score = elapsedMs > 0 ? bytesSaved / elapsedMs : double.PositiveInfinity;

            if (best is null || score > best.Value.Score)
            {
                best = new ScoredCandidate(snapshot.Codec, score, snapshot);
            }
        }

        if (best is null)
        {
            // Every codec grew the payload — recommend None.
            return new CompressionDecision(
                KafkaConstants.Compression.None,
                Reason: "all candidate codecs failed to shrink the payload — keep raw",
                PerCodec: perCodec);
        }

        var winner = best.Value;
        var ratio = winner.Snapshot.UncompressedBytes == 0
            ? 0.0
            : (double)winner.Snapshot.CompressedBytes / winner.Snapshot.UncompressedBytes;
        var reason = $"best bytes-saved-per-ms among {perCodec.Count(p => p.SampleCount > 0)} codecs " +
                     $"({CompressionCodec.GetCompressionName(winner.Codec)}: ratio {ratio:0.000})";

        return new CompressionDecision(winner.Codec, reason, perCodec);
    }

    /// <summary>
    /// Drop accumulated stats. Use when re-sampling after the data shape on
    /// a topic has demonstrably changed (e.g. schema evolution).
    /// </summary>
    public void Reset()
    {
        Interlocked.Exchange(ref _observed, 0);
        Interlocked.Exchange(ref _sampled, 0);
        foreach (var s in _stats)
        {
            s.Reset();
        }
    }

    private static double TicksToMs(long ticks) =>
        ticks * 1000.0 / Stopwatch.Frequency;

    private readonly record struct ScoredCandidate(int Codec, double Score, CodecStatsSnapshot Snapshot);

    private sealed class CompressionStats
    {
        private readonly int _codec;
        private long _uncompressedBytes;
        private long _compressedBytes;
        private long _compressTicks;
        private long _sampleCount;

        public CompressionStats(int codec) => _codec = codec;

        public void Add(int uncompressed, int compressed, long elapsedTicks)
        {
            Interlocked.Add(ref _uncompressedBytes, uncompressed);
            Interlocked.Add(ref _compressedBytes, compressed);
            Interlocked.Add(ref _compressTicks, elapsedTicks);
            Interlocked.Increment(ref _sampleCount);
        }

        public CodecStatsSnapshot Snapshot() => new(
            _codec,
            Interlocked.Read(ref _sampleCount),
            Interlocked.Read(ref _uncompressedBytes),
            Interlocked.Read(ref _compressedBytes),
            Interlocked.Read(ref _compressTicks));

        public void Reset()
        {
            Interlocked.Exchange(ref _uncompressedBytes, 0);
            Interlocked.Exchange(ref _compressedBytes, 0);
            Interlocked.Exchange(ref _compressTicks, 0);
            Interlocked.Exchange(ref _sampleCount, 0);
        }
    }
}

/// <summary>Tuning knobs for <see cref="AdaptiveCompressionSampler"/>.</summary>
public sealed record CompressionSamplerOptions
{
    /// <summary>
    /// Sample every Nth <see cref="AdaptiveCompressionSampler.Observe"/> call.
    /// The other N-1 calls only bump a counter, so the steady-state cost is
    /// negligible. Default <c>100</c> — ~1% of records run the codec sweep.
    /// </summary>
    public int SampleEveryNthRecord { get; init; } = 100;

    /// <summary>
    /// Minimum number of sweeps before <see cref="AdaptiveCompressionSampler.TryDecide"/>
    /// will return a recommendation. With the default
    /// <see cref="SampleEveryNthRecord"/> = 100, the default value of 50
    /// means a topic settles after ~5000 records — enough to smooth out
    /// shape outliers, fast enough that warm-up isn't noticeable.
    /// </summary>
    public int MinSampleCount { get; init; } = 50;

    /// <summary>
    /// Codecs to consider. Default is lz4, zstd, snappy, none — Kafka's
    /// gzip is intentionally excluded (it dominates the per-ms axis with
    /// modest ratio gains, so it almost always loses the score and adds
    /// CPU jitter on hot paths).
    /// </summary>
    public IReadOnlyList<int> CandidateCodecs { get; init; } =
    [
        KafkaConstants.Compression.Lz4,
        KafkaConstants.Compression.Zstd,
        KafkaConstants.Compression.Snappy,
        KafkaConstants.Compression.None,
    ];

    public static CompressionSamplerOptions Default { get; } = new();
}

/// <summary>The recommendation returned by <see cref="AdaptiveCompressionSampler.TryDecide"/>.</summary>
/// <param name="Codec">
/// The recommended compression type — one of the <c>KafkaConstants.Compression.*</c>
/// values (None / Gzip / Snappy / Lz4 / Zstd).
/// </param>
/// <param name="Reason">
/// One-line operator-readable explanation. Suitable for log lines and
/// audit trails.
/// </param>
/// <param name="PerCodec">
/// Per-codec stats snapshot — useful for diagnostics, dashboards, and
/// the <c>surgewave topics describe --compression-stats</c> CLI variant.
/// </param>
public sealed record CompressionDecision(
    int Codec,
    string Reason,
    IReadOnlyList<CodecStatsSnapshot> PerCodec);

/// <summary>
/// Per-codec stats snapshot — atomic point-in-time view of how one codec
/// performed across all sampled batches.
/// </summary>
public sealed record CodecStatsSnapshot(
    int Codec,
    long SampleCount,
    long UncompressedBytes,
    long CompressedBytes,
    long CompressTicks);
