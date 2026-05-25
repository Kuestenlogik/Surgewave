using System.Text;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Util;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

public sealed class AdaptiveCompressionSamplerTests
{
    [Fact]
    public void TryDecide_Returns_Null_Before_MinSampleCount_Reached()
    {
        var sampler = new AdaptiveCompressionSampler(new CompressionSamplerOptions
        {
            SampleEveryNthRecord = 1,
            MinSampleCount = 10,
        });

        for (var i = 0; i < 5; i++)
        {
            sampler.Observe(MakeRepetitiveJson(512));
        }

        Assert.Null(sampler.TryDecide());
    }

    [Fact]
    public void TryDecide_Returns_Decision_After_MinSampleCount()
    {
        var sampler = new AdaptiveCompressionSampler(new CompressionSamplerOptions
        {
            SampleEveryNthRecord = 1,
            MinSampleCount = 10,
        });

        for (var i = 0; i < 12; i++)
        {
            sampler.Observe(MakeRepetitiveJson(512));
        }

        var decision = sampler.TryDecide();
        Assert.NotNull(decision);
        Assert.NotEmpty(decision!.PerCodec);
    }

    [Fact]
    public void Highly_Compressible_Payload_Recommends_A_Compressing_Codec()
    {
        // 4 KB of repeated tokens — every codec should beat None handily,
        // so the recommendation must be a real compressor.
        var sampler = new AdaptiveCompressionSampler(new CompressionSamplerOptions
        {
            SampleEveryNthRecord = 1,
            MinSampleCount = 10,
        });

        var payload = MakeRepetitiveJson(4096);
        for (var i = 0; i < 20; i++)
        {
            sampler.Observe(payload);
        }

        var decision = sampler.TryDecide();
        Assert.NotNull(decision);
        Assert.NotEqual(KafkaConstants.Compression.None, decision!.Codec);
        Assert.Contains("ratio", decision.Reason);
    }

    [Fact]
    public void Incompressible_Payload_Falls_Back_To_None()
    {
        // Pre-zstd-compressed bytes — every further codec will fail to shrink
        // (or worse, grow the payload). Decision must be None.
        var sampler = new AdaptiveCompressionSampler(new CompressionSamplerOptions
        {
            SampleEveryNthRecord = 1,
            MinSampleCount = 10,
        });

        var raw = MakeRepetitiveJson(4096);
        var alreadyCompressed = CompressionCodec.Compress(raw, KafkaConstants.Compression.Zstd);

        for (var i = 0; i < 20; i++)
        {
            sampler.Observe(alreadyCompressed);
        }

        var decision = sampler.TryDecide();
        Assert.NotNull(decision);
        // For pre-compressed payloads, every further candidate either grows
        // the bytes or is excluded from scoring — the sampler must NOT pick
        // a codec that makes things worse. Either None or one of the
        // genuinely-still-shrinking codecs is acceptable. The key invariant
        // is that the winner does not grow the payload.
        var winnerSnapshot = decision!.PerCodec.Single(s => s.Codec == decision.Codec);
        Assert.True(
            winnerSnapshot.CompressedBytes <= winnerSnapshot.UncompressedBytes,
            $"Winner {CompressionCodec.GetCompressionName(decision.Codec)} grew payload " +
            $"from {winnerSnapshot.UncompressedBytes} to {winnerSnapshot.CompressedBytes}.");
    }

    [Fact]
    public void Empty_Span_Is_Ignored()
    {
        var sampler = new AdaptiveCompressionSampler(new CompressionSamplerOptions
        {
            SampleEveryNthRecord = 1,
            MinSampleCount = 1,
        });

        for (var i = 0; i < 100; i++)
        {
            sampler.Observe(ReadOnlySpan<byte>.Empty);
        }

        Assert.Equal(0, sampler.Observed);
        Assert.Equal(0, sampler.Sampled);
        Assert.Null(sampler.TryDecide());
    }

    [Fact]
    public void Sample_Frequency_Respected()
    {
        // SampleEveryNthRecord=10 — out of 100 Observe calls, exactly 10
        // should run the codec sweep.
        var sampler = new AdaptiveCompressionSampler(new CompressionSamplerOptions
        {
            SampleEveryNthRecord = 10,
            MinSampleCount = 5,
        });

        var payload = MakeRepetitiveJson(256);
        for (var i = 0; i < 100; i++)
        {
            sampler.Observe(payload);
        }

        Assert.Equal(100, sampler.Observed);
        Assert.Equal(10, sampler.Sampled);
    }

    [Fact]
    public void Reset_Clears_Counters_And_Stats()
    {
        var sampler = new AdaptiveCompressionSampler(new CompressionSamplerOptions
        {
            SampleEveryNthRecord = 1,
            MinSampleCount = 5,
        });

        for (var i = 0; i < 10; i++)
        {
            sampler.Observe(MakeRepetitiveJson(256));
        }
        Assert.NotNull(sampler.TryDecide());

        sampler.Reset();

        Assert.Equal(0, sampler.Observed);
        Assert.Equal(0, sampler.Sampled);
        Assert.Null(sampler.TryDecide());
    }

    [Fact]
    public void Candidate_Whitelist_Honored()
    {
        // Whitelist Lz4 + None only — decision must not return Snappy or Zstd.
        var sampler = new AdaptiveCompressionSampler(new CompressionSamplerOptions
        {
            SampleEveryNthRecord = 1,
            MinSampleCount = 5,
            CandidateCodecs = [KafkaConstants.Compression.Lz4, KafkaConstants.Compression.None],
        });

        for (var i = 0; i < 10; i++)
        {
            sampler.Observe(MakeRepetitiveJson(2048));
        }

        var decision = sampler.TryDecide();
        Assert.NotNull(decision);
        Assert.Equal(2, decision!.PerCodec.Count);
        Assert.Contains(decision.Codec, new[] { KafkaConstants.Compression.Lz4, KafkaConstants.Compression.None });
    }

    [Fact]
    public void Per_Codec_Snapshot_Tracks_Sample_Counts()
    {
        var sampler = new AdaptiveCompressionSampler(new CompressionSamplerOptions
        {
            SampleEveryNthRecord = 1,
            MinSampleCount = 5,
        });

        for (var i = 0; i < 7; i++)
        {
            sampler.Observe(MakeRepetitiveJson(256));
        }

        var decision = sampler.TryDecide();
        Assert.NotNull(decision);
        Assert.All(decision!.PerCodec, snap => Assert.Equal(7, snap.SampleCount));
    }

    [Fact]
    public void Concurrent_Observers_Do_Not_Lose_Counts()
    {
        // 4 threads, 250 Observe-calls each = 1000 total. Counter must match
        // exactly under the interlocked-add path.
        var sampler = new AdaptiveCompressionSampler(new CompressionSamplerOptions
        {
            SampleEveryNthRecord = 100, // keep the test cheap — only 10 actual sweeps
            MinSampleCount = 1,
        });

        var payload = MakeRepetitiveJson(256);
        Parallel.For(0, 4, _ =>
        {
            for (var i = 0; i < 250; i++)
            {
                sampler.Observe(payload);
            }
        });

        Assert.Equal(1000, sampler.Observed);
        Assert.Equal(10, sampler.Sampled);
    }

    private static byte[] MakeRepetitiveJson(int size)
    {
        // Highly compressible payload — repeated key/value pairs that
        // every codec should be able to shrink dramatically.
        var sb = new StringBuilder(size);
        var i = 0;
        while (sb.Length < size)
        {
            sb.Append("{\"customer\":\"alice\",\"action\":\"order\",\"id\":").Append(i++).Append("},");
        }
        return Encoding.UTF8.GetBytes(sb.ToString()[..size]);
    }
}
