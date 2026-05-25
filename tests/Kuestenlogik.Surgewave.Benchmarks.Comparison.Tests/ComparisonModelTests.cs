using Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;
using Xunit;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Tests;

/// <summary>
/// Verifies comparison model types: default values, delta calculations, and report construction.
/// </summary>
public class ComparisonModelTests
{
    [Fact]
    public void BenchmarkParams_Defaults()
    {
        var p = new BenchmarkParams();

        Assert.Equal(100_000, p.MessageCount);
        Assert.Equal(100, p.MessageSizeBytes);
        Assert.Equal(1000, p.BatchSize);
        Assert.Equal(3, p.Partitions);
        Assert.Equal(1, p.ProducerCount);
        Assert.Equal(1, p.ConsumerCount);
        Assert.Equal("localhost:29092", p.KafkaBootstrap);
        Assert.False(p.SurgewaveOnly);
        Assert.Null(p.OutputPath);
        Assert.Null(p.ReportPath);
        Assert.Contains(BenchmarkPlatform.SurgewaveEmbeddedNative, p.Platforms);
        Assert.Contains(BenchmarkPlatform.ApacheKafkaContainer, p.Platforms);
        Assert.Equal("localhost:9092", p.SurgewaveStandaloneAddress);
        Assert.Equal("surgewave:latest", p.SurgewaveContainerImage);
        Assert.Equal("confluentinc/cp-kafka:7.6.0", p.KafkaContainerImage);
        Assert.Equal("redpandadata/redpanda:latest", p.RedpandaContainerImage);
    }

    [Fact]
    public void BenchmarkParams_CustomValues()
    {
        var p = new BenchmarkParams
        {
            MessageCount = 500_000,
            MessageSizeBytes = 1024,
            BatchSize = 5000,
            Partitions = 6,
            ProducerCount = 4,
            ConsumerCount = 2,
            KafkaBootstrap = "kafka.local:9092",
            SurgewaveOnly = true,
            OutputPath = "results.json",
            ReportPath = "report.md"
        };

        Assert.Equal(500_000, p.MessageCount);
        Assert.Equal(1024, p.MessageSizeBytes);
        Assert.Equal(5000, p.BatchSize);
        Assert.Equal(6, p.Partitions);
        Assert.Equal(4, p.ProducerCount);
        Assert.Equal(2, p.ConsumerCount);
        Assert.Equal("kafka.local:9092", p.KafkaBootstrap);
        Assert.True(p.SurgewaveOnly);
        Assert.Equal("results.json", p.OutputPath);
        Assert.Equal("report.md", p.ReportPath);
    }

    [Fact]
    public void BenchmarkParams_IsPlatformEnabled()
    {
        var p = new BenchmarkParams();
        Assert.True(p.IsPlatformEnabled(BenchmarkPlatform.SurgewaveEmbeddedNative));
        Assert.True(p.IsPlatformEnabled(BenchmarkPlatform.ApacheKafkaContainer));
        Assert.False(p.IsPlatformEnabled(BenchmarkPlatform.RedpandaContainer));
    }

    [Fact]
    public void ComparisonResult_Properties()
    {
        var result = new ComparisonResult
        {
            Platform = "Surgewave Embedded + Native",
            PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
            ProduceThroughputMsgPerSec = 2_000_000,
            ProduceThroughputMbPerSec = 190.7,
            ConsumeThroughputMsgPerSec = 2_500_000,
            ConsumeThroughputMbPerSec = 238.4,
            ProduceLatencyP50Ms = 0.05,
            ProduceLatencyP90Ms = 0.10,
            ProduceLatencyP99Ms = 0.25,
            ConsumeLatencyP50Ms = 0.01,
            ConsumeLatencyP90Ms = 0.03,
            ConsumeLatencyP99Ms = 0.08,
            TotalBytesProduced = 10_000_000,
            Duration = TimeSpan.FromSeconds(5)
        };

        Assert.Equal("Surgewave Embedded + Native", result.Platform);
        Assert.Equal(BenchmarkPlatform.SurgewaveEmbeddedNative, result.PlatformType);
        Assert.Equal(2_000_000, result.ProduceThroughputMsgPerSec);
        Assert.Equal(190.7, result.ProduceThroughputMbPerSec);
        Assert.Equal(2_500_000, result.ConsumeThroughputMsgPerSec);
        Assert.Equal(238.4, result.ConsumeThroughputMbPerSec);
        Assert.Equal(0.05, result.ProduceLatencyP50Ms);
        Assert.Equal(0.10, result.ProduceLatencyP90Ms);
        Assert.Equal(0.25, result.ProduceLatencyP99Ms);
        Assert.Equal(0.01, result.ConsumeLatencyP50Ms);
        Assert.Equal(0.03, result.ConsumeLatencyP90Ms);
        Assert.Equal(0.08, result.ConsumeLatencyP99Ms);
        Assert.Equal(10_000_000, result.TotalBytesProduced);
        Assert.Equal(TimeSpan.FromSeconds(5), result.Duration);
    }

    [Fact]
    public void ComparisonReport_DeltaCalculation_WithKafka()
    {
        var surgewaveResult = new ComparisonResult
        {
            Platform = "Surgewave Embedded + Native",
            PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
            ProduceThroughputMsgPerSec = 2_000_000,
            ConsumeThroughputMsgPerSec = 2_500_000,
            ProduceLatencyP99Ms = 0.25,
            ConsumeLatencyP99Ms = 0.08
        };
        var kafkaResult = new ComparisonResult
        {
            Platform = "Apache Kafka Container",
            PlatformType = BenchmarkPlatform.ApacheKafkaContainer,
            ProduceThroughputMsgPerSec = 1_000_000,
            ConsumeThroughputMsgPerSec = 1_500_000,
            ProduceLatencyP99Ms = 2.5,
            ConsumeLatencyP99Ms = 1.0
        };

        var report = new ComparisonReport
        {
            ScenarioName = "Throughput",
            Description = "Test throughput",
            Results = [surgewaveResult, kafkaResult]
        };

        // Legacy accessors
        Assert.Equal("Surgewave Embedded + Native", report.Surgewave.Platform);
        Assert.Equal("Apache Kafka Container", report.Kafka!.Platform);

        // Produce throughput: (2M - 1M) / 1M * 100 = +100%
        Assert.NotNull(report.ProduceThroughputDeltaPercent);
        Assert.Equal(100.0, report.ProduceThroughputDeltaPercent!.Value, 0.01);

        // Consume throughput: (2.5M - 1.5M) / 1.5M * 100 = +66.7%
        Assert.NotNull(report.ConsumeThroughputDeltaPercent);
        Assert.Equal(66.67, report.ConsumeThroughputDeltaPercent!.Value, 0.01);

        // Produce latency P99: (0.25 - 2.5) / 2.5 * 100 = -90% (Surgewave is faster)
        Assert.NotNull(report.ProduceLatencyP99DeltaPercent);
        Assert.Equal(-90.0, report.ProduceLatencyP99DeltaPercent!.Value, 0.01);

        // Consume latency P99: (0.08 - 1.0) / 1.0 * 100 = -92% (Surgewave is faster)
        Assert.NotNull(report.ConsumeLatencyP99DeltaPercent);
        Assert.Equal(-92.0, report.ConsumeLatencyP99DeltaPercent!.Value, 0.01);
    }

    [Fact]
    public void ComparisonReport_DeltaCalculation_WithoutKafka()
    {
        var surgewaveResult = new ComparisonResult
        {
            Platform = "Surgewave Embedded + Native",
            PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
            ProduceThroughputMsgPerSec = 2_000_000,
            ConsumeThroughputMsgPerSec = 2_500_000,
            ProduceLatencyP99Ms = 0.25,
            ConsumeLatencyP99Ms = 0.08
        };

        var report = new ComparisonReport
        {
            ScenarioName = "Throughput",
            Description = "Test throughput",
            Results = [surgewaveResult]
        };

        Assert.Null(report.Kafka);
        Assert.Null(report.ProduceThroughputDeltaPercent);
        Assert.Null(report.ConsumeThroughputDeltaPercent);
        Assert.Null(report.ProduceLatencyP99DeltaPercent);
        Assert.Null(report.ConsumeLatencyP99DeltaPercent);
    }

    [Fact]
    public void ComparisonReport_DeltaCalculation_ZeroKafkaValues()
    {
        var report = new ComparisonReport
        {
            ScenarioName = "Test",
            Description = "Test",
            Results =
            [
                new ComparisonResult
                {
                    Platform = "Surgewave Embedded + Native",
                    PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
                    ProduceThroughputMsgPerSec = 1_000_000,
                    ConsumeThroughputMsgPerSec = 500_000,
                    ProduceLatencyP99Ms = 0.1,
                    ConsumeLatencyP99Ms = 0.05
                },
                new ComparisonResult
                {
                    Platform = "Apache Kafka Container",
                    PlatformType = BenchmarkPlatform.ApacheKafkaContainer,
                    ProduceThroughputMsgPerSec = 0,
                    ConsumeThroughputMsgPerSec = 0,
                    ProduceLatencyP99Ms = 0,
                    ConsumeLatencyP99Ms = 0
                }
            ]
        };

        // Zero Kafka values should return null (avoid division by zero)
        Assert.Null(report.ProduceThroughputDeltaPercent);
        Assert.Null(report.ConsumeThroughputDeltaPercent);
        Assert.Null(report.ProduceLatencyP99DeltaPercent);
        Assert.Null(report.ConsumeLatencyP99DeltaPercent);
    }

    [Fact]
    public void ComparisonSubResult_Properties()
    {
        var sub = new ComparisonSubResult
        {
            Label = "Batch 1000",
            Results =
            [
                new ComparisonResult
                {
                    Platform = "Surgewave Embedded + Native",
                    PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
                    ProduceThroughputMsgPerSec = 2_000_000
                },
                new ComparisonResult
                {
                    Platform = "Apache Kafka Container",
                    PlatformType = BenchmarkPlatform.ApacheKafkaContainer,
                    ProduceThroughputMsgPerSec = 1_500_000
                }
            ]
        };

        Assert.Equal("Batch 1000", sub.Label);
        Assert.Equal("Surgewave Embedded + Native", sub.Surgewave.Platform);
        Assert.Equal(2_000_000, sub.Surgewave.ProduceThroughputMsgPerSec);
        Assert.NotNull(sub.Kafka);
        Assert.Equal("Apache Kafka Container", sub.Kafka!.Platform);
    }

    [Fact]
    public void ComparisonSubResult_NullKafka()
    {
        var sub = new ComparisonSubResult
        {
            Label = "100B",
            Results =
            [
                new ComparisonResult
                {
                    Platform = "Surgewave Embedded + Native",
                    PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
                    ProduceThroughputMsgPerSec = 2_000_000
                }
            ]
        };

        Assert.Equal("100B", sub.Label);
        Assert.Null(sub.Kafka);
    }

    [Fact]
    public void ComparisonReport_WithSubResults()
    {
        var report = new ComparisonReport
        {
            ScenarioName = "Batch Size Impact",
            Description = "Throughput across batch sizes",
            Results =
            [
                new ComparisonResult
                {
                    Platform = "Surgewave Embedded + Native",
                    PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
                    ProduceThroughputMsgPerSec = 2_000_000
                }
            ],
            SubResults =
            [
                new ComparisonSubResult
                {
                    Label = "Batch 1",
                    Results =
                    [
                        new ComparisonResult
                        {
                            Platform = "Surgewave Embedded + Native",
                            PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
                            ProduceThroughputMsgPerSec = 100_000
                        }
                    ]
                },
                new ComparisonSubResult
                {
                    Label = "Batch 1000",
                    Results =
                    [
                        new ComparisonResult
                        {
                            Platform = "Surgewave Embedded + Native",
                            PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
                            ProduceThroughputMsgPerSec = 2_000_000
                        }
                    ]
                }
            ]
        };

        Assert.NotNull(report.SubResults);
        Assert.Equal(2, report.SubResults.Count);
        Assert.Equal("Batch 1", report.SubResults[0].Label);
        Assert.Equal("Batch 1000", report.SubResults[1].Label);
    }

    [Fact]
    public void ComparisonReport_NegativeDelta_KafkaFaster()
    {
        var report = new ComparisonReport
        {
            ScenarioName = "Test",
            Description = "Test scenario",
            Results =
            [
                new ComparisonResult
                {
                    Platform = "Surgewave Embedded + Native",
                    PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
                    ProduceThroughputMsgPerSec = 800_000,
                    ConsumeThroughputMsgPerSec = 900_000,
                    ProduceLatencyP99Ms = 3.0,
                    ConsumeLatencyP99Ms = 2.0
                },
                new ComparisonResult
                {
                    Platform = "Apache Kafka Container",
                    PlatformType = BenchmarkPlatform.ApacheKafkaContainer,
                    ProduceThroughputMsgPerSec = 1_000_000,
                    ConsumeThroughputMsgPerSec = 1_200_000,
                    ProduceLatencyP99Ms = 1.5,
                    ConsumeLatencyP99Ms = 1.0
                }
            ]
        };

        // Surgewave slower: (800K - 1M) / 1M * 100 = -20%
        Assert.NotNull(report.ProduceThroughputDeltaPercent);
        Assert.Equal(-20.0, report.ProduceThroughputDeltaPercent!.Value, 0.01);

        // Surgewave slower: (900K - 1.2M) / 1.2M * 100 = -25%
        Assert.NotNull(report.ConsumeThroughputDeltaPercent);
        Assert.Equal(-25.0, report.ConsumeThroughputDeltaPercent!.Value, 0.01);

        // Surgewave higher latency: (3.0 - 1.5) / 1.5 * 100 = +100%
        Assert.NotNull(report.ProduceLatencyP99DeltaPercent);
        Assert.Equal(100.0, report.ProduceLatencyP99DeltaPercent!.Value, 0.01);
    }

    [Fact]
    public void ComparisonReport_MultiPlatform_GetResult()
    {
        var report = new ComparisonReport
        {
            ScenarioName = "Multi-Platform",
            Description = "All platforms",
            Results =
            [
                new ComparisonResult
                {
                    Platform = "Surgewave Embedded + Native",
                    PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
                    ProduceThroughputMsgPerSec = 2_000_000
                },
                new ComparisonResult
                {
                    Platform = "Redpanda Container",
                    PlatformType = BenchmarkPlatform.RedpandaContainer,
                    ProduceThroughputMsgPerSec = 1_700_000
                },
                new ComparisonResult
                {
                    Platform = "Apache Kafka Container",
                    PlatformType = BenchmarkPlatform.ApacheKafkaContainer,
                    ProduceThroughputMsgPerSec = 1_500_000
                }
            ]
        };

        Assert.NotNull(report.GetResult(BenchmarkPlatform.SurgewaveEmbeddedNative));
        Assert.NotNull(report.GetResult(BenchmarkPlatform.RedpandaContainer));
        Assert.NotNull(report.GetResult(BenchmarkPlatform.ApacheKafkaContainer));
        Assert.Null(report.GetResult(BenchmarkPlatform.SurgewaveContainerKafka));
    }

    [Fact]
    public void ComparisonReport_MultiPlatform_GetThroughputDelta()
    {
        var report = new ComparisonReport
        {
            ScenarioName = "Multi-Platform",
            Description = "All platforms",
            Results =
            [
                new ComparisonResult
                {
                    Platform = "Surgewave Embedded + Native",
                    PlatformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
                    ProduceThroughputMsgPerSec = 2_000_000,
                    ConsumeThroughputMsgPerSec = 2_500_000
                },
                new ComparisonResult
                {
                    Platform = "Apache Kafka Container",
                    PlatformType = BenchmarkPlatform.ApacheKafkaContainer,
                    ProduceThroughputMsgPerSec = 1_000_000,
                    ConsumeThroughputMsgPerSec = 1_250_000
                }
            ]
        };

        // Surgewave vs Kafka produce: (2M - 1M) / 1M * 100 = +100%
        var produceDelta = report.GetProduceThroughputDelta(
            BenchmarkPlatform.ApacheKafkaContainer, BenchmarkPlatform.SurgewaveEmbeddedNative);
        Assert.NotNull(produceDelta);
        Assert.Equal(100.0, produceDelta!.Value, 0.01);

        // Surgewave vs Kafka consume: (2.5M - 1.25M) / 1.25M * 100 = +100%
        var consumeDelta = report.GetConsumeThroughputDelta(
            BenchmarkPlatform.ApacheKafkaContainer, BenchmarkPlatform.SurgewaveEmbeddedNative);
        Assert.NotNull(consumeDelta);
        Assert.Equal(100.0, consumeDelta!.Value, 0.01);
    }

    [Fact]
    public void PlatformConfig_DisplayName()
    {
        Assert.Equal("Surgewave Embedded + Native", BenchmarkPlatform.SurgewaveEmbeddedNative.DisplayName());
        Assert.Equal("Apache Kafka Container", BenchmarkPlatform.ApacheKafkaContainer.DisplayName());
        Assert.Equal("Redpanda Container", BenchmarkPlatform.RedpandaContainer.DisplayName());
    }

    [Fact]
    public void PlatformConfig_IsEmbedded()
    {
        Assert.True(BenchmarkPlatform.SurgewaveEmbeddedNative.IsEmbedded());
        Assert.True(BenchmarkPlatform.SurgewaveEmbeddedKafka.IsEmbedded());
        Assert.False(BenchmarkPlatform.ApacheKafkaContainer.IsEmbedded());
        Assert.False(BenchmarkPlatform.SurgewaveStandaloneNative.IsEmbedded());
    }

    [Fact]
    public void PlatformConfig_IsContainer()
    {
        Assert.True(BenchmarkPlatform.SurgewaveContainerNative.IsContainer());
        Assert.True(BenchmarkPlatform.ApacheKafkaContainer.IsContainer());
        Assert.True(BenchmarkPlatform.RedpandaContainer.IsContainer());
        Assert.False(BenchmarkPlatform.SurgewaveEmbeddedNative.IsContainer());
    }

    [Fact]
    public void PlatformConfig_TryParse()
    {
        Assert.True(BenchmarkPlatformExtensions.TryParse("embedded-native", out var p1));
        Assert.Equal(BenchmarkPlatform.SurgewaveEmbeddedNative, p1);

        Assert.True(BenchmarkPlatformExtensions.TryParse("kafka", out var p2));
        Assert.Equal(BenchmarkPlatform.ApacheKafkaContainer, p2);

        Assert.True(BenchmarkPlatformExtensions.TryParse("redpanda", out var p3));
        Assert.Equal(BenchmarkPlatform.RedpandaContainer, p3);

        Assert.False(BenchmarkPlatformExtensions.TryParse("unknown-thing", out _));
    }

    [Fact]
    public void PlatformConfig_ParsePreset()
    {
        var all = BenchmarkPlatformExtensions.ParsePreset("all");
        Assert.NotNull(all);
        Assert.Equal(8, all!.Count);

        var fair = BenchmarkPlatformExtensions.ParsePreset("fair");
        Assert.NotNull(fair);
        Assert.Equal(3, fair!.Count);
        Assert.Contains(BenchmarkPlatform.SurgewaveContainerKafka, fair);
        Assert.Contains(BenchmarkPlatform.ApacheKafkaContainer, fair);
        Assert.Contains(BenchmarkPlatform.RedpandaContainer, fair);

        var containers = BenchmarkPlatformExtensions.ParsePreset("containers");
        Assert.NotNull(containers);
        Assert.Equal(4, containers!.Count);

        Assert.Null(BenchmarkPlatformExtensions.ParsePreset("invalid"));
    }
}
