using System.Text.Json;
using Kuestenlogik.Surgewave.Broker.AutoTuning;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

[Trait("Category", TestCategories.Unit)]
public sealed class ColdStartAutoTuneServiceTests : IDisposable
{
    private readonly string _tempDir;

    public ColdStartAutoTuneServiceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"surgewave-coldstart-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            // ignore cleanup
        }
    }

    [Fact]
    public void Config_Defaults_AreSafe()
    {
        var c = new ColdStartAutoTuneConfig();
        Assert.False(c.Enabled);
        Assert.Equal(TimeSpan.FromHours(24), c.ObservationWindow);
        Assert.Equal(TimeSpan.FromSeconds(60), c.CheckInterval);
        Assert.False(c.AutoApply);
        Assert.Equal("auto-tuned.json", c.AutoTunedJsonPath);
    }

    [Fact]
    public void TryReportOnce_WindowOpen_DoesNothing()
    {
        var fakeTime = new FakeTimeProvider();
        var profiler = new ColdStartWorkloadProfiler(TimeSpan.FromHours(24), fakeTime);
        var service = CreateService(profiler, autoApply: false);

        Assert.False(service.TryReportOnce());
        Assert.False(service.HasReported);
        Assert.False(File.Exists(GetReportPath()));
    }

    [Fact]
    public void TryReportOnce_WindowClosed_WritesReport()
    {
        var fakeTime = new FakeTimeProvider();
        var profiler = new ColdStartWorkloadProfiler(TimeSpan.FromHours(24), fakeTime);

        // Generate enough traffic to trip the batch-size rule (avg record
        // size < 512 B, total records > 10_000).
        for (var i = 0; i < 20_000; i++)
        {
            profiler.RecordProduce("hot-topic", recordCount: 1, byteCount: 100);
        }

        // Close the observation window.
        fakeTime.Advance(TimeSpan.FromHours(25));

        var service = CreateService(profiler, autoApply: false);
        Assert.True(service.TryReportOnce());
        Assert.True(service.HasReported);

        var reportPath = GetReportPath();
        Assert.True(File.Exists(reportPath));

        using var doc = JsonDocument.Parse(File.ReadAllText(reportPath));
        var root = doc.RootElement;

        Assert.False(root.GetProperty("autoApplied").GetBoolean());
        Assert.True(root.GetProperty("profile").GetProperty("isComplete").GetBoolean());
        Assert.True(root.GetProperty("recommendations").GetArrayLength() >= 1);

        // Suggest-only mode → nothing in appliedRuleIds.
        Assert.Equal(0, root.GetProperty("appliedRuleIds").GetArrayLength());
    }

    [Fact]
    public void TryReportOnce_IsIdempotent()
    {
        var fakeTime = new FakeTimeProvider();
        var profiler = new ColdStartWorkloadProfiler(TimeSpan.FromHours(24), fakeTime);
        profiler.RecordProduce("t", 1, 100);
        fakeTime.Advance(TimeSpan.FromHours(25));

        var service = CreateService(profiler, autoApply: false);

        Assert.True(service.TryReportOnce());
        Assert.False(service.TryReportOnce()); // second call no-ops
        Assert.True(service.HasReported);
    }

    [Fact]
    public void TryReportOnce_AutoApply_AppliesRecommendations()
    {
        // Use a short observation window so the num.partitions rule
        // (>= 10 MB/s sustained) trips on a feasible byte budget. The rule
        // recommends "num.partitions", which IS in DynamicConfigKeys, so
        // SetConfig succeeds.
        var window = TimeSpan.FromSeconds(10);
        var fakeTime = new FakeTimeProvider();
        var profiler = new ColdStartWorkloadProfiler(window, fakeTime);

        // 11 s * 10 MB/s ≈ 110 MB total — 200 records × 1 MB each does the job.
        for (var i = 0; i < 200; i++)
        {
            profiler.RecordProduce("hot-topic", recordCount: 1, byteCount: 1_048_576);
        }
        fakeTime.Advance(window + TimeSpan.FromSeconds(1));

        var brokerConfig = new BrokerConfig
        {
            DataDirectory = _tempDir,
            DefaultNumPartitions = 1,
        };
        var dynamicConfig = new DynamicBrokerConfig(brokerConfig, NullLogger<DynamicBrokerConfig>.Instance);
        var service = new ColdStartAutoTuneService(
            new ColdStartAutoTuneConfig
            {
                Enabled = true,
                ObservationWindow = window,
                AutoApply = true,
                AutoTunedJsonPath = GetReportPath(),
            },
            brokerConfig,
            dynamicConfig,
            profiler,
            NullLogger<ColdStartAutoTuneService>.Instance,
            fakeTime);

        Assert.True(service.TryReportOnce());

        using var doc = JsonDocument.Parse(File.ReadAllText(GetReportPath()));
        var applied = doc.RootElement.GetProperty("appliedRuleIds");
        Assert.True(applied.GetArrayLength() >= 1);

        // num.partitions rule should have flipped the dynamic config.
        var newPartitions = dynamicConfig.GetConfig("num.partitions");
        Assert.NotNull(newPartitions);
        Assert.NotEqual("1", newPartitions);
    }

    private ColdStartAutoTuneService CreateService(ColdStartWorkloadProfiler profiler, bool autoApply)
    {
        var brokerConfig = new BrokerConfig { DataDirectory = _tempDir };
        var dynamicConfig = new DynamicBrokerConfig(brokerConfig, NullLogger<DynamicBrokerConfig>.Instance);
        return new ColdStartAutoTuneService(
            new ColdStartAutoTuneConfig
            {
                Enabled = true,
                ObservationWindow = TimeSpan.FromHours(24),
                AutoApply = autoApply,
                AutoTunedJsonPath = GetReportPath(),
            },
            brokerConfig,
            dynamicConfig,
            profiler,
            NullLogger<ColdStartAutoTuneService>.Instance,
            timeProvider: TimeProvider.System);
    }

    private string GetReportPath() => Path.Combine(_tempDir, "auto-tuned.json");

    /// <summary>
    /// Minimal manually-controllable TimeProvider so the 24 h observation
    /// window can be "fast-forwarded" in unit tests without pulling in the
    /// Microsoft.Extensions.TimeProvider.Testing package just for this.
    /// </summary>
    private sealed class FakeTimeProvider : TimeProvider
    {
        private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan delta) => _now += delta;
    }
}
