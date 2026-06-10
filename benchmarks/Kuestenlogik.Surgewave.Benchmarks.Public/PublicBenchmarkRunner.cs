using Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;

namespace Kuestenlogik.Surgewave.Benchmarks.Public;

/// <summary>
/// Orchestrates a full G3 run: captures the hardware fingerprint,
/// iterates the curated scenarios, hands the combined result over to
/// the reporters. Pure orchestration — no scenario logic, no I/O
/// formatting. Both the <c>surgewave-bench</c> dotnet-tool and the
/// <c>surgewave bench public</c> CLI subcommand call into this same
/// runner so the report layout is identical regardless of entry
/// point.
/// </summary>
public sealed class PublicBenchmarkRunner
{
    private readonly IReadOnlyList<IPublicScenario> _scenarios;

    public PublicBenchmarkRunner()
        : this(PublicBenchmarkSuite.AllScenarios) { }

    public PublicBenchmarkRunner(IReadOnlyList<IPublicScenario> scenarios)
    {
        _scenarios = scenarios;
    }

    public async Task<PublicBenchmarkReport> RunAsync(
        PublicBenchmarkOptions options,
        IProgress<string>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var hardware = HardwareFingerprint.Capture();
        var startedAt = DateTimeOffset.UtcNow;
        var scenarioResults = new List<ScenarioReport>(_scenarios.Count);

        foreach (var scenario in _scenarios)
        {
            cancellationToken.ThrowIfCancellationRequested();
            progress?.Report($"running: {scenario.Id} — {scenario.Name}");

            var watch = System.Diagnostics.Stopwatch.StartNew();
            var rows = await scenario.RunAsync(options, cancellationToken).ConfigureAwait(false);
            watch.Stop();

            scenarioResults.Add(new ScenarioReport(
                Id: scenario.Id,
                Name: scenario.Name,
                Description: scenario.Description,
                Rows: rows,
                Duration: watch.Elapsed));
        }

        return new PublicBenchmarkReport(
            StartedAt: startedAt,
            CompletedAt: DateTimeOffset.UtcNow,
            Hardware: hardware,
            Options: options,
            Scenarios: scenarioResults);
    }
}

public sealed record ScenarioReport(
    string Id,
    string Name,
    string Description,
    IReadOnlyList<ScenarioResult> Rows,
    TimeSpan Duration);

public sealed record PublicBenchmarkReport(
    DateTimeOffset StartedAt,
    DateTimeOffset CompletedAt,
    HardwareFingerprint Hardware,
    PublicBenchmarkOptions Options,
    IReadOnlyList<ScenarioReport> Scenarios);
