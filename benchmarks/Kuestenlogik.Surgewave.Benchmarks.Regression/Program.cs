using System.Text.Json;
using Kuestenlogik.Surgewave.Benchmarks.Regression;
using Kuestenlogik.Surgewave.Benchmarks.Regression.Models;

if (args.Length == 0)
{
    PrintUsage();
    return 1;
}

var command = args[0].ToUpperInvariant();
return command switch
{
    "COMPARE" => await RunCompareAsync(args[1..]).ConfigureAwait(false),
    "UPDATE-BASELINE" => await RunUpdateBaselineAsync(args[1..]).ConfigureAwait(false),
    "REPORT" => await RunReportAsync(args[1..]).ConfigureAwait(false),
    _ => PrintUsage()
};

static int PrintUsage()
{
    Console.WriteLine("Surgewave Performance Regression Suite");
    Console.WriteLine();
    Console.WriteLine("Usage:");
    Console.WriteLine("  compare <results-json> <baseline-json> [-o report.md] [--config config.json] [--fail-on-regression]");
    Console.WriteLine("  update-baseline <results-json> <baseline-json>");
    Console.WriteLine("  report <results-json> <baseline-json> -o report.md");
    Console.WriteLine();
    Console.WriteLine("Commands:");
    Console.WriteLine("  compare           Compare benchmark results against baseline and report regressions");
    Console.WriteLine("  update-baseline   Merge benchmark results into an existing baseline file");
    Console.WriteLine("  report            Generate a Markdown regression report (always writes to file)");
    return 1;
}

static async Task<int> RunCompareAsync(string[] args)
{
    if (args.Length < 2)
    {
        Console.Error.WriteLine("Error: compare requires <results-json> and <baseline-json> arguments.");
        return 1;
    }

    var resultsPath = args[0];
    var baselinePath = args[1];
    string? outputPath = null;
    string? configPath = null;
    var failOnRegression = false;

    for (int i = 2; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "-o" when i + 1 < args.Length:
                outputPath = args[++i];
                break;
            case "--config" when i + 1 < args.Length:
                configPath = args[++i];
                break;
            case "--fail-on-regression":
                failOnRegression = true;
                break;
        }
    }

    var config = await ConfigLoader.LoadAsync(configPath).ConfigureAwait(false);
    var currentResults = await BenchmarkResultParser.ParseBdnReportFileAsync(resultsPath).ConfigureAwait(false);
    var baseline = await BaselineManager.LoadAsync(baselinePath).ConfigureAwait(false);

    var detector = new RegressionDetector(config);
    var results = detector.Compare(baseline.Benchmarks, currentResults);

    var report = RegressionReportGenerator.GenerateMarkdown(results, baseline.Benchmarks, currentResults);
    Console.WriteLine(report);

    if (!string.IsNullOrEmpty(outputPath))
    {
        var dir = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }

        await File.WriteAllTextAsync(outputPath, report).ConfigureAwait(false);
        Console.WriteLine($"Report written to: {outputPath}");
    }

    var regressionCount = results.Count(r => r.Severity == RegressionSeverity.Regression);
    if (regressionCount > 0)
    {
        Console.Error.WriteLine($"Detected {regressionCount} regression(s).");
        return failOnRegression ? 1 : 0;
    }

    Console.WriteLine("No regressions detected.");
    return 0;
}

static async Task<int> RunUpdateBaselineAsync(string[] args)
{
    if (args.Length < 2)
    {
        Console.Error.WriteLine("Error: update-baseline requires <results-json> and <baseline-json> arguments.");
        return 1;
    }

    var resultsPath = args[0];
    var baselinePath = args[1];

    var currentResults = await BenchmarkResultParser.ParseBdnReportFileAsync(resultsPath).ConfigureAwait(false);
    var existing = await BaselineManager.LoadAsync(baselinePath).ConfigureAwait(false);
    var merged = BaselineManager.MergeResults(existing, currentResults);

    await BaselineManager.SaveAsync(baselinePath, merged).ConfigureAwait(false);

    Console.WriteLine($"Baseline updated: {baselinePath}");
    Console.WriteLine($"  Total benchmarks: {merged.Benchmarks.Count}");
    Console.WriteLine($"  New/updated: {currentResults.Count}");
    return 0;
}

static async Task<int> RunReportAsync(string[] args)
{
    if (args.Length < 2)
    {
        Console.Error.WriteLine("Error: report requires <results-json> and <baseline-json> arguments.");
        return 1;
    }

    var resultsPath = args[0];
    var baselinePath = args[1];
    string? outputPath = null;
    string? configPath = null;

    for (int i = 2; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "-o" when i + 1 < args.Length:
                outputPath = args[++i];
                break;
            case "--config" when i + 1 < args.Length:
                configPath = args[++i];
                break;
        }
    }

    if (string.IsNullOrEmpty(outputPath))
    {
        Console.Error.WriteLine("Error: report command requires -o <output-path> argument.");
        return 1;
    }

    var config = await ConfigLoader.LoadAsync(configPath).ConfigureAwait(false);
    var currentResults = await BenchmarkResultParser.ParseBdnReportFileAsync(resultsPath).ConfigureAwait(false);
    var baseline = await BaselineManager.LoadAsync(baselinePath).ConfigureAwait(false);

    var detector = new RegressionDetector(config);
    var results = detector.Compare(baseline.Benchmarks, currentResults);

    var report = RegressionReportGenerator.GenerateMarkdown(results, baseline.Benchmarks, currentResults);

    var dir = Path.GetDirectoryName(outputPath);
    if (!string.IsNullOrEmpty(dir))
    {
        Directory.CreateDirectory(dir);
    }

    await File.WriteAllTextAsync(outputPath, report).ConfigureAwait(false);
    Console.WriteLine($"Report written to: {outputPath}");
    return 0;
}
