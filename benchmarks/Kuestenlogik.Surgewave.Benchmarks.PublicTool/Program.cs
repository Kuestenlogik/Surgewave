using System.CommandLine;
using Kuestenlogik.Surgewave.Benchmarks.Public;
using Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;
using Spectre.Console;

// Standalone dotnet-tool entry point. The actual benchmark execution
// + reporting lives in Kuestenlogik.Surgewave.Benchmarks.Public so
// the `surgewave bench public` CLI subcommand can call the exact
// same engine — see src/Kuestenlogik.Surgewave.Tool/Commands/Bench.

var outputOption = new Option<FileInfo?>("--output", "-o")
{
    Description = "Path to write the Markdown report. Default: stdout.",
};
var jsonOption = new Option<FileInfo?>("--json")
{
    Description = "Path to write the JSON sidecar (for --compare). Default: skip JSON.",
};
var compareOption = new Option<FileInfo?>("--compare")
{
    Description = "Reference JSON file to diff against — adds a Δ% table to the report.",
};
var messageCountOption = new Option<int>("--message-count")
{
    Description = "Messages per scenario per system.",
    DefaultValueFactory = _ => 100_000,
};
var payloadOption = new Option<int>("--payload")
{
    Description = "Payload size in bytes.",
    DefaultValueFactory = _ => 1_024,
};
var batchOption = new Option<int>("--batch-size")
{
    Description = "Producer batch size.",
    DefaultValueFactory = _ => 16_384,
};
var compressionOption = new Option<string>("--compression")
{
    Description = "Compression codec (none, gzip, snappy, lz4, zstd).",
    DefaultValueFactory = _ => "lz4",
};

var publicCommand = new Command("public", "Run the curated G3 public benchmark suite.")
{
    outputOption,
    jsonOption,
    compareOption,
    messageCountOption,
    payloadOption,
    batchOption,
    compressionOption,
};

publicCommand.SetAction(async (parseResult, ct) =>
{
    var options = new PublicBenchmarkOptions(
        MessageCount: parseResult.GetValue(messageCountOption),
        PayloadBytes: parseResult.GetValue(payloadOption),
        BatchSize: parseResult.GetValue(batchOption),
        CompressionCodec: parseResult.GetValue(compressionOption) ?? "lz4",
        Acks: -1,
        ReplicationFactor: 1,
        WarmupRounds: 1,
        MeasurementRounds: 3);

    var progress = new Progress<string>(msg => AnsiConsole.MarkupLineInterpolated($"[dim]→[/] {msg}"));
    var runner = new PublicBenchmarkRunner();
    var report = await runner.RunAsync(options, progress, ct).ConfigureAwait(false);

    var comparedAgainst = parseResult.GetValue(compareOption)?.Name;
    var markdown = MarkdownReporter.Render(report, comparedAgainst);

    var compareFile = parseResult.GetValue(compareOption);
    if (compareFile is not null && compareFile.Exists)
    {
        var reference = JsonResultStore.LoadFromFile(compareFile.FullName);
        markdown += "\n" + CompareReporter.Render(report, reference);
    }

    var outFile = parseResult.GetValue(outputOption);
    if (outFile is null)
    {
        Console.Out.Write(markdown);
    }
    else
    {
        await File.WriteAllTextAsync(outFile.FullName, markdown, ct).ConfigureAwait(false);
        AnsiConsole.MarkupLineInterpolated($"[green]✓[/] Markdown report → {outFile.FullName}");
    }

    var jsonFile = parseResult.GetValue(jsonOption);
    if (jsonFile is not null)
    {
        JsonResultStore.SaveToFile(report, jsonFile.FullName);
        AnsiConsole.MarkupLineInterpolated($"[green]✓[/] JSON sidecar  → {jsonFile.FullName}");
    }

    return 0;
});

var root = new RootCommand("Surgewave Public Benchmark Suite")
{
    publicCommand,
};

return await root.Parse(args).InvokeAsync().ConfigureAwait(false);
