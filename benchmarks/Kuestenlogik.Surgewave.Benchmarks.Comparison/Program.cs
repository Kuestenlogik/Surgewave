using Kuestenlogik.Surgewave.Benchmarks.Comparison;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Infrastructure;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Reporting;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Scenarios;
using Spectre.Console;

// ─────────────────────────────────────────────────────────────────────────────
// Surgewave Multi-Platform Comparison Benchmark
//
// Competitor-first commands (each saves a JSON result to artifacts/benchmarks/results/):
//   benchmark-kafka    [msgCount] [msgSize] [bootstrap] [--include-surgewave]
//   benchmark-redpanda [msgCount] [msgSize] [bootstrap] [--include-surgewave]
//   benchmark-pulsar   [msgCount] [msgSize] [bootstrap] [--include-surgewave]
//   benchmark-nats     [msgCount] [msgSize] [natsUrl]   [--include-surgewave]
//   benchmark-rabbitmq [msgCount] [msgSize] [host]      [--include-surgewave]
//   benchmark-surgewave    [msgCount] [msgSize]
//   compare                                              (loads all JSON results and prints table)
//
// Multi-platform scenario-based commands:
//   dotnet run -- [scenario] [options]
//
// Scenarios:
//   throughput       Throughput comparison
//   latency          Latency comparison
//   batch-size       Batch size impact
//   message-size     Message size impact
//   multi-producer   Producer scaling
//   all              All scenarios (default)
//
// Platform selection:
//   --platforms embedded-native,kafka,redpanda
//   --platforms all           (all 8 configurations)
//   --platforms containers    (5,6,7,8 -- container variants)
//   --platforms fair          (6,7,8 -- all container + kafka protocol)
//   --platforms embedded      (1,2 -- embedded variants)
//   --platforms standalone    (3,4 -- standalone variants)
//   --platforms surgewave         (1-6 -- all Surgewave variants)
//
// Legacy commands (retained for backward compatibility):
//   surgewave-vs-kafka     -> benchmark-kafka
//   surgewave-vs-redpanda  -> benchmark-redpanda
//   surgewave-vs-pulsar    -> benchmark-pulsar
//   surgewave-vs-nats      -> benchmark-nats
//   surgewave-vs-rabbitmq  -> benchmark-rabbitmq
//   kafka-compare      Surgewave vs real Kafka (Testcontainers)
//   embedded-compare   Embedded protocol comparison
//   broker             Broker comparison (external)
//   client             Client comparison
//   kafka-client       Kafka client to Surgewave
//   kafka-only         Kafka-only baseline
//
// Options:
//   --messages N              Messages to produce (default: 100000)
//   --message-size N          Message size in bytes (default: 100)
//   --batch-size N            Batch size (default: 1000)
//   --kafka HOST:PORT         Kafka bootstrap (default: localhost:29092)
//   --surgewave-standalone ADDR   Surgewave standalone address (default: localhost:9092)
//   --surgewave-image IMAGE       Surgewave container image (default: surgewave:latest)
//   --kafka-image IMAGE       Kafka container image (default: confluentinc/cp-kafka:7.6.0)
//   --redpanda-image IMAGE    Redpanda container image (default: redpandadata/redpanda:latest)
//   --output FILE             Save JSON results (scenario-based commands)
//   --report FILE             Generate markdown report (scenario-based commands)
//   --surgewave-only              Skip all non-Surgewave platforms
// ─────────────────────────────────────────────────────────────────────────────

var p = new BenchmarkParams();
var scenario = "all";
var platformsSpecified = false;

for (int i = 0; i < args.Length; i++)
{
    switch (args[i].ToLowerInvariant())
    {
        case "--messages" when i + 1 < args.Length:
            p.MessageCount = int.Parse(args[++i]);
            break;
        case "--message-size" when i + 1 < args.Length:
            p.MessageSizeBytes = int.Parse(args[++i]);
            break;
        case "--batch-size" when i + 1 < args.Length:
            p.BatchSize = int.Parse(args[++i]);
            break;
        case "--kafka" when i + 1 < args.Length:
            p.KafkaBootstrap = args[++i];
            break;
        case "--surgewave-standalone" when i + 1 < args.Length:
            p.SurgewaveStandaloneAddress = args[++i];
            break;
        case "--surgewave-image" when i + 1 < args.Length:
            p.SurgewaveContainerImage = args[++i];
            break;
        case "--kafka-image" when i + 1 < args.Length:
            p.KafkaContainerImage = args[++i];
            break;
        case "--redpanda-image" when i + 1 < args.Length:
            p.RedpandaContainerImage = args[++i];
            break;
        case "--platforms" when i + 1 < args.Length:
            p.Platforms = ParsePlatforms(args[++i]);
            platformsSpecified = true;
            break;
        case "--output" when i + 1 < args.Length:
            p.OutputPath = args[++i];
            break;
        case "--report" when i + 1 < args.Length:
            p.ReportPath = args[++i];
            break;
        case "--surgewave-only":
            p.SurgewaveOnly = true;
            p.Platforms = [BenchmarkPlatform.SurgewaveEmbeddedNative];
            platformsSpecified = true;
            break;
        case "--help" or "-h":
            PrintUsage();
            return;
        default:
            if (!args[i].StartsWith('-'))
                scenario = args[i];
            break;
    }
}

// If --surgewave-only was set but platforms were also specified, surgewave-only takes precedence
if (p.SurgewaveOnly && !platformsSpecified)
{
    p.Platforms = [BenchmarkPlatform.SurgewaveEmbeddedNative];
}

// ─── Competitor-first commands ────────────────────────────────────────────────

switch (scenario.ToLowerInvariant())
{
    case "benchmark-kafka":
        await KafkaBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "benchmark-redpanda":
        await RedpandaBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "benchmark-pulsar":
        await PulsarBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "benchmark-nats":
        await NatsBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "benchmark-rabbitmq":
        await RabbitMqBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "benchmark-surgewave":
        await SurgewaveBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "compare":
        await CompareCommand.RunAsync(args.Skip(1).ToArray());
        return;
}

// ─── Legacy commands (backward compatibility) ─────────────────────────────────

switch (scenario.ToLowerInvariant())
{
    case "kafka-compare":
        await KafkaComparisonBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "surgewave-vs-kafka":           // legacy
        await KafkaBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "surgewave-vs-redpanda":        // legacy
        await RedpandaBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "surgewave-vs-pulsar":          // legacy
        await PulsarBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "surgewave-vs-nats":            // legacy
        await NatsBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "surgewave-vs-rabbitmq":        // legacy
        await RabbitMqBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "embedded-compare":
        await EmbeddedProtocolComparisonBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "broker":
        await BrokerComparisonBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "client":
        await ClientComparisonBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "kafka-client":
        await KafkaClientToSurgewaveBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
    case "kafka-only":
        await KafkaOnlyBenchmark.RunAsync(args.Skip(1).ToArray());
        return;
}

// ─── New scenario-based commands ─────────────────────────────────────────────

AnsiConsole.Write(new FigletText("Surgewave Compare").Color(Color.Cyan1));

var platformNames = string.Join(", ", p.Platforms.OrderBy(pl => (int)pl).Select(pl => pl.DisplayName()));
AnsiConsole.MarkupLine($"[dim]Messages: {p.MessageCount:N0} | Size: {p.MessageSizeBytes}B | Batch: {p.BatchSize}[/]");
AnsiConsole.MarkupLine($"[dim]Platforms: {platformNames}[/]");
AnsiConsole.WriteLine();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    AnsiConsole.MarkupLine("[yellow]Cancellation requested...[/]");
};

var reports = new List<ComparisonReport>();

try
{
    switch (scenario.ToLowerInvariant())
    {
        case "throughput":
            reports.Add(await new ThroughputComparison().RunAsync(p, cts.Token));
            break;
        case "latency":
            reports.Add(await new LatencyComparison().RunAsync(p, cts.Token));
            break;
        case "batch-size":
            reports.Add(await new BatchSizeComparison().RunAsync(p, cts.Token));
            break;
        case "message-size":
            reports.Add(await new MessageSizeComparison().RunAsync(p, cts.Token));
            break;
        case "multi-producer":
            reports.Add(await new MultiProducerComparison().RunAsync(p, cts.Token));
            break;
        case "all":
            await RunAllAsync(p, reports, cts.Token);
            break;
        default:
            AnsiConsole.MarkupLine($"[red]Unknown scenario: {scenario}[/]");
            AnsiConsole.MarkupLine("[dim]Competitor-first: benchmark-kafka, benchmark-redpanda, benchmark-pulsar, benchmark-nats, benchmark-surgewave, compare[/]");
            AnsiConsole.MarkupLine("[dim]Scenario-based:   throughput, latency, batch-size, message-size, multi-producer, all[/]");
            AnsiConsole.MarkupLine("[dim]Legacy:           surgewave-vs-kafka, surgewave-vs-redpanda, surgewave-vs-pulsar, surgewave-vs-nats, kafka-compare, embedded-compare, broker, client, kafka-client, kafka-only[/]");
            return;
    }
}
finally
{
    // Always clean up containers
    await ContainerManager.StopAllAsync();
}

// Output
ComparisonReportGenerator.PrintToConsole(reports);

if (!string.IsNullOrEmpty(p.OutputPath))
{
    ComparisonReportGenerator.SaveJson(p.OutputPath, reports);
    AnsiConsole.MarkupLine($"[dim]JSON results saved to: {p.OutputPath}[/]");
}

if (!string.IsNullOrEmpty(p.ReportPath))
{
    var markdown = ComparisonReportGenerator.GenerateMarkdown(reports);
    var directory = Path.GetDirectoryName(p.ReportPath);
    if (!string.IsNullOrEmpty(directory))
        Directory.CreateDirectory(directory);
    await File.WriteAllTextAsync(p.ReportPath, markdown);
    AnsiConsole.MarkupLine($"[dim]Markdown report saved to: {p.ReportPath}[/]");
}

static async Task RunAllAsync(BenchmarkParams p, List<ComparisonReport> reports, CancellationToken ct)
{
    var scenarios = new ComparisonScenario[]
    {
        new ThroughputComparison(),
        new LatencyComparison(),
        new BatchSizeComparison(),
        new MessageSizeComparison(),
        new MultiProducerComparison()
    };

    foreach (var s in scenarios)
    {
        try
        {
            reports.Add(await s.RunAsync(p, ct));
        }
        catch (OperationCanceledException)
        {
            AnsiConsole.MarkupLine($"[yellow]Scenario '{s.Name}' cancelled[/]");
            break;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Scenario '{s.Name}' failed: {ex.Message}[/]");
        }
    }
}

static HashSet<BenchmarkPlatform> ParsePlatforms(string input)
{
    // Check for presets first
    var preset = BenchmarkPlatformExtensions.ParsePreset(input);
    if (preset != null)
        return preset;

    // Parse comma-separated platform names
    var platforms = new HashSet<BenchmarkPlatform>();
    foreach (var name in input.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
    {
        // Check if the individual name is a preset
        var subPreset = BenchmarkPlatformExtensions.ParsePreset(name);
        if (subPreset != null)
        {
            foreach (var p in subPreset)
                platforms.Add(p);
            continue;
        }

        if (BenchmarkPlatformExtensions.TryParse(name, out var platform))
        {
            platforms.Add(platform);
        }
        else
        {
            AnsiConsole.MarkupLine($"[yellow]Unknown platform: {name}[/]");
        }
    }

    if (platforms.Count == 0)
    {
        AnsiConsole.MarkupLine("[yellow]No valid platforms specified, using defaults[/]");
        return [BenchmarkPlatform.SurgewaveEmbeddedNative, BenchmarkPlatform.ApacheKafkaContainer];
    }

    return platforms;
}

static void PrintUsage()
{
    Console.WriteLine("""
        Surgewave Multi-Platform Comparison Benchmark
        ==========================================

        Competitor-First Commands (save JSON to artifacts/benchmarks/results/):
          benchmark-surgewave    [msgCount] [msgSize]
          benchmark-kafka    [msgCount] [msgSize] [bootstrap]   [--include-surgewave]
          benchmark-redpanda [msgCount] [msgSize] [bootstrap]   [--include-surgewave]
          benchmark-pulsar   [msgCount] [msgSize] [bootstrap]   [--include-surgewave]
          benchmark-nats     [msgCount] [msgSize] [natsUrl]     [--include-surgewave]
          benchmark-rabbitmq [msgCount] [msgSize] [host]        [--include-surgewave]
          compare                                                (cross-platform table from saved JSONs)

        Scenario-Based Commands:
          dotnet run -- [scenario] [options]

        Scenarios:
          throughput       Throughput comparison (msg/sec, MB/sec)
          latency          Latency comparison (P50/P90/P99)
          batch-size       Batch size impact on throughput
          message-size     Message size impact on throughput
          multi-producer   Producer scaling (1, 3, 5, 10 concurrent)
          all              All scenarios (default)

        Platform Selection (--platforms):
          embedded-native    Surgewave Embedded + Native Client
          embedded-kafka     Surgewave Embedded + Kafka Client
          standalone-native  Surgewave Standalone + Native Client
          standalone-kafka   Surgewave Standalone + Kafka Client
          container-native   Surgewave Container + Native Client
          container-kafka    Surgewave Container + Kafka Client
          kafka              Apache Kafka Container
          redpanda           Redpanda Container

        Platform Presets (--platforms):
          all              All 8 platforms
          containers       Container variants (5,6,7,8)
          fair             Fair comparison (6,7,8 -- all container + Kafka protocol)
          embedded         Embedded variants (1,2)
          standalone       Standalone variants (3,4)
          surgewave            All Surgewave variants (1-6)

        Legacy Commands (backward compatible aliases):
          surgewave-vs-kafka     -> benchmark-kafka
          surgewave-vs-redpanda  -> benchmark-redpanda
          surgewave-vs-pulsar    -> benchmark-pulsar
          surgewave-vs-nats      -> benchmark-nats
          surgewave-vs-rabbitmq  -> benchmark-rabbitmq
          kafka-compare      Surgewave vs real Kafka (Testcontainers)
          embedded-compare   Protocol comparison on embedded broker
          broker             Broker comparison (external)
          client             Client comparison
          kafka-client       Kafka client to Surgewave broker
          kafka-only         Kafka-only baseline

        Options:
          --messages N              Messages to produce (default: 100000)
          --message-size N          Message size in bytes (default: 100)
          --batch-size N            Batch size (default: 1000)
          --platforms SPEC          Platform selection (see above)
          --kafka HOST:PORT         Kafka bootstrap server (default: localhost:29092)
          --surgewave-standalone ADDR   Surgewave standalone address (default: localhost:9092)
          --surgewave-image IMAGE       Surgewave container image (default: surgewave:latest)
          --kafka-image IMAGE       Kafka container image (default: confluentinc/cp-kafka:7.6.0)
          --redpanda-image IMAGE    Redpanda container image (default: redpandadata/redpanda:latest)
          --output FILE             Save JSON results (scenario-based commands)
          --report FILE             Generate markdown report (scenario-based commands)
          --surgewave-only              Skip all non-Surgewave platforms

        Examples:
          dotnet run -- throughput --messages 1000000
          dotnet run -- latency --platforms embedded-native,kafka,redpanda
          dotnet run -- all --platforms all --output results.json --report report.md
          dotnet run -- throughput --platforms fair
          dotnet run -- all --platforms containers --kafka-image confluentinc/cp-kafka:7.7.0
          dotnet run -- multi-producer --surgewave-only
        """);
}
