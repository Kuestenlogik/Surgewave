using Kuestenlogik.Surgewave.Benchmarks.RealWorld;

// ─────────────────────────────────────────────────────────────────────────────
// Surgewave Real-World Benchmark Suite
//
// Unlike BenchmarkDotNet microbenchmarks, these are scenario-based tests that
// exercise Surgewave under realistic conditions: multi-broker cluster, disk I/O,
// replication, sustained load, and failure recovery.
//
// Usage:
//   surgewave-benchmark [scenario] [options]
//
// Scenarios:
//   throughput    Max throughput measurement
//   latency       End-to-end latency percentiles
//   scaling       Linear scaling verification
//   replication   Replication overhead measurement
//   consumer      Consumer performance scaling
//   failover      Failover impact measurement
//   storage       Storage engine comparison
//   all           Run all scenarios
//
// Options:
//   --brokers N        Number of brokers (default: 3)
//   --messages N       Messages to produce (default: 100000)
//   --message-size N   Message size in bytes (default: 100)
//   --duration N       Max duration in seconds (default: 60)
//   --batch-size N     Batch size for batching producer (default: 1000)
//   --output FILE      Save results to JSON file
//   --compare FILE     Compare against baseline
//   --report FILE      Generate markdown report
// ─────────────────────────────────────────────────────────────────────────────

var config = new BenchmarkConfig();
var scenario = "all";

for (int i = 0; i < args.Length; i++)
{
    switch (args[i].ToLowerInvariant())
    {
        case "--brokers" when i + 1 < args.Length:
            config.BrokerCount = int.Parse(args[++i]);
            break;
        case "--messages" when i + 1 < args.Length:
            config.MessageCount = int.Parse(args[++i]);
            break;
        case "--message-size" when i + 1 < args.Length:
            config.MessageSizeBytes = int.Parse(args[++i]);
            break;
        case "--duration" when i + 1 < args.Length:
            config.DurationSeconds = int.Parse(args[++i]);
            break;
        case "--batch-size" when i + 1 < args.Length:
            config.BatchSize = int.Parse(args[++i]);
            break;
        case "--output" when i + 1 < args.Length:
            config.OutputPath = args[++i];
            break;
        case "--compare" when i + 1 < args.Length:
            config.ComparePath = args[++i];
            break;
        case "--report" when i + 1 < args.Length:
            config.ReportPath = args[++i];
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

var runner = new BenchmarkRunner(config);
await runner.RunAsync(scenario);

static void PrintUsage()
{
    Console.WriteLine("""
        Surgewave Real-World Benchmark Suite
        =================================

        Usage:
          dotnet run -- [scenario] [options]

        Scenarios:
          throughput      Max throughput measurement
          latency         End-to-end latency percentiles (P50/P90/P99/P99.9/P99.99)
          scaling         Linear scaling verification (1, 2, 3, 5 brokers)
          replication     Replication overhead measurement (RF=1 vs RF=3)
          consumer        Consumer performance scaling (1, 3, 5 consumers)
          failover        Failover impact measurement (crash + recovery)
          storage         Storage engine comparison (Memory/File/Arrow)
          all             Run all scenarios (default)

        Options:
          --brokers N         Number of brokers (default: 3)
          --messages N        Messages to produce (default: 100000)
          --message-size N    Message size in bytes (default: 100)
          --duration N        Max duration per scenario in seconds (default: 60)
          --batch-size N      Batch size for producer (default: 1000)
          --output FILE       Save results to JSON file
          --compare FILE      Compare against baseline JSON
          --report FILE       Generate markdown report

        Examples:
          dotnet run -- throughput --messages 1000000
          dotnet run -- latency --brokers 1 --messages 10000
          dotnet run -- all --output results.json --report report.md
          dotnet run -- storage --message-size 1024
          dotnet run -- throughput --compare baselines/realworld-baseline.json
        """);
}
