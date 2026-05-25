using System.Diagnostics;
using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Kuestenlogik.Surgewave.Streams.Testing;
using Kuestenlogik.Surgewave.Streams.Windows;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Benchmarks.Streams;

/// <summary>
/// Console-based Streams latency benchmark measuring P50/P90/P99/P99.9/P99.99 percentiles
/// for state store operations, topology processing, and join operations.
///
/// Compares all store backends and topology patterns side-by-side.
///
/// Run with: dotnet run -- streams-latency [operation] [recordCount] [storeType]
///
/// Operations: all, statestore, topology, join, window, serde
/// Store types: all, inmemory, rocksdb, sqlite, mappedfile, caching
/// </summary>
public static class StreamsLatencyBenchmark
{
    public static async Task RunAsync(string[] args)
    {
        var operation = args.Length > 0 ? args[0].ToLowerInvariant() : "all";
        var recordCount = args.Length > 1 ? int.Parse(args[1]) : 10_000;
        var storeFilter = args.Length > 2 ? args[2].ToLowerInvariant() : "all";

        Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     STREAMS LATENCY BENCHMARK (P50/P90/P99/P99.9/P99.99)         ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");
        Console.WriteLine($"Operation:    {operation}");
        Console.WriteLine($"Records:      {recordCount:N0}");
        Console.WriteLine($"Store filter: {storeFilter}");
        Console.WriteLine();

        if (operation is "all" or "statestore")
            RunStateStoreBenchmarks(recordCount, storeFilter);

        if (operation is "all" or "topology")
            RunTopologyBenchmarks(recordCount);

        if (operation is "all" or "join")
            RunJoinBenchmarks(recordCount);

        if (operation is "all" or "window")
            RunWindowBenchmarks(recordCount, storeFilter);

        if (operation is "all" or "serde")
            RunSerdeBenchmarks(recordCount);

        await Task.CompletedTask;
    }

    private static void RunStateStoreBenchmarks(int recordCount, string storeFilter)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     STATE STORE LATENCY                                          ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        var storeTypes = GetStoreTypes(storeFilter);
        var putResults = new Dictionary<string, long[]>();
        var getResults = new Dictionary<string, long[]>();
        var deleteResults = new Dictionary<string, long[]>();

        foreach (var storeType in storeTypes)
        {
            Console.WriteLine($"─── {storeType} ───");
            var (store, tempDir) = CreateStore(storeType);
            try
            {
                // Preload
                for (int i = 0; i < recordCount; i++)
                    store.Put($"key-{i:D8}", $"value-{i}-{new string('x', 100)}");

                // Measure Put
                var putLatencies = MeasureLatencies(recordCount, i =>
                    store.Put($"put-{i:D8}", $"new-value-{i}"));
                putResults[storeType] = putLatencies;
                PrintLatencyStats($"{storeType} Put", putLatencies);

                // Measure Get (existing keys)
                var getLatencies = MeasureLatencies(recordCount, i =>
                    store.Get($"key-{i % recordCount:D8}"));
                getResults[storeType] = getLatencies;
                PrintLatencyStats($"{storeType} Get", getLatencies);

                // Measure Delete
                var deleteLatencies = MeasureLatencies(Math.Min(recordCount, 1000), i =>
                {
                    var key = $"key-{i:D8}";
                    store.Delete(key);
                    store.Put(key, $"restored-{i}");
                });
                deleteResults[storeType] = deleteLatencies;
                PrintLatencyStats($"{storeType} Delete+Put", deleteLatencies);
            }
            finally
            {
                store.Dispose();
                CleanupTempDir(tempDir);
            }
            Console.WriteLine();
        }

        if (storeTypes.Length > 1)
        {
            PrintComparisonTable("Put Latency", storeTypes, putResults);
            PrintComparisonTable("Get Latency", storeTypes, getResults);
        }
    }

    private static void RunTopologyBenchmarks(int recordCount)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     TOPOLOGY PROCESSING LATENCY                                  ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        var topologies = new (string Name, Func<TopologyTestDriver> Factory)[]
        {
            ("Passthrough", () =>
            {
                var b = new StreamsBuilder();
                b.Stream<string, string>("input").To("output");
                return new TopologyTestDriver(b.Build());
            }),
            ("Filter", () =>
            {
                var b = new StreamsBuilder();
                b.Stream<string, string>("input").Filter((k, v) => v.Length > 10).To("output");
                return new TopologyTestDriver(b.Build());
            }),
            ("MapValues", () =>
            {
                var b = new StreamsBuilder();
                b.Stream<string, string>("input").MapValues(v => v.ToUpperInvariant()).To("output");
                return new TopologyTestDriver(b.Build());
            }),
            ("Filter+Map+Filter", () =>
            {
                var b = new StreamsBuilder();
                b.Stream<string, string>("input")
                    .Filter((k, v) => v.Length > 5)
                    .MapValues(v => v.ToUpperInvariant())
                    .Filter((k, v) => !v.StartsWith("SKIP", StringComparison.Ordinal))
                    .To("output");
                return new TopologyTestDriver(b.Build());
            })
        };

        var results = new Dictionary<string, long[]>();

        foreach (var (name, factory) in topologies)
        {
            using var driver = factory();
            var input = driver.CreateInputTopic<string, string>("input");

            // Warmup
            for (int i = 0; i < Math.Min(1000, recordCount / 10); i++)
                input.PipeInput($"key-{i}", $"value-{i}-warmup-data");

            var latencies = MeasureLatencies(recordCount, i =>
                input.PipeInput($"key-{i % 100}", $"value-{i}-benchmark-data"));

            results[name] = latencies;
            PrintLatencyStats(name, latencies);
            Console.WriteLine();
        }

        if (results.Count > 1)
            PrintComparisonTable("Topology Processing", [.. results.Keys], results);
    }

    private static void RunJoinBenchmarks(int recordCount)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     JOIN LATENCY                                                 ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        // Stream-Table Join
        {
            var b = new StreamsBuilder();
            var stream = b.Stream<string, string>("stream");
            var table = b.Table<string, string>("table");
            stream.Join(table, (s, t) => $"{s}+{t}").To("output");
            using var driver = new TopologyTestDriver(b.Build());
            var tableInput = driver.CreateInputTopic<string, string>("table");
            var streamInput = driver.CreateInputTopic<string, string>("stream");

            // Populate table
            for (int i = 0; i < recordCount; i++)
                tableInput.PipeInput($"key-{i:D6}", $"table-{i}");

            var latencies = MeasureLatencies(recordCount, i =>
                streamInput.PipeInput($"key-{i % recordCount:D6}", $"stream-{i}"));
            PrintLatencyStats("Stream-Table Join", latencies);
        }

        // Stream-Stream Join
        {
            var b = new StreamsBuilder();
            var left = b.Stream<string, string>("left");
            var right = b.Stream<string, string>("right");
            left.Join(right, (l, r) => $"{l}+{r}", JoinWindows.Of(TimeSpan.FromMinutes(5))).To("output");
            using var driver = new TopologyTestDriver(b.Build());
            var leftInput = driver.CreateInputTopic<string, string>("left");
            var rightInput = driver.CreateInputTopic<string, string>("right");

            var latencies = MeasureLatencies(recordCount, i =>
            {
                leftInput.PipeInput($"key-{i % 100:D6}", $"left-{i}");
                rightInput.PipeInput($"key-{i % 100:D6}", $"right-{i}");
            });
            PrintLatencyStats("Stream-Stream Join", latencies);
        }

        // Table-Table Join
        {
            var b = new StreamsBuilder();
            var left = b.Table<string, string>("left-t");
            var right = b.Table<string, string>("right-t");
            left.Join(right, (l, r) => $"{l}+{r}");
            using var driver = new TopologyTestDriver(b.Build());
            var leftInput = driver.CreateInputTopic<string, string>("left-t");
            var rightInput = driver.CreateInputTopic<string, string>("right-t");

            // Populate right table
            for (int i = 0; i < recordCount; i++)
                rightInput.PipeInput($"key-{i:D6}", $"right-{i}");

            var latencies = MeasureLatencies(recordCount, i =>
                leftInput.PipeInput($"key-{i % recordCount:D6}", $"left-{i}"));
            PrintLatencyStats("Table-Table Join", latencies);
        }

        Console.WriteLine();
    }

    private static void RunWindowBenchmarks(int recordCount, string storeFilter)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     WINDOW STORE LATENCY                                         ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        var windowSize = TimeSpan.FromMinutes(1);
        var retention = TimeSpan.FromHours(24);
        var baseTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // InMemory Window Store
        {
            var store = new InMemoryWindowStore<string, int>("bench", windowSize, retention);
            for (int i = 0; i < recordCount; i++)
                store.Put($"key-{i % 100}", i, baseTs + i * 60_000L);

            var putLatencies = MeasureLatencies(recordCount, i =>
                store.Put($"key-{i % 100}", i, baseTs + (recordCount + i) * 60_000L));
            PrintLatencyStats("InMemory Window Put", putLatencies);

            var fetchLatencies = MeasureLatencies(recordCount, i =>
                store.Fetch($"key-{i % 100}", baseTs + (i % recordCount) * 60_000L));
            PrintLatencyStats("InMemory Window Fetch", fetchLatencies);

            store.Dispose();
        }

        Console.WriteLine();
    }

    private static void RunSerdeBenchmarks(int recordCount)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     SERDE LATENCY                                                ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        var strSerde = Serdes.String();
        var intSerde = Serdes.Int32();
        var jsonSerde = Serdes.Json<SampleRecord>();

        var sampleStr = "benchmark-value-" + new string('x', 100);
        var sampleRecord = new SampleRecord { Id = 1, Name = "test", Value = 42.5 };

        var strBytes = strSerde.Serialize(sampleStr);
        var intBytes = intSerde.Serialize(42);
        var jsonBytes = jsonSerde.Serialize(sampleRecord);

        var strSerialize = MeasureLatencies(recordCount, _ => strSerde.Serialize(sampleStr));
        PrintLatencyStats("String Serialize", strSerialize);

        var strDeserialize = MeasureLatencies(recordCount, _ => strSerde.Deserialize(strBytes));
        PrintLatencyStats("String Deserialize", strDeserialize);

        var intSerialize = MeasureLatencies(recordCount, _ => intSerde.Serialize(42));
        PrintLatencyStats("Int32 Serialize", intSerialize);

        var jsonSerialize = MeasureLatencies(recordCount, _ => jsonSerde.Serialize(sampleRecord));
        PrintLatencyStats("JSON Serialize", jsonSerialize);

        var jsonDeserialize = MeasureLatencies(recordCount, _ => jsonSerde.Deserialize(jsonBytes));
        PrintLatencyStats("JSON Deserialize", jsonDeserialize);

        Console.WriteLine();
    }

    // ═══════════════════════════════════════════════════════════════
    // INFRASTRUCTURE
    // ═══════════════════════════════════════════════════════════════

    private static long[] MeasureLatencies(int count, Action<int> operation)
    {
        var latencies = new long[count];
        for (int i = 0; i < count; i++)
        {
            var start = Stopwatch.GetTimestamp();
            operation(i);
            latencies[i] = Stopwatch.GetTimestamp() - start;
        }
        return latencies;
    }

    private static void PrintLatencyStats(string label, long[] latencies)
    {
        if (latencies.Length == 0)
        {
            Console.WriteLine($"  No data for {label}");
            return;
        }

        Array.Sort(latencies);
        var ticksPerUs = Stopwatch.Frequency / 1_000_000.0;

        Console.WriteLine($"  {label} ({latencies.Length:N0} samples)");
        Console.WriteLine("  Percentile      Latency");
        Console.WriteLine("  ─────────────────────────────");
        Console.WriteLine($"  Min           {FormatLatency(latencies[0], ticksPerUs)}");
        Console.WriteLine($"  P50 (median)  {FormatLatency(GetPercentile(latencies, 50), ticksPerUs)}");
        Console.WriteLine($"  P90           {FormatLatency(GetPercentile(latencies, 90), ticksPerUs)}");
        Console.WriteLine($"  P99           {FormatLatency(GetPercentile(latencies, 99), ticksPerUs)}");
        Console.WriteLine($"  P99.9         {FormatLatency(GetPercentile(latencies, 99.9), ticksPerUs)}");
        Console.WriteLine($"  P99.99        {FormatLatency(GetPercentile(latencies, 99.99), ticksPerUs)}");
        Console.WriteLine($"  Max           {FormatLatency(latencies[^1], ticksPerUs)}");
        Console.WriteLine($"  Avg           {FormatLatency((long)latencies.Average(), ticksPerUs)}");
        Console.WriteLine();
    }

    private static void PrintComparisonTable(string title, string[] labels, Dictionary<string, long[]> results)
    {
        var ticksPerUs = Stopwatch.Frequency / 1_000_000.0;

        Console.WriteLine($"  ═══ {title} Comparison ═══");
        Console.WriteLine();

        // Header
        var header = "  Percentile    ";
        foreach (var label in labels)
            header += $"| {label,-16}";
        Console.WriteLine(header);
        Console.WriteLine("  " + new string('─', header.Length));

        var percentiles = new[] { ("P50", 50.0), ("P90", 90.0), ("P99", 99.0), ("P99.9", 99.9), ("P99.99", 99.99) };

        foreach (var (name, pct) in percentiles)
        {
            var line = $"  {name,-14} ";
            foreach (var label in labels)
            {
                if (results.TryGetValue(label, out var latencies) && latencies.Length > 0)
                {
                    var sorted = (long[])latencies.Clone();
                    Array.Sort(sorted);
                    line += $"| {FormatLatency(GetPercentile(sorted, pct), ticksPerUs),-16}";
                }
                else
                {
                    line += $"| {"N/A",-16}";
                }
            }
            Console.WriteLine(line);
        }
        Console.WriteLine();
    }

    private static long GetPercentile(long[] sortedLatencies, double percentile)
    {
        var index = (int)Math.Ceiling(percentile / 100.0 * sortedLatencies.Length) - 1;
        return sortedLatencies[Math.Max(0, Math.Min(index, sortedLatencies.Length - 1))];
    }

    private static string FormatLatency(long ticks, double ticksPerMicrosecond)
    {
        var microseconds = ticks / ticksPerMicrosecond;
        return microseconds switch
        {
            < 1 => $"{microseconds * 1000:F0} ns",
            < 1000 => $"{microseconds:F1} µs",
            < 1_000_000 => $"{microseconds / 1000:F2} ms",
            _ => $"{microseconds / 1_000_000:F2} s"
        };
    }

    private static string[] GetStoreTypes(string filter) => filter switch
    {
        "inmemory" => ["InMemory"],
        "rocksdb" => ["RocksDb"],
        "sqlite" => ["Sqlite"],
        "mappedfile" => ["MappedFile"],
        "caching" => ["Caching"],
        _ => ["InMemory", "RocksDb", "Sqlite", "MappedFile", "Caching"]
    };

    private static (IKeyValueStore<string, string> Store, string TempDir) CreateStore(string storeType)
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "surgewave-bench-latency", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        using var metrics = new StreamsMetrics();
        var config = new StreamsConfig
        {
            ApplicationId = "bench",
            BootstrapServers = "dummy:9092",
            StateDir = tempDir
        };
        var context = new ProcessorContext(config, metrics, NullLogger.Instance);

        var keySerde = Serdes.String();
        var valueSerde = Serdes.String();

        IKeyValueStore<string, string> store = storeType switch
        {
            "RocksDb" => new RocksDbKeyValueStore<string, string>("bench", keySerde, valueSerde),
            "Sqlite" => new SqliteKeyValueStore<string, string>("bench", keySerde, valueSerde),
            "MappedFile" => new MappedFileKeyValueStore<string, string>("bench", keySerde, valueSerde),
#pragma warning disable CA2000
            "Caching" => new CachingKeyValueStore<string, string>(
                new InMemoryKeyValueStore<string, string>("bench-inner"), maxCacheSize: 50_000),
#pragma warning restore CA2000
            _ => new InMemoryKeyValueStore<string, string>("bench")
        };

        store.Init(context);
        return (store, tempDir);
    }

    private static void CleanupTempDir(string tempDir)
    {
        try
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, recursive: true);
        }
        catch { /* ignore */ }
    }

    private sealed class SampleRecord
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public double Value { get; set; }
    }
}
