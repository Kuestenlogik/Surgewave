using BenchmarkDotNet.Running;
using Kuestenlogik.Surgewave.Benchmarks.Comparison;
using Kuestenlogik.Surgewave.Benchmarks.Integration;
using Kuestenlogik.Surgewave.Benchmarks.Latency;

namespace Kuestenlogik.Surgewave.Benchmarks;

public class Program
{
    public static async Task Main(string[] args)
    {
        // Ensure artifacts directory exists
        BenchmarkConfig.EnsureArtifactsDirectory();

        if (args.Length == 0)
        {
            PrintUsage();
            return;
        }

        var command = args[0].ToLowerInvariant();

        // Handle category-based shortcuts (map to BenchmarkDotNet --allCategories)
        if (command.StartsWith("--category=", StringComparison.Ordinal))
        {
            var category = command["--category=".Length..];
            RunBenchmarkDotNet(["--allCategories=" + category, .. args.Skip(1)]);
            return;
        }

        if (command.StartsWith("-c=", StringComparison.Ordinal))
        {
            var category = command["-c=".Length..];
            RunBenchmarkDotNet(["--allCategories=" + category, .. args.Skip(1)]);
            return;
        }

        switch (command)
        {
            // === CATEGORY SHORTCUTS ===
            case "unit":
                RunBenchmarkDotNet(["--allCategories=Unit", .. args.Skip(1)]);
                break;

            case "storage":
                RunBenchmarkDotNet(["--allCategories=Storage", .. args.Skip(1)]);
                break;

            case "simd":
                RunBenchmarkDotNet(["--allCategories=Simd", .. args.Skip(1)]);
                break;

            case "compression":
                RunBenchmarkDotNet(["--allCategories=Compression", .. args.Skip(1)]);
                break;

            case "serialization":
                RunBenchmarkDotNet(["--allCategories=Serialization", .. args.Skip(1)]);
                break;

            case "protocol":
                RunBenchmarkDotNet(["--allCategories=Protocol", .. args.Skip(1)]);
                break;

            case "streams":
                RunBenchmarkDotNet(["--allCategories=Streams", .. args.Skip(1)]);
                break;

            case "sharedmemory":
            case "shared-memory":
            case "ringbuffer":
                RunBenchmarkDotNet(["--allCategories=SharedMemory", .. args.Skip(1)]);
                break;

            case "protocols":
                RunBenchmarkDotNet(["--allCategories=ProtocolComparison", .. args.Skip(1)]);
                break;

            case "amqp":
                RunBenchmarkDotNet(["--allCategories=AmqpProtocol", .. args.Skip(1)]);
                break;

            case "transport-compare":
                RunBenchmarkDotNet(["--allCategories=TransportComparison", .. args.Skip(1)]);
                break;

            case "peer-transport":
                RunBenchmarkDotNet(["--filter", "*PeerTransport*", .. args.Skip(1)]);
                break;

            case "statestore":
                RunBenchmarkDotNet(["--allCategories=StateStore", .. args.Skip(1)]);
                break;

            case "streams-latency":
                await Streams.StreamsLatencyBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            // === LATENCY BENCHMARKS (P50/P90/P99/P99.9/P99.99) ===
            case "latency":
                await RunLatencyBenchmarkAsync(args);
                break;

            case "latency-compare":
                await RunLatencyComparisonAsync(args);
                break;

            // === THROUGHPUT BENCHMARKS ===
            case "embedded":
                await EmbeddedThroughputBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "replication":
                await ReplicationThroughputBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            // Enterprise plugin: Kuestenlogik.Surgewave.Transport.SharedMemory
            // case "transport":
            //     await TransportLatencyTest.RunAsync(args.Skip(1).ToArray());
            //     break;

            // === COMPETITOR-FIRST BENCHMARK COMMANDS ===
            case "benchmark-kafka":
                await KafkaBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "benchmark-redpanda":
                await RedpandaBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "benchmark-pulsar":
                await PulsarBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "benchmark-nats":
                await NatsBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "benchmark-rabbitmq":
                await RabbitMqBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "benchmark-surgewave":
                await SurgewaveBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            // === COMPARISON COMMANDS ===
            case "compare":
                await CompareCommand.RunAsync(args.Skip(1).ToArray());
                break;

            case "broker":
                await BrokerComparisonBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "kafka":
                await KafkaOnlyBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "kafka-surgewave":
                await KafkaClientToSurgewaveBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "embedded-compare":
                await EmbeddedProtocolComparisonBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            // === LEGACY ALIASES ===
            case "surgewave-vs-kafka":           // legacy -> benchmark-kafka
                await KafkaBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "surgewave-vs-redpanda":        // legacy -> benchmark-redpanda
                await RedpandaBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "surgewave-vs-pulsar":          // legacy -> benchmark-pulsar
                await PulsarBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "surgewave-vs-nats":            // legacy -> benchmark-nats
                await NatsBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "surgewave-vs-rabbitmq":        // legacy -> benchmark-rabbitmq
                await RabbitMqBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "vs-kafka":
                await KafkaComparisonBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            case "client-compare":
                await ClientComparisonBenchmark.RunAsync(args.Skip(1).ToArray());
                break;

            // === BENCHMARKDOTNET RUNNER ===
            default:
                RunBenchmarkDotNet(args);
                break;
        }
    }

    private static async Task RunLatencyBenchmarkAsync(string[] args)
    {
        var msgCount = args.Length > 1 ? int.Parse(args[1]) : 10000;
        var msgSize = args.Length > 2 ? int.Parse(args[2]) : 100;
        var storageMode = args.Length > 3 ? args[3] : "memory";
        await LatencyBenchmark.RunAsync(msgCount, msgSize, storageMode);
    }

    private static async Task RunLatencyComparisonAsync(string[] args)
    {
        var msgCount = args.Length > 1 ? int.Parse(args[1]) : 5000;
        var msgSize = args.Length > 2 ? int.Parse(args[2]) : 100;
        var kafkaBootstrap = args.Length > 3 ? args[3] : "localhost:29092";
        await LatencyComparisonBenchmark.RunAsync(msgCount, msgSize, kafkaBootstrap);
    }

    private static void RunBenchmarkDotNet(string[] args)
    {
        // Get all benchmark types from all category assemblies
        var assemblies = new[]
        {
            typeof(Kuestenlogik.Surgewave.Benchmarks.Unit.SerializationBenchmarks).Assembly,
            typeof(Kuestenlogik.Surgewave.Benchmarks.Storage.ThroughputBenchmarks).Assembly,
            typeof(Kuestenlogik.Surgewave.Benchmarks.Integration.EndToEndBenchmarks).Assembly,
            typeof(Kuestenlogik.Surgewave.Benchmarks.Comparison.ComparisonBenchmark).Assembly,
            typeof(Kuestenlogik.Surgewave.Benchmarks.Latency.LatencyBenchmark).Assembly,
            typeof(Kuestenlogik.Surgewave.Benchmarks.Streams.StateStoreBenchmarks).Assembly,
            typeof(Kuestenlogik.Surgewave.Benchmarks.Transport.ProtocolComparisonBenchmarks).Assembly
        };

        BenchmarkSwitcher.FromAssemblies(assemblies).Run(args, BenchmarkConfig.Create());
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Surgewave Benchmark Suite");
        Console.WriteLine("=====================");
        Console.WriteLine($"Results output: {BenchmarkConfig.ArtifactsPath}");
        Console.WriteLine();
        Console.WriteLine("CATEGORY SHORTCUTS (runs BenchmarkDotNet with category filter):");
        Console.WriteLine("  dotnet run -- unit                           - All unit benchmarks");
        Console.WriteLine("  dotnet run -- storage                        - All storage benchmarks");
        Console.WriteLine("  dotnet run -- simd                           - All SIMD benchmarks");
        Console.WriteLine("  dotnet run -- compression                    - All compression benchmarks");
        Console.WriteLine("  dotnet run -- serialization                  - All serialization benchmarks");
        Console.WriteLine("  dotnet run -- protocol                       - All protocol benchmarks");
        Console.WriteLine("  dotnet run -- streams                         - All Streams BenchmarkDotNet");
        Console.WriteLine("  dotnet run -- sharedmemory                   - SPSC ring buffer + MappedFileStore");
        Console.WriteLine("  dotnet run -- protocols                      - MQTT/WebSocket/Native/Kafka protocol comparison");
        Console.WriteLine("  dotnet run -- amqp                           - AMQP 0.9.1 frame read/write + topic-mapping benchmarks");
        Console.WriteLine("  dotnet run -- transport-compare              - SHM ring buffer vs TCP MemoryStream comparison");
        Console.WriteLine("  dotnet run -- statestore                     - State store BenchmarkDotNet");
        Console.WriteLine("  dotnet run -- streams-latency [op] [count] [store]");
        Console.WriteLine("                                    - Streams P50/P90/P99/P99.9/P99.99");
        Console.WriteLine("                                      ops: all|statestore|topology|join|window|serde");
        Console.WriteLine("                                      stores: all|inmemory|rocksdb|sqlite|mappedfile|caching");
        Console.WriteLine("  dotnet run -- --category=Latency             - Custom category filter");
        Console.WriteLine("  dotnet run -- -c=P99                         - Short form");
        Console.WriteLine();
        Console.WriteLine("LATENCY BENCHMARKS (P50/P90/P99/P99.9/P99.99):");
        Console.WriteLine("  dotnet run -- latency [msgCount] [msgSize] [storage]");
        Console.WriteLine("                                    - Surgewave Native vs Kafka client latency");
        Console.WriteLine("  dotnet run -- latency-compare [msgCount] [msgSize] [kafkaBootstrap]");
        Console.WriteLine("                                    - Surgewave vs Real Kafka broker latency");
        Console.WriteLine();
        Console.WriteLine("THROUGHPUT BENCHMARKS:");
        Console.WriteLine("  dotnet run -- embedded [msgCount] [msgSize] [batchSize] [storage]");
        Console.WriteLine("                                    - Embedded broker throughput");
        Console.WriteLine("  dotnet run -- transport [msgCount] [msgSize]");
        Console.WriteLine("                                    - Ring buffer/transport latency");
        Console.WriteLine();
        Console.WriteLine("COMPETITOR-FIRST BENCHMARKS (save JSON to artifacts/benchmarks/results/):");
        Console.WriteLine("  dotnet run -- benchmark-surgewave    [msgCount] [msgSize]");
        Console.WriteLine("                                    - Surgewave embedded (Kafka + Native protocol)");
        Console.WriteLine("  dotnet run -- benchmark-kafka    [msgCount] [msgSize] [bootstrap] [--include-surgewave]");
        Console.WriteLine("                                    - Apache Kafka (optionally +Surgewave)");
        Console.WriteLine("  dotnet run -- benchmark-redpanda [msgCount] [msgSize] [bootstrap] [--include-surgewave]");
        Console.WriteLine("                                    - Redpanda (optionally +Surgewave)");
        Console.WriteLine("  dotnet run -- benchmark-pulsar   [msgCount] [msgSize] [bootstrap] [--include-surgewave]");
        Console.WriteLine("                                    - Apache Pulsar via KoP (optionally +Surgewave)");
        Console.WriteLine("  dotnet run -- benchmark-nats     [msgCount] [msgSize] [natsUrl]   [--include-surgewave]");
        Console.WriteLine("                                    - NATS JetStream (optionally +Surgewave)");
        Console.WriteLine("  dotnet run -- benchmark-rabbitmq [msgCount] [msgSize] [host]      [--include-surgewave]");
        Console.WriteLine("                                    - RabbitMQ AMQP 0.9.1 (optionally +Surgewave)");
        Console.WriteLine("  dotnet run -- compare            - Cross-platform table from all saved JSON results");
        Console.WriteLine();
        Console.WriteLine("COMPARISON BENCHMARKS:");
        Console.WriteLine("  dotnet run -- kafka-surgewave [msgCount] [msgSize] - Kafka client vs Surgewave broker");
        Console.WriteLine("  dotnet run -- embedded-compare [msgCount] [msgSize] [batchSize]");
        Console.WriteLine("                                    - Native vs Kafka protocol (embedded)");
        Console.WriteLine("  dotnet run -- vs-kafka [msgCount] [msgSize]");
        Console.WriteLine("                                    - Surgewave vs Real Kafka (Testcontainers)");
        Console.WriteLine();
        Console.WriteLine("LEGACY ALIASES (backward compatible):");
        Console.WriteLine("  surgewave-vs-kafka     -> benchmark-kafka");
        Console.WriteLine("  surgewave-vs-redpanda  -> benchmark-redpanda");
        Console.WriteLine("  surgewave-vs-pulsar    -> benchmark-pulsar");
        Console.WriteLine("  surgewave-vs-nats      -> benchmark-nats");
        Console.WriteLine("  surgewave-vs-rabbitmq  -> benchmark-rabbitmq");
        Console.WriteLine();
        Console.WriteLine("CROSS-SYSTEM (requires running brokers):");
        Console.WriteLine("  dotnet run -- kafka [msgCount] [msgSize]     - Kafka-only baseline");
        Console.WriteLine("  dotnet run -- broker [msgCount] [msgSize] [surgewaveHost] [kafkaHost]");
        Console.WriteLine("                                               - Surgewave vs Kafka broker");
        Console.WriteLine();
        Console.WriteLine("BENCHMARKDOTNET (native args):");
        Console.WriteLine("  dotnet run -- --list tree                    - List all benchmarks");
        Console.WriteLine("  dotnet run -- --filter *Serialization*       - Filter by name");
        Console.WriteLine("  dotnet run -- --allCategories=Unit           - All benchmarks in category");
        Console.WriteLine("  dotnet run -- --anyCategories=P99,Latency    - Any matching category");
        Console.WriteLine();
        Console.WriteLine("Available categories:");
        Console.WriteLine("  Primary:   Unit, Storage, Integration, Comparison, Latency, Streams, SharedMemory");
        Console.WriteLine("  Transport: ProtocolComparison, TransportComparison, AmqpProtocol");
        Console.WriteLine("  Features:  Serialization, Compression, Protocol, Simd, BufferPool, Throughput");
        Console.WriteLine("  Streams:   StateStore, Topology, Window, Join, Serde");
        Console.WriteLine("  Latency:   P50, P90, P99, P99.9, P99.99, EndToEnd");
        Console.WriteLine("  Systems:   Kafka, Redpanda, Native, Embedded");
        Console.WriteLine();
    }
}
