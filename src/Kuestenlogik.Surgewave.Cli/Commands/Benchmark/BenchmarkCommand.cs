using System.CommandLine;
using System.CommandLine.Parsing;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Benchmark;

/// <summary>
/// Built-in benchmark command (surgewave benchmark)
/// Supports two modes:
/// - throughput: Batched producing for maximum messages/sec
/// - latency: Single message with P50/P99 percentile measurements
/// </summary>
public class BenchmarkCommand : CommandBase
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly Option<string> _modeOpt = new("--mode", "-m") { Description = "Benchmark mode: throughput (batched) or latency (P50/P99)", DefaultValueFactory = _ => "throughput" };
    private readonly Option<int> _messagesOpt = new("--messages", "-n") { Description = "Number of messages to produce", DefaultValueFactory = _ => 100000 };
    private readonly Option<int> _messageSizeOpt = new("--size", "-s") { Description = "Message size in bytes", DefaultValueFactory = _ => 100 };
    private readonly Option<string> _topicOpt = new("--topic", "-t") { Description = "Topic name for benchmark", DefaultValueFactory = _ => "__surgewave_benchmark" };
    private readonly Option<bool> _skipConsumeOpt = new("--produce-only") { Description = "Only run produce benchmark", DefaultValueFactory = _ => false };
    private readonly Option<bool> _cleanupOpt = new("--cleanup") { Description = "Delete benchmark topic after completion", DefaultValueFactory = _ => true };
    private readonly Option<int> _batchSizeOpt = new("--batch-size") { Description = "Batch size for producing/fetching (throughput mode)", DefaultValueFactory = _ => 1000 };

    public BenchmarkCommand() : base("benchmark", "Run produce/consume performance benchmark")
    {
        Options.Add(_modeOpt);
        Options.Add(_messagesOpt);
        Options.Add(_messageSizeOpt);
        Options.Add(_topicOpt);
        Options.Add(_skipConsumeOpt);
        Options.Add(_cleanupOpt);
        Options.Add(_batchSizeOpt);

        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var (host, port) = ParseBootstrapServer(GetBootstrapServers(parseResult));
        var format = GetFormat(parseResult);
        var mode = parseResult.GetValue(_modeOpt) ?? "throughput";
        var messageCount = parseResult.GetValue(_messagesOpt);
        var messageSize = parseResult.GetValue(_messageSizeOpt);
        var topic = parseResult.GetValue(_topicOpt) ?? "__surgewave_benchmark";
        var skipConsume = parseResult.GetValue(_skipConsumeOpt);
        var cleanup = parseResult.GetValue(_cleanupOpt);
        var batchSize = parseResult.GetValue(_batchSizeOpt);

        // For latency mode, use fewer messages by default
        if (mode == "latency" && messageCount == 100000)
        {
            messageCount = 10000; // Default to 10k for latency measurements
        }

        // Generate test payload
        var payloadBytes = Encoding.UTF8.GetBytes(new string('X', messageSize));

        try
        {
            await using var client = new SurgewaveNativeClient(host, port);
            await client.ConnectAsync(ct);

            // Create topic if needed
            var topics = await client.Topics.ListAsync(ct);
            if (!topics.Any(t => t.Name == topic))
            {
                await client.Topics.CreateAsync(topic, 1, 1, ct);
                await Task.Delay(500, ct); // Let topic propagate
            }

            if (format == OutputFormat.Table)
            {
                var modeLabel = mode == "throughput" ? "Throughput (batched)" : "Latency (P50/P99)";
                AnsiConsole.Write(new Rule($"[bold blue]Surgewave Benchmark - {modeLabel}[/]").LeftJustified());
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine($"[dim]Broker:[/] {host}:{port}");
                AnsiConsole.MarkupLine($"[dim]Mode:[/] {mode}");
                AnsiConsole.MarkupLine($"[dim]Messages:[/] {messageCount:N0}");
                AnsiConsole.MarkupLine($"[dim]Message size:[/] {messageSize} bytes");
                AnsiConsole.MarkupLine($"[dim]Total data:[/] {(messageCount * (long)messageSize) / 1024.0 / 1024.0:F2} MB");
                if (mode == "throughput")
                {
                    AnsiConsole.MarkupLine($"[dim]Batch size:[/] {batchSize}");
                }
                AnsiConsole.WriteLine();
            }

            if (mode == "latency")
            {
                var latencyResult = await RunLatencyBenchmark(client, topic, payloadBytes, messageCount, batchSize, skipConsume, format, ct);
                OutputLatencyResults(format, host, port, messageCount, messageSize, latencyResult);
            }
            else
            {
                // Throughput mode (default)
                var produceResult = await RunThroughputProduceBenchmark(client, topic, payloadBytes, messageCount, batchSize, format, ct);

                ConsumeResult? consumeResult = null;
                if (!skipConsume)
                {
                    consumeResult = await RunConsumeBenchmark(client, topic, messageCount, batchSize, messageSize, format, ct);
                }

                OutputThroughputResults(format, host, port, messageCount, messageSize, batchSize, produceResult, consumeResult);
            }

            // Cleanup
            if (cleanup)
            {
                await client.Topics.DeleteAsync(topic, ct);
            }
            return 0;
        }
        catch (Exception ex)
        {
            WriteError($"Benchmark failed: {ex.Message}");
            return 1;
        }
    }

    /// <summary>
    /// Throughput benchmark - uses batching for maximum messages/sec
    /// </summary>
    private async Task<ProduceResult> RunThroughputProduceBenchmark(
        SurgewaveNativeClient client,
        string topic,
        byte[] payload,
        int messageCount,
        int batchSize,
        OutputFormat format,
        CancellationToken ct)
    {
        var sw = new Stopwatch();
        var produced = 0;

        // Pre-build batch
        var batch = new List<(byte[]? Key, byte[] Value)>(batchSize);
        for (int i = 0; i < batchSize; i++)
        {
            batch.Add((null, payload));
        }

        if (format == OutputFormat.Table)
        {
            await AnsiConsole.Progress()
                .AutoClear(false)
                .HideCompleted(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .StartAsync(async ctx =>
                {
                    var task = ctx.AddTask("[cyan]Producing batches[/]", maxValue: messageCount);
                    sw.Start();

                    while (produced < messageCount && !ct.IsCancellationRequested)
                    {
                        var remaining = messageCount - produced;
                        var currentBatchSize = Math.Min(batchSize, remaining);

                        if (currentBatchSize < batchSize)
                        {
                            // Smaller final batch
                            var smallBatch = batch.Take(currentBatchSize).ToList();
                            await client.Messaging.SendBatchAsync(topic, 0, smallBatch, ct);
                        }
                        else
                        {
                            await client.Messaging.SendBatchAsync(topic, 0, batch, ct);
                        }

                        produced += currentBatchSize;
                        task.Value = produced;
                    }

                    sw.Stop();
                    task.StopTask();
                });
        }
        else
        {
            sw.Start();
            while (produced < messageCount && !ct.IsCancellationRequested)
            {
                var remaining = messageCount - produced;
                var currentBatchSize = Math.Min(batchSize, remaining);

                if (currentBatchSize < batchSize)
                {
                    var smallBatch = batch.Take(currentBatchSize).ToList();
                    await client.Messaging.SendBatchAsync(topic, 0, smallBatch, ct);
                }
                else
                {
                    await client.Messaging.SendBatchAsync(topic, 0, batch, ct);
                }

                produced += currentBatchSize;

                if (format == OutputFormat.Plain && produced % 50000 == 0)
                {
                    Console.WriteLineToError($"Produced {produced:N0}/{messageCount:N0}...");
                }
            }
            sw.Stop();
        }

        return new ProduceResult
        {
            MessagesProduced = produced,
            ElapsedMs = sw.ElapsedMilliseconds,
            ThroughputMsgSec = sw.ElapsedMilliseconds > 0 ? produced * 1000.0 / sw.ElapsedMilliseconds : produced,
            ThroughputMBSec = sw.ElapsedMilliseconds > 0 ? (produced * payload.Length) / 1024.0 / 1024.0 * 1000 / sw.ElapsedMilliseconds : 0
        };
    }

    /// <summary>
    /// Latency benchmark - single message produces with precise timing
    /// </summary>
    private async Task<LatencyResult> RunLatencyBenchmark(
        SurgewaveNativeClient client,
        string topic,
        byte[] payload,
        int messageCount,
        int consumeBatchSize,
        bool skipConsume,
        OutputFormat format,
        CancellationToken ct)
    {
        var produceLatencies = new List<double>(messageCount);
        var consumeLatencies = new List<double>(messageCount);
        var sw = new Stopwatch();
        var totalSw = Stopwatch.StartNew();
        // Calculate maxBytes for fetch: batchSize * (messageSize + 100 bytes overhead)
        var consumeMaxBytes = consumeBatchSize * (payload.Length + 100);

        // Produce with latency measurement
        if (format == OutputFormat.Table)
        {
            await AnsiConsole.Progress()
                .AutoClear(false)
                .HideCompleted(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .StartAsync(async ctx =>
                {
                    var task = ctx.AddTask("[cyan]Measuring produce latency[/]", maxValue: messageCount);

                    for (int i = 0; i < messageCount && !ct.IsCancellationRequested; i++)
                    {
                        sw.Restart();
                        await client.Messaging.SendAsync(topic, 0, null, payload, ct);
                        sw.Stop();
                        produceLatencies.Add(sw.Elapsed.TotalMilliseconds);
                        task.Increment(1);
                    }

                    task.StopTask();
                });
        }
        else
        {
            for (int i = 0; i < messageCount && !ct.IsCancellationRequested; i++)
            {
                sw.Restart();
                await client.Messaging.SendAsync(topic, 0, null, payload, ct);
                sw.Stop();
                produceLatencies.Add(sw.Elapsed.TotalMilliseconds);

                if (format == OutputFormat.Plain && (i + 1) % 1000 == 0)
                {
                    Console.WriteLineToError($"Produced {i + 1:N0}/{messageCount:N0}...");
                }
            }
        }

        var produceElapsedMs = totalSw.ElapsedMilliseconds;

        // Consume with latency measurement
        if (!skipConsume)
        {
            totalSw.Restart();
            long offset = 0;
            var consumed = 0;

            if (format == OutputFormat.Table)
            {
                await AnsiConsole.Progress()
                    .AutoClear(false)
                    .HideCompleted(false)
                    .Columns(
                        new TaskDescriptionColumn(),
                        new ProgressBarColumn(),
                        new PercentageColumn(),
                        new SpinnerColumn())
                    .StartAsync(async ctx =>
                    {
                        var task = ctx.AddTask("[green]Measuring consume latency[/]", maxValue: messageCount);

                        while (consumed < messageCount && !ct.IsCancellationRequested)
                        {
                            sw.Restart();
                            var result = await client.Messaging.ReceiveAsync(topic, 0, offset, consumeMaxBytes, maxWaitMs: 0, ct);
                            sw.Stop();

                            if (result.Messages.Count == 0)
                            {
                                await Task.Delay(10, ct);
                                continue;
                            }

                            var perMessageLatency = sw.Elapsed.TotalMilliseconds / result.Messages.Count;
                            foreach (var msg in result.Messages)
                            {
                                consumeLatencies.Add(perMessageLatency);
                                consumed++;
                                offset = msg.Offset + 1;
                                task.Increment(1);
                                if (consumed >= messageCount) break;
                            }
                        }

                        task.StopTask();
                    });
            }
            else
            {
                while (consumed < messageCount && !ct.IsCancellationRequested)
                {
                    sw.Restart();
                    var result = await client.Messaging.ReceiveAsync(topic, 0, offset, consumeMaxBytes, maxWaitMs: 0, ct);
                    sw.Stop();

                    if (result.Messages.Count == 0)
                    {
                        await Task.Delay(10, ct);
                        continue;
                    }

                    var perMessageLatency = sw.Elapsed.TotalMilliseconds / result.Messages.Count;
                    foreach (var msg in result.Messages)
                    {
                        consumeLatencies.Add(perMessageLatency);
                        consumed++;
                        offset = msg.Offset + 1;
                        if (consumed >= messageCount) break;
                    }

                    if (format == OutputFormat.Plain && consumed % 1000 == 0)
                    {
                        Console.WriteLineToError($"Consumed {consumed:N0}/{messageCount:N0}...");
                    }
                }
            }
        }

        var consumeElapsedMs = totalSw.ElapsedMilliseconds;

        // Calculate percentiles
        produceLatencies.Sort();
        consumeLatencies.Sort();

        return new LatencyResult
        {
            MessagesProduced = produceLatencies.Count,
            MessagesConsumed = consumeLatencies.Count,
            ProduceElapsedMs = produceElapsedMs,
            ConsumeElapsedMs = consumeElapsedMs,
            ProduceP50 = GetPercentile(produceLatencies, 50),
            ProduceP95 = GetPercentile(produceLatencies, 95),
            ProduceP99 = GetPercentile(produceLatencies, 99),
            ProduceMin = produceLatencies.Count > 0 ? produceLatencies[0] : 0,
            ProduceMax = produceLatencies.Count > 0 ? produceLatencies[^1] : 0,
            ProduceAvg = produceLatencies.Count > 0 ? produceLatencies.Average() : 0,
            ConsumeP50 = GetPercentile(consumeLatencies, 50),
            ConsumeP95 = GetPercentile(consumeLatencies, 95),
            ConsumeP99 = GetPercentile(consumeLatencies, 99),
            ConsumeMin = consumeLatencies.Count > 0 ? consumeLatencies[0] : 0,
            ConsumeMax = consumeLatencies.Count > 0 ? consumeLatencies[^1] : 0,
            ConsumeAvg = consumeLatencies.Count > 0 ? consumeLatencies.Average() : 0
        };
    }

    private static double GetPercentile(List<double> sortedValues, int percentile)
    {
        if (sortedValues.Count == 0) return 0;
        var index = (int)Math.Ceiling(percentile / 100.0 * sortedValues.Count) - 1;
        return sortedValues[Math.Max(0, Math.Min(index, sortedValues.Count - 1))];
    }

    private async Task<ConsumeResult> RunConsumeBenchmark(
        SurgewaveNativeClient client,
        string topic,
        int expectedMessages,
        int batchSize,
        int messageSize,
        OutputFormat format,
        CancellationToken ct)
    {
        var sw = new Stopwatch();
        var consumed = 0;
        long offset = 0;
        // Calculate maxBytes: batchSize messages * (messageSize + 100 bytes overhead per message)
        var maxBytes = batchSize * (messageSize + 100);

        if (format == OutputFormat.Table)
        {
            await AnsiConsole.Progress()
                .AutoClear(false)
                .HideCompleted(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .StartAsync(async ctx =>
                {
                    var task = ctx.AddTask("[green]Consuming messages[/]", maxValue: expectedMessages);
                    sw.Start();

                    while (consumed < expectedMessages && !ct.IsCancellationRequested)
                    {
                        var result = await client.Messaging.ReceiveAsync(topic, 0, offset, maxBytes, maxWaitMs: 0, ct);
                        if (result.Messages.Count == 0)
                        {
                            await Task.Delay(10, ct);
                            continue;
                        }

                        foreach (var msg in result.Messages)
                        {
                            consumed++;
                            offset = msg.Offset + 1;
                            task.Increment(1);

                            if (consumed >= expectedMessages) break;
                        }
                    }

                    sw.Stop();
                    task.StopTask();
                });
        }
        else
        {
            sw.Start();
            while (consumed < expectedMessages && !ct.IsCancellationRequested)
            {
                var result = await client.Messaging.ReceiveAsync(topic, 0, offset, maxBytes, maxWaitMs: 0, ct);
                if (result.Messages.Count == 0)
                {
                    await Task.Delay(10, ct);
                    continue;
                }

                foreach (var msg in result.Messages)
                {
                    consumed++;
                    offset = msg.Offset + 1;

                    if (consumed >= expectedMessages) break;
                }

                if (format == OutputFormat.Plain && consumed % 10000 == 0)
                {
                    Console.WriteLineToError($"Consumed {consumed:N0}/{expectedMessages:N0}...");
                }
            }
            sw.Stop();
        }

        return new ConsumeResult
        {
            MessagesConsumed = consumed,
            ElapsedMs = sw.ElapsedMilliseconds,
            ThroughputMsgSec = sw.ElapsedMilliseconds > 0 ? consumed * 1000.0 / sw.ElapsedMilliseconds : consumed
        };
    }

    private static void OutputThroughputResults(
        OutputFormat format,
        string host,
        int port,
        int messageCount,
        int messageSize,
        int batchSize,
        ProduceResult produce,
        ConsumeResult? consume)
    {
        if (format == OutputFormat.Json)
        {
            var result = new
            {
                Mode = "throughput",
                Broker = $"{host}:{port}",
                Messages = messageCount,
                MessageSizeBytes = messageSize,
                BatchSize = batchSize,
                Produce = new
                {
                    produce.MessagesProduced,
                    produce.ElapsedMs,
                    produce.ThroughputMsgSec,
                    produce.ThroughputMBSec
                },
                Consume = consume != null ? new
                {
                    consume.MessagesConsumed,
                    consume.ElapsedMs,
                    consume.ThroughputMsgSec
                } : null
            };
            Console.WriteLine(JsonSerializer.Serialize(result, JsonOptions));
        }
        else if (format == OutputFormat.Plain)
        {
            Console.WriteLine($"mode: throughput (batch={batchSize})");
            Console.WriteLine($"produce: {produce.ThroughputMsgSec:N0} msg/s, {produce.ThroughputMBSec:F2} MB/s ({produce.ElapsedMs}ms)");
            if (consume != null)
            {
                Console.WriteLine($"consume: {consume.ThroughputMsgSec:N0} msg/s ({consume.ElapsedMs}ms)");
            }
        }
        else
        {
            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Rule("[bold green]Throughput Results[/]").LeftJustified());
            AnsiConsole.WriteLine();

            var table = new Table()
                .AddColumn("Metric")
                .AddColumn(new TableColumn("Value").RightAligned());

            table.AddRow("[cyan]Produce throughput[/]", $"[bold]{produce.ThroughputMsgSec:N0}[/] msg/s");
            table.AddRow("[cyan]Produce data rate[/]", $"[bold]{produce.ThroughputMBSec:F2}[/] MB/s");
            table.AddRow("[dim]Produce time[/]", $"{produce.ElapsedMs:N0} ms");
            table.AddRow("[dim]Batch size[/]", $"{batchSize:N0}");

            if (consume != null)
            {
                table.AddEmptyRow();
                table.AddRow("[green]Consume throughput[/]", $"[bold]{consume.ThroughputMsgSec:N0}[/] msg/s");
                table.AddRow("[dim]Consume time[/]", $"{consume.ElapsedMs:N0} ms");
            }

            AnsiConsole.Write(table);
        }
    }

    private static void OutputLatencyResults(
        OutputFormat format,
        string host,
        int port,
        int messageCount,
        int messageSize,
        LatencyResult result)
    {
        if (format == OutputFormat.Json)
        {
            var output = new
            {
                Mode = "latency",
                Broker = $"{host}:{port}",
                Messages = messageCount,
                MessageSizeBytes = messageSize,
                Produce = new
                {
                    result.MessagesProduced,
                    result.ProduceElapsedMs,
                    P50Ms = result.ProduceP50,
                    P95Ms = result.ProduceP95,
                    P99Ms = result.ProduceP99,
                    MinMs = result.ProduceMin,
                    MaxMs = result.ProduceMax,
                    AvgMs = result.ProduceAvg
                },
                Consume = result.MessagesConsumed > 0 ? new
                {
                    result.MessagesConsumed,
                    result.ConsumeElapsedMs,
                    P50Ms = result.ConsumeP50,
                    P95Ms = result.ConsumeP95,
                    P99Ms = result.ConsumeP99,
                    MinMs = result.ConsumeMin,
                    MaxMs = result.ConsumeMax,
                    AvgMs = result.ConsumeAvg
                } : null
            };
            Console.WriteLine(JsonSerializer.Serialize(output, JsonOptions));
        }
        else if (format == OutputFormat.Plain)
        {
            Console.WriteLine("mode: latency");
            Console.WriteLine($"produce: P50={result.ProduceP50:F2}ms P95={result.ProduceP95:F2}ms P99={result.ProduceP99:F2}ms (min={result.ProduceMin:F2}ms max={result.ProduceMax:F2}ms avg={result.ProduceAvg:F2}ms)");
            if (result.MessagesConsumed > 0)
            {
                Console.WriteLine($"consume: P50={result.ConsumeP50:F2}ms P95={result.ConsumeP95:F2}ms P99={result.ConsumeP99:F2}ms (min={result.ConsumeMin:F2}ms max={result.ConsumeMax:F2}ms avg={result.ConsumeAvg:F2}ms)");
            }
        }
        else
        {
            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Rule("[bold green]Latency Results[/]").LeftJustified());
            AnsiConsole.WriteLine();

            var table = new Table()
                .AddColumn("Operation")
                .AddColumn(new TableColumn("P50").RightAligned())
                .AddColumn(new TableColumn("P95").RightAligned())
                .AddColumn(new TableColumn("P99").RightAligned())
                .AddColumn(new TableColumn("Min").RightAligned())
                .AddColumn(new TableColumn("Max").RightAligned())
                .AddColumn(new TableColumn("Avg").RightAligned());

            table.AddRow(
                "[cyan]Produce[/]",
                $"{result.ProduceP50:F2} ms",
                $"{result.ProduceP95:F2} ms",
                $"[bold]{result.ProduceP99:F2} ms[/]",
                $"{result.ProduceMin:F2} ms",
                $"{result.ProduceMax:F2} ms",
                $"{result.ProduceAvg:F2} ms");

            if (result.MessagesConsumed > 0)
            {
                table.AddRow(
                    "[green]Consume[/]",
                    $"{result.ConsumeP50:F2} ms",
                    $"{result.ConsumeP95:F2} ms",
                    $"[bold]{result.ConsumeP99:F2} ms[/]",
                    $"{result.ConsumeMin:F2} ms",
                    $"{result.ConsumeMax:F2} ms",
                    $"{result.ConsumeAvg:F2} ms");
            }

            AnsiConsole.Write(table);

            // Also show throughput for reference
            AnsiConsole.WriteLine();
            var throughputMsgSec = result.ProduceElapsedMs > 0 ? result.MessagesProduced * 1000.0 / result.ProduceElapsedMs : result.MessagesProduced;
            AnsiConsole.MarkupLine($"[dim]Effective produce rate:[/] {throughputMsgSec:N0} msg/s (sequential)");
        }
    }

    private sealed class ProduceResult
    {
        public int MessagesProduced { get; init; }
        public long ElapsedMs { get; init; }
        public double ThroughputMsgSec { get; init; }
        public double ThroughputMBSec { get; init; }
    }

    private sealed class ConsumeResult
    {
        public int MessagesConsumed { get; init; }
        public long ElapsedMs { get; init; }
        public double ThroughputMsgSec { get; init; }
    }

    private sealed class LatencyResult
    {
        public int MessagesProduced { get; init; }
        public int MessagesConsumed { get; init; }
        public long ProduceElapsedMs { get; init; }
        public long ConsumeElapsedMs { get; init; }
        public double ProduceP50 { get; init; }
        public double ProduceP95 { get; init; }
        public double ProduceP99 { get; init; }
        public double ProduceMin { get; init; }
        public double ProduceMax { get; init; }
        public double ProduceAvg { get; init; }
        public double ConsumeP50 { get; init; }
        public double ConsumeP95 { get; init; }
        public double ConsumeP99 { get; init; }
        public double ConsumeMin { get; init; }
        public double ConsumeMax { get; init; }
        public double ConsumeAvg { get; init; }
    }
}
