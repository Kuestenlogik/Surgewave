using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Benchmarks.QuicVsTcp;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Quic;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing.Network;
using Kuestenlogik.Surgewave.Transport;
using Kuestenlogik.Surgewave.Transport.Quic;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

// -------------------------------------------------------------------------
// Surgewave-over-TCP vs Surgewave-over-QUIC benchmark with configurable UDP loss.
// -------------------------------------------------------------------------
// Usage:
//   surgewave-bench-transport --messages 50000 --size 512 --batch 100
//                         --loss 0,0.1,1,5 --latency 0,5
//                         --protocols tcp,quic
//
// What this measures:
//   * Throughput (msgs/sec, MB/sec) for each (transport, loss, latency) cell
//   * Real UDP-layer packet drops against the QUIC path
//   * Simulated one-way latency on both TCP and QUIC
//
// TCP note: application-layer byte drops do not model TCP packet loss
// correctly (the kernel already ACK'd the bytes). TCP rows therefore run
// with loss=0 regardless of --loss. Latency simulation is honoured for both.
// -------------------------------------------------------------------------

var cli = ParseCli(args);
Console.WriteLine($"Surgewave Transport Benchmark — {cli.Messages} messages, {cli.Size} bytes, batch={cli.Batch}");
Console.WriteLine($"Protocols: [{string.Join(", ", cli.Protocols)}]");
Console.WriteLine($"Loss rates: [{string.Join(", ", cli.LossRates.Select(r => (r * 100).ToString("0.##", CultureInfo.InvariantCulture) + '%'))}]");
Console.WriteLine($"Latencies: [{string.Join(", ", cli.LatenciesMs.Select(l => l + "ms"))}]");
Console.WriteLine();

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.SetMinimumLevel(LogLevel.Warning);
    builder.AddConsole();
});

Console.WriteLine("Starting embedded Surgewave broker...");

await using var surgewave = await SurgewaveRuntime.CreateBuilder()
    .WithHost("127.0.0.1")
    .WithPort(0)
    .WithStorageEngine(StorageEngines.Memory)
    .WithAutoCreateTopics(true)
    .WithLogging(loggerFactory)
    .Build()
    .StartAsync();

var tcpBrokerPort = surgewave.Port;
Console.WriteLine($"Broker TCP native listener on 127.0.0.1:{tcpBrokerPort}");

SurgewaveStreamHandlerHolder.Instance = surgewave.Broker;

// Pick a free UDP port for the QUIC listener.
int quicBrokerPort = PickFreeUdpPort();
Console.WriteLine($"Broker QUIC listener target: 127.0.0.1:{quicBrokerPort}");

List<BenchmarkResult> results;

if (cli.Protocols.Contains(SurgewaveTransportType.Quic) &&
    (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS()))
{
    // Client-side: accept the broker's self-signed dev cert.
    QuicTransport.TrustAllCertificates = true;

    var quicConfig = new QuicConfig
    {
        Enabled = true,
        Port = quicBrokerPort,
        MaxConnections = 100,
        MaxStreamsPerConnection = 256,
        IdleTimeoutSeconds = 120
    };
    var quicAdapter = new QuicBrokerAdapter(
        Options.Create(quicConfig),
        loggerFactory.CreateLogger<QuicBrokerAdapter>());
    try
    {
        _ = quicAdapter.StartAsync(CancellationToken.None);
        Console.WriteLine("Waiting for QUIC listener to come up...");
        await Task.Delay(TimeSpan.FromMilliseconds(750));

        results = await RunAllScenariosAsync(cli, tcpBrokerPort, quicBrokerPort);

        await quicAdapter.StopAsync(CancellationToken.None);
    }
    finally
    {
        quicAdapter.Dispose();
    }
}
else
{
    if (cli.Protocols.Contains(SurgewaveTransportType.Quic))
    {
        Console.Error.WriteLine("QUIC benchmark requires Windows, Linux or macOS with msquic. Skipping QUIC rows.");
        cli = cli with { Protocols = cli.Protocols.Where(p => p != SurgewaveTransportType.Quic).ToArray() };
    }
    results = await RunAllScenariosAsync(cli, tcpBrokerPort, quicBrokerPort);
}

PrintTable(results);
return 0;

// -------------------------------------------------------------------------

static async Task<List<BenchmarkResult>> RunAllScenariosAsync(
    CliOptions cli,
    int tcpBrokerPort,
    int quicBrokerPort)
{
    var list = new List<BenchmarkResult>();
    foreach (var scenario in BuildScenarios(cli))
    {
        Console.WriteLine();
        Console.WriteLine($"▶ {scenario.Name}");
        var result = await RunScenarioAsync(scenario, cli, tcpBrokerPort, quicBrokerPort);
        list.Add(result);

        if (result.Error is null)
        {
            Console.WriteLine(
                $"  → {result.MessagesPerSecond,12:N0} msg/s   "
                + $"{result.MegabytesPerSecond,8:N1} MB/s   "
                + $"elapsed={result.Elapsed.TotalMilliseconds,8:N0} ms   "
                + $"drops={result.ProxyDatagramsDropped}/{result.ProxyDatagramsForwarded + result.ProxyDatagramsDropped}");
        }
        else
        {
            Console.WriteLine($"  ✗ ERROR: {result.Error}");
        }
    }
    return list;
}

// -------------------------------------------------------------------------
// Implementation
// -------------------------------------------------------------------------

static int PickFreeUdpPort()
{
    using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
    probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
    var port = ((IPEndPoint)probe.LocalEndPoint!).Port;
    return port;
}

static int PickFreeTcpPort()
{
    using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
    probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
    probe.Listen(1);
    return ((IPEndPoint)probe.LocalEndPoint!).Port;
}

static List<BenchmarkScenario> BuildScenarios(CliOptions cli)
{
    var list = new List<BenchmarkScenario>();
    foreach (var proto in cli.Protocols)
    {
        foreach (var lossRate in cli.LossRates)
        {
            // TCP rows skip >0 loss because app-layer TCP loss simulation is not faithful.
            if (proto == SurgewaveTransportType.Tcp && lossRate > 0) continue;

            foreach (var latencyMs in cli.LatenciesMs)
            {
                var lossLabel = lossRate == 0 ? "0%" : $"{lossRate * 100:0.##}%";
                var name = $"{proto,-4} loss={lossLabel,-6} latency={latencyMs}ms";
                list.Add(new BenchmarkScenario(name, proto, lossRate, latencyMs));
            }
        }
    }
    return list;
}

static async Task<BenchmarkResult> RunScenarioAsync(
    BenchmarkScenario scenario,
    CliOptions cli,
    int tcpBrokerPort,
    int quicBrokerPort)
{
    try
    {
        // Direct path: when there is no loss and no added latency we talk
        // straight to the broker port. This removes the in-process proxy
        // overhead, which is most visible in the TCP byte-pump loop, and
        // produces an honest apples-to-apples baseline for both transports.
        var direct = scenario.DropRate == 0 && scenario.LatencyMs == 0;

        if (scenario.Transport == SurgewaveTransportType.Quic)
        {
            if (direct)
            {
                var elapsed = await DriveLoadAsync(scenario, cli, quicBrokerPort);
                return new BenchmarkResult(scenario, cli.Messages, cli.Size, elapsed, 0, 0, null);
            }

            var proxyPort = PickFreeUdpPort();
            await using var udpProxy = new LossyUdpProxy(
                proxyPort, quicBrokerPort, scenario.DropRate, scenario.LatencyMs);
            _ = udpProxy.Start();
            var elapsedProxied = await DriveLoadAsync(scenario, cli, proxyPort);
            return new BenchmarkResult(
                scenario, cli.Messages, cli.Size, elapsedProxied,
                udpProxy.TotalDropped, udpProxy.TotalForwarded, null);
        }
        else
        {
            if (direct)
            {
                var elapsed = await DriveLoadAsync(scenario, cli, tcpBrokerPort);
                return new BenchmarkResult(scenario, cli.Messages, cli.Size, elapsed, 0, 0, null);
            }

            var proxyPort = PickFreeTcpPort();
            await using var tcpProxy = new LossyTcpProxy(
                proxyPort, tcpBrokerPort, scenario.LatencyMs);
            await tcpProxy.Start();
            var elapsedProxied = await DriveLoadAsync(scenario, cli, proxyPort);
            return new BenchmarkResult(scenario, cli.Messages, cli.Size, elapsedProxied, 0, 0, null);
        }
    }
    catch (Exception ex)
    {
        return new BenchmarkResult(scenario, 0, cli.Size, TimeSpan.Zero, 0, 0, ex.Message);
    }
}

static async Task<TimeSpan> DriveLoadAsync(BenchmarkScenario scenario, CliOptions cli, int clientPort)
{
    var payload = new byte[cli.Size];
    new Random(42).NextBytes(payload);
    var batch = new List<(byte[]? Key, byte[] Value)>(cli.Batch);
    for (int i = 0; i < cli.Batch; i++) batch.Add((null, payload));

    await using var client = new SurgewaveNativeClient(
        "127.0.0.1", clientPort, scenario.Transport, enablePipelining: true);
    await client.ConnectAsync();

    var topic = $"bench-{Guid.NewGuid():N}";
    await client.Topics.CreateAsync(topic, partitions: 1);

    // Warm-up: a handful of batches before starting the clock.
    for (int i = 0; i < 3; i++)
    {
        await client.Messaging.SendBatchAsync(topic, 0, batch);
    }

    var sw = Stopwatch.StartNew();
    var remaining = cli.Messages;
    while (remaining > 0)
    {
        var toSend = Math.Min(cli.Batch, remaining);
        var slice = toSend == cli.Batch ? batch : batch.Take(toSend).ToList();
        await client.Messaging.SendBatchAsync(topic, 0, slice);
        remaining -= toSend;
    }
    sw.Stop();
    return sw.Elapsed;
}

static void PrintTable(List<BenchmarkResult> results)
{
    Console.WriteLine();
    Console.WriteLine("=========================================================================================");
    Console.WriteLine("  Scenario                              msg/s         MB/s      elapsed     drops");
    Console.WriteLine("=========================================================================================");
    foreach (var r in results)
    {
        if (r.Error is not null)
        {
            Console.WriteLine($"  {r.Scenario.Name,-38} ERROR: {r.Error}");
            continue;
        }
        Console.WriteLine(
            $"  {r.Scenario.Name,-38} {r.MessagesPerSecond,10:N0}  {r.MegabytesPerSecond,8:N1}  "
            + $"{r.Elapsed.TotalMilliseconds,8:N0} ms  "
            + $"{r.ProxyDatagramsDropped,5}/{r.ProxyDatagramsForwarded + r.ProxyDatagramsDropped,-5}");
    }
    Console.WriteLine("=========================================================================================");
}

static CliOptions ParseCli(string[] args)
{
    var messages = 20_000;
    var size = 512;
    var batch = 100;
    var lossRates = new[] { 0.0, 0.001, 0.01, 0.05 };
    var latencies = new[] { 0, 5 };
    var protocols = new[] { SurgewaveTransportType.Tcp, SurgewaveTransportType.Quic };

    for (int i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--messages": messages = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--size": size = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--batch": batch = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--loss":
                lossRates = args[++i].Split(',')
                    .Select(s => double.Parse(s, CultureInfo.InvariantCulture) / 100.0)
                    .ToArray();
                break;
            case "--latency":
                latencies = args[++i].Split(',').Select(s => int.Parse(s, CultureInfo.InvariantCulture)).ToArray();
                break;
            case "--protocols":
                protocols = args[++i].Split(',').Select(s => s.Trim().ToLowerInvariant() switch
                {
                    "tcp" => SurgewaveTransportType.Tcp,
                    "quic" => SurgewaveTransportType.Quic,
                    _ => throw new ArgumentException($"Unknown protocol: {s}")
                }).ToArray();
                break;
        }
    }
    return new CliOptions(messages, size, batch, lossRates, latencies, protocols);
}

internal sealed record CliOptions(
    int Messages,
    int Size,
    int Batch,
    double[] LossRates,
    int[] LatenciesMs,
    SurgewaveTransportType[] Protocols);
