using System.Diagnostics;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Transport.Tcp;

namespace Kuestenlogik.Surgewave.Benchmarks.Integration;

public static class ReplicationThroughputBenchmark
{
    public static async Task RunAsync(string[] args)
    {
        TcpTransportRegistration.Register();

        var messageCount = args.Length > 0 ? int.Parse(args[0]) : 200_000;
        var messageSizeBytes = args.Length > 1 ? int.Parse(args[1]) : 100;
        var batchSize = args.Length > 2 ? int.Parse(args[2]) : 1000;
        var topicName = "replication-benchmark";

        Console.WriteLine("Replication Throughput Benchmark (2-Broker Cluster)");
        Console.WriteLine("===================================================");
        Console.WriteLine($"Messages:     {messageCount:N0}");
        Console.WriteLine($"Message size: {messageSizeBytes} bytes");
        Console.WriteLine($"Batch size:   {batchSize}");
        Console.WriteLine($"Total data:   {(long)messageCount * messageSizeBytes / 1024 / 1024:N1} MB");
        Console.WriteLine();

        Console.WriteLine("Starting 2-broker cluster with Raft...");
        var startSw = Stopwatch.StartNew();

        await using var broker1 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(0)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithRaft(true)
            .WithRaftElectionTimeout(500, 1500)
            .WithRaftHeartbeatInterval(100)
            .WithRaftPeerDiscoveryTimeout(15)
            .WithReplicationFactor(2)
            .WithMemoryStorage()
            .WithIPv4Only()
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .WithCleanup()
            .Build()
            .StartAsync();

        await using var broker2 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(1)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithRaft(true)
            .WithRaftElectionTimeout(500, 1500)
            .WithRaftHeartbeatInterval(100)
            .WithRaftPeerDiscoveryTimeout(15)
            .WithReplicationFactor(2)
            .WithMemoryStorage()
            .WithIPv4Only()
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .WithCleanup()
            .WithCluster($"0:localhost:{broker1.Port}")
            .Build()
            .StartAsync();

        startSw.Stop();
        Console.WriteLine($"Cluster started in {startSw.ElapsedMilliseconds} ms");
        Console.WriteLine($"Broker 1: port {broker1.Port}");
        Console.WriteLine($"Broker 2: port {broker2.Port}");
        Console.WriteLine();

        var messageValue = new byte[messageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        await using var client = new SurgewaveNativeClient("localhost", broker1.Port);
        await client.ConnectAsync();

        await client.Topics.CreateAsync(topicName, 1, replicationFactor: 2);
        await Task.Delay(1000);

        Console.WriteLine("Waiting for Raft leader election...");
        await Task.Delay(3000);

        Console.WriteLine("=== REPLICATED PRODUCER BENCHMARK (RF=2) ===");
        await using var producer = new SurgewaveBatchingProducer(
            client, topicName, partition: 0,
            maxBatchSize: batchSize,
            lingerTime: TimeSpan.FromMilliseconds(5));

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(null, messageValue);
            if (i > 0 && i % 50_000 == 0)
                Console.WriteLine($"  Produced {i:N0} messages...");
        }
        await producer.FlushAsync();
        sw.Stop();

        var produceMsgPerSec = messageCount * 1000.0 / sw.ElapsedMilliseconds;
        var produceMBPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / sw.ElapsedMilliseconds;

        Console.WriteLine($"  Time:       {sw.ElapsedMilliseconds:N0} ms");
        Console.WriteLine($"  Throughput: {produceMsgPerSec:N0} msg/sec");
        Console.WriteLine($"  Throughput: {produceMBPerSec:N1} MB/sec");
        Console.WriteLine();

        Console.WriteLine("=== REPLICATED CONSUMER BENCHMARK (RF=2) ===");
        sw.Restart();
        var consumed = 0;
        long offset = 0;

        while (consumed < messageCount)
        {
            var result = await client.Messaging.ReceiveAsync(topicName, 0, offset, maxBytes: 1024 * 1024);
            if (result.Messages.Count == 0)
            {
                await Task.Delay(10);
                continue;
            }
            consumed += result.Messages.Count;
            offset = result.Messages[^1].Offset + 1;
            if (consumed > 0 && consumed % 50_000 == 0)
                Console.WriteLine($"  Consumed {consumed:N0} messages...");
        }
        sw.Stop();

        var consumeMsgPerSec = consumed * 1000.0 / sw.ElapsedMilliseconds;
        var consumeMBPerSec = (long)consumed * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / sw.ElapsedMilliseconds;

        Console.WriteLine($"  Time:       {sw.ElapsedMilliseconds:N0} ms");
        Console.WriteLine($"  Throughput: {consumeMsgPerSec:N0} msg/sec");
        Console.WriteLine($"  Throughput: {consumeMBPerSec:N1} MB/sec");
        Console.WriteLine();

        Console.WriteLine("=== SUMMARY (Replicated, RF=2, TCP inter-broker) ===");
        Console.WriteLine($"Cluster startup: {startSw.ElapsedMilliseconds} ms");
        Console.WriteLine($"Producer: {produceMsgPerSec:N0} msg/sec ({produceMBPerSec:N1} MB/sec)");
        Console.WriteLine($"Consumer: {consumeMsgPerSec:N0} msg/sec ({consumeMBPerSec:N1} MB/sec)");
    }
}
