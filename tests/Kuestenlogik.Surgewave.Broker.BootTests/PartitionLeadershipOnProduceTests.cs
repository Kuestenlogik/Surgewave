using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// The shipped broker refuses a produce for a partition another broker leads (#227): NOT_LEADER_OR_FOLLOWER, as Kafka
/// answers, so a client with stale metadata refreshes it. The embedded runtime had this check since #164; the shipped
/// broker never wired it and appended the records to a follower's copy, where nobody would read them.
/// </summary>
/// <remarks>
/// Two brokers of the shipped binary in one cluster. The produce is written by hand, because a Kafka client sends it
/// to the leader it knows from the metadata.
/// </remarks>
[Trait("Category", "Integration")]
[Collection("BrokerBoot")]
public sealed class PartitionLeadershipOnProduceTests
{
    private const short NotLeaderOrFollower = 6;

    [Fact(Timeout = 240_000)]
    public async Task AProduceForAPartitionAnotherBrokerLeadsIsRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        var dirs = new[] { NewDataDir(), NewDataDir() };
        var ports = new[] { BrokerPorts.Reserve(), BrokerPorts.Reserve() };
        var nodes = string.Join(",", Enumerable.Range(0, 2).Select(i => $"{i + 1}:127.0.0.1:{ports[i].Kafka}:{ports[i].Replication}"));
        var voters = string.Join(",", Enumerable.Range(0, 2).Select(i => $"{i + 1}@127.0.0.1:{ports[i].Replication}"));
        var topic = $"orders-{Guid.NewGuid():N}";

        try
        {
            var starting = Enumerable.Range(0, 2).Select(i => BrokerProcess.StartAsync(dirs[i], ports[i], ct,
                $"--Surgewave:BrokerId={i + 1}",
                $"--Surgewave:ClusterNodes={nodes}",
                $"--Surgewave:ControllerQuorumVoters={voters}",
                "--Surgewave:Connect:Enabled=false")).ToArray();
            await using var first = await starting[0];
            await using var second = await starting[1];

            // Two partitions, one replica each: the assignment puts one on each broker.
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = first.BootstrapServers }).Build();
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 2, ReplicationFactor = 1 }]);
            var ledBySecond = await PartitionLedByAsync(first.KafkaPort, topic, leader: 2, ct);

            var error = await ProduceAsync(first.KafkaPort, topic, ledBySecond, ct);

            Assert.True(error == NotLeaderOrFollower,
                $"broker 1 answered {error} for {topic}-{ledBySecond}, which broker 2 leads\n{first.Dump()}");
        }
        finally
        {
            foreach (var dir in dirs)
                BrokerProcess.TryDelete(dir);
        }
    }

    private static string NewDataDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    // The partition the given broker leads, once broker 1 itself names leaders for the topic: a broker that does not
    // know the partition yet has no opinion on who leads it (#164), so asking any other broker would race it.
    private static async Task<int> PartitionLedByAsync(int port, string topic, int leader, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(60));
        while (true)
        {
            var leaders = await LeadersAsync(port, topic, timeout.Token);
            if (leaders.Count == 2 && leaders.Values.All(l => l > 0))
                return Assert.Single(leaders, kv => kv.Value == leader).Key;
            // The metadata log reaches the broker in its own time, and nothing tells a client when; the next request
            // asks again.
            await Task.Delay(TimeSpan.FromMilliseconds(200), timeout.Token);
        }
    }

    /// <summary>One Metadata v1 request for the topic to <paramref name="port"/>; each partition's leader.</summary>
    private static async Task<Dictionary<int, int>> LeadersAsync(int port, string topic, CancellationToken ct)
    {
        var body = new List<byte>();
        Int16(body, 3); // api key: Metadata
        Int16(body, 1); // version
        Int32(body, 1); // correlation id
        String(body, "boot-test");
        Int32(body, 1); // topics
        String(body, topic);
        var response = await ExchangeAsync(port, body, ct);

        var at = 4; // correlation id
        var brokers = ReadInt32(response, ref at);
        for (var b = 0; b < brokers; b++)
        {
            at += 4; // node id
            SkipString(response, ref at); // host
            at += 4; // port
            SkipString(response, ref at); // rack
        }
        at += 4; // controller id
        var leaders = new Dictionary<int, int>();
        var topics = ReadInt32(response, ref at);
        for (var t = 0; t < topics; t++)
        {
            at += 2; // error
            SkipString(response, ref at); // name
            at += 1; // is internal
            var partitions = ReadInt32(response, ref at);
            for (var p = 0; p < partitions; p++)
            {
                at += 2; // error
                var index = ReadInt32(response, ref at);
                leaders[index] = ReadInt32(response, ref at);
                var replicas = ReadInt32(response, ref at);
                at += 4 * replicas;
                var isr = ReadInt32(response, ref at);
                at += 4 * isr;
            }
        }
        return leaders;
    }

    private static async Task<byte[]> ExchangeAsync(int port, List<byte> body, CancellationToken ct)
    {
        using var client = new TcpClient();
        await client.ConnectAsync("127.0.0.1", port, ct);
        var stream = client.GetStream();
        var request = new List<byte>();
        Int32(request, body.Count);
        request.AddRange(body);
        await stream.WriteAsync(request.ToArray(), ct);

        var size = new byte[4];
        await stream.ReadExactlyAsync(size, ct);
        var response = new byte[BinaryPrimitives.ReadInt32BigEndian(size)];
        await stream.ReadExactlyAsync(response, ct);
        return response;
    }

    private static int ReadInt32(byte[] from, ref int at)
    {
        var value = BinaryPrimitives.ReadInt32BigEndian(from.AsSpan(at));
        at += 4;
        return value;
    }

    private static void SkipString(byte[] from, ref int at)
    {
        var length = BinaryPrimitives.ReadInt16BigEndian(from.AsSpan(at));
        at += 2 + Math.Max(0, (int)length);
    }

    /// <summary>One Produce v3 request with one record batch, sent to <paramref name="port"/>; its partition's error.</summary>
    private static async Task<short> ProduceAsync(int port, string topic, int partition, CancellationToken ct)
    {
        var body = new List<byte>();
        Int16(body, 0); // api key: Produce
        Int16(body, 3); // version
        Int32(body, 1); // correlation id
        String(body, "boot-test");
        Int16(body, -1); // transactional id: null
        Int16(body, 1); // acks
        Int32(body, 30_000); // timeout
        Int32(body, 1); // topics
        String(body, topic);
        Int32(body, 1); // partitions
        Int32(body, partition);
        var batch = RecordBatch();
        Int32(body, batch.Length);
        body.AddRange(batch);
        var response = await ExchangeAsync(port, body, ct);

        // correlation id, topics count, topic name, partitions count, partition index, error code
        var at = 4 + 4;
        at += 2 + BinaryPrimitives.ReadInt16BigEndian(response.AsSpan(at));
        at += 4 + 4;
        return BinaryPrimitives.ReadInt16BigEndian(response.AsSpan(at));
    }

    // A record batch v2 with one record. The checksum is left zero: the leadership check comes before the append.
    private static byte[] RecordBatch()
    {
        var record = new List<byte> { 0, 0, 0, 1 }; // attributes, timestamp delta, offset delta, key length -1 (zigzag 1)
        var value = Encoding.UTF8.GetBytes("stale");
        record.Add((byte)(value.Length * 2)); // value length, zigzag
        record.AddRange(value);
        record.Add(0); // headers
        var records = new List<byte> { (byte)(record.Count * 2) }; // record length, zigzag
        records.AddRange(record);

        var afterLength = new List<byte>();
        Int32(afterLength, 0); // partition leader epoch
        afterLength.Add(2); // magic
        Int32(afterLength, 0); // crc
        Int16(afterLength, 0); // attributes
        Int32(afterLength, 0); // last offset delta
        Int64(afterLength, 0); // first timestamp
        Int64(afterLength, 0); // max timestamp
        Int64(afterLength, -1); // producer id
        Int16(afterLength, -1); // producer epoch
        Int32(afterLength, -1); // base sequence
        Int32(afterLength, 1); // records
        afterLength.AddRange(records);

        var batch = new List<byte>();
        Int64(batch, 0); // base offset
        Int32(batch, afterLength.Count);
        batch.AddRange(afterLength);
        return [.. batch];
    }

    private static void Int16(List<byte> to, short value) { var b = new byte[2]; BinaryPrimitives.WriteInt16BigEndian(b, value); to.AddRange(b); }

    private static void Int32(List<byte> to, int value) { var b = new byte[4]; BinaryPrimitives.WriteInt32BigEndian(b, value); to.AddRange(b); }

    private static void Int64(List<byte> to, long value) { var b = new byte[8]; BinaryPrimitives.WriteInt64BigEndian(b, value); to.AddRange(b); }

    private static void String(List<byte> to, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        Int16(to, (short)bytes.Length);
        to.AddRange(bytes);
    }
}
