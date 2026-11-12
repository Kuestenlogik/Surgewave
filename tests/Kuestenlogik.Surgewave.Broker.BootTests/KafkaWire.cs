using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// Kafka requests written by hand and sent to one broker's own port. A Kafka client is told the cluster's brokers by
/// the first one it reaches and then asks whichever it likes — the leader, the controller, any — so what a given
/// broker itself answers can only be asked this way.
/// </summary>
internal static class KafkaWire
{
    private const string ClientId = "boot-test";
    private const sbyte TopicResource = 2;

    /// <summary>One Metadata v1 request for <paramref name="topic"/> to <paramref name="port"/>.</summary>
    public static async Task<TopicMetadata> MetadataAsync(int port, string topic, CancellationToken ct)
    {
        var body = new List<byte>();
        Int16(body, 3); // api key: Metadata
        Int16(body, 1); // version
        Int32(body, 1); // correlation id
        String(body, ClientId);
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
        var controllerId = ReadInt32(response, ref at);
        short error = 0;
        var leaders = new Dictionary<int, int>();
        var topics = ReadInt32(response, ref at);
        for (var t = 0; t < topics; t++)
        {
            error = ReadInt16(response, ref at);
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
        return new TopicMetadata(controllerId, error, leaders);
    }

    /// <summary>
    /// One DescribeConfigs v1 request for <paramref name="config"/> of <paramref name="topic"/> to
    /// <paramref name="port"/>: the value, or the broker's error in words.
    /// </summary>
    public static async Task<string> TopicConfigAsync(int port, string topic, string config, CancellationToken ct)
    {
        var body = new List<byte>();
        Int16(body, 32); // api key: DescribeConfigs
        Int16(body, 1); // version
        Int32(body, 1); // correlation id
        String(body, ClientId);
        Int32(body, 1); // resources
        body.Add((byte)TopicResource);
        String(body, topic);
        Int32(body, 1); // config names
        String(body, config);
        body.Add(0); // include synonyms
        var response = await ExchangeAsync(port, body, ct);

        var at = 4 + 4; // correlation id, throttle time
        Assert.Equal(1, ReadInt32(response, ref at));
        var error = ReadInt16(response, ref at);
        var message = ReadString(response, ref at);
        if (error != 0)
            return $"error {error}: {message}";
        at += 1; // resource type
        SkipString(response, ref at); // resource name
        var configs = ReadInt32(response, ref at);
        for (var c = 0; c < configs; c++)
        {
            var name = ReadString(response, ref at);
            var value = ReadString(response, ref at);
            at += 3; // read only, source, sensitive
            var synonyms = ReadInt32(response, ref at);
            for (var s = 0; s < synonyms; s++)
            {
                SkipString(response, ref at); // name
                SkipString(response, ref at); // value
                at += 1; // source
            }
            if (name == config)
                return value ?? "(null)";
        }
        return $"(no {config} among the {configs} configs described)";
    }

    /// <summary>Sends one request to <paramref name="port"/> on a connection of its own and reads the response.</summary>
    public static async Task<byte[]> ExchangeAsync(int port, List<byte> body, CancellationToken ct)
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

    public static short ReadInt16(byte[] from, ref int at)
    {
        var value = BinaryPrimitives.ReadInt16BigEndian(from.AsSpan(at));
        at += 2;
        return value;
    }

    public static int ReadInt32(byte[] from, ref int at)
    {
        var value = BinaryPrimitives.ReadInt32BigEndian(from.AsSpan(at));
        at += 4;
        return value;
    }

    public static string? ReadString(byte[] from, ref int at)
    {
        var length = ReadInt16(from, ref at);
        if (length < 0)
            return null;
        var value = Encoding.UTF8.GetString(from, at, length);
        at += length;
        return value;
    }

    public static void SkipString(byte[] from, ref int at) => ReadString(from, ref at);

    public static void Int16(List<byte> to, short value) { var b = new byte[2]; BinaryPrimitives.WriteInt16BigEndian(b, value); to.AddRange(b); }

    public static void Int32(List<byte> to, int value) { var b = new byte[4]; BinaryPrimitives.WriteInt32BigEndian(b, value); to.AddRange(b); }

    public static void Int64(List<byte> to, long value) { var b = new byte[8]; BinaryPrimitives.WriteInt64BigEndian(b, value); to.AddRange(b); }

    public static void String(List<byte> to, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        Int16(to, (short)bytes.Length);
        to.AddRange(bytes);
    }
}

/// <summary>
/// What one broker says about a topic: the controller it names, the topic's error code, each partition's leader.
/// </summary>
internal sealed record TopicMetadata(int ControllerId, short Error, IReadOnlyDictionary<int, int> Leaders);
