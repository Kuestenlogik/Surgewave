namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Represents a broker endpoint address (host and port)
/// </summary>
public readonly record struct BrokerAddress(string Host, int Port)
{
    /// <summary>
    /// Default Kafka port
    /// </summary>
    public const int DefaultPort = KafkaConstants.Ports.Kafka;

    /// <summary>
    /// Parse a single broker address from "host:port" format
    /// </summary>
    public static BrokerAddress Parse(string address)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(address);

        var colonIndex = address.LastIndexOf(':');
        if (colonIndex > 0 && int.TryParse(address.AsSpan(colonIndex + 1), out var port))
        {
            return new BrokerAddress(address[..colonIndex], port);
        }

        return new BrokerAddress(address, DefaultPort);
    }

    /// <summary>
    /// Parse bootstrap servers string (comma-separated) and return the first broker
    /// </summary>
    public static BrokerAddress ParseFirst(string bootstrapServers)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bootstrapServers);

        var firstComma = bootstrapServers.IndexOf(',');
        var firstServer = firstComma >= 0
            ? bootstrapServers[..firstComma]
            : bootstrapServers;

        return Parse(firstServer.Trim());
    }

    /// <summary>
    /// Parse all bootstrap servers from comma-separated string
    /// </summary>
    public static IReadOnlyList<BrokerAddress> ParseAll(string bootstrapServers)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bootstrapServers);

        var addresses = new List<BrokerAddress>();
        foreach (var server in bootstrapServers.Split(','))
        {
            var trimmed = server.Trim();
            if (trimmed.Length > 0)
            {
                addresses.Add(Parse(trimmed));
            }
        }

        return addresses;
    }

    public override string ToString() => $"{Host}:{Port}";
}
