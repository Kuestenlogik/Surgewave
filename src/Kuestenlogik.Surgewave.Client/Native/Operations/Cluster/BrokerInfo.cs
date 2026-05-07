namespace Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;

/// <summary>
/// Broker information.
/// </summary>
public record BrokerInfo
{
    public int BrokerId { get; init; }
    public string Host { get; init; } = string.Empty;
    public int Port { get; init; }
    public int ReplicationPort { get; init; }
    public bool IsController { get; init; }
    public bool IsAlive { get; init; }
    public string? Rack { get; init; }
}
