namespace Kuestenlogik.Surgewave.Gateway;

/// <summary>
/// Configuration for a single Surgewave cluster connection.
/// </summary>
public sealed class ClusterConfig
{
    /// <summary>
    /// The Surgewave broker host address.
    /// </summary>
    public string BrokerHost { get; set; } = "localhost";

    /// <summary>
    /// The Surgewave broker port.
    /// </summary>
    public int BrokerPort { get; set; } = 9092;

    /// <summary>
    /// Enable request pipelining for better performance.
    /// </summary>
    public bool EnablePipelining { get; set; } = true;
}
