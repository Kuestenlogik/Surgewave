namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Neutral description of a broker listener endpoint advertised during registration (#59 b5).
/// </summary>
/// <param name="Name">Listener name (e.g. "PLAINTEXT", "REPLICATION").</param>
/// <param name="Host">Advertised host.</param>
/// <param name="Port">Advertised port.</param>
/// <param name="SecurityProtocol">Security protocol (0=PLAINTEXT, 1=SSL, 2=SASL_PLAINTEXT, 3=SASL_SSL).</param>
public sealed record ListenerSpec(string Name, string Host, int Port, short SecurityProtocol);
