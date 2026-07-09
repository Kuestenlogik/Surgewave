namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Session state for SCRAM authentication handshake
/// </summary>
public sealed class ScramSession
{
    public string? Username { get; set; }
    public string? ClientFirstBare { get; set; }
    public string? ServerFirst { get; set; }
    public string? CombinedNonce { get; set; }
    public byte[] StoredKey { get; set; } = [];
    public byte[] ServerKey { get; set; } = [];
    public ScramState State { get; set; } = ScramState.Initial;
    public bool IsFakeUser { get; set; }
}

/// <summary>
/// SCRAM authentication state machine states
/// </summary>
public enum ScramState
{
    Initial,
    ServerFirstSent,
    Completed
}
