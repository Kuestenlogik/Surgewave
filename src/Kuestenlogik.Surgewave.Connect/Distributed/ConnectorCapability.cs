namespace Kuestenlogik.Surgewave.Connect.Distributed;

/// <summary>
/// Describes a connector type that a worker can instantiate.
/// Sent as part of the worker heartbeat to advertise available capabilities.
/// </summary>
public sealed record ConnectorCapability(
    string ClassName,
    string Type,
    string DisplayName,
    string Version);
