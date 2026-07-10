namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Represents a token owner for filtering.
/// </summary>
public sealed class TokenOwner
{
    public required string PrincipalType { get; init; }
    public required string PrincipalName { get; init; }
}
