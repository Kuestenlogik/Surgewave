namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Represents a principal that can renew a delegation token.
/// </summary>
public sealed class TokenRenewer
{
    public required string PrincipalType { get; init; }
    public required string PrincipalName { get; init; }
}
