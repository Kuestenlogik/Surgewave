namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Represents a delegation token.
/// </summary>
public sealed class DelegationToken
{
    public required string TokenId { get; init; }
    public required byte[] Hmac { get; init; }
    public required string OwnerPrincipalType { get; init; }
    public required string OwnerPrincipalName { get; init; }
    public required string RequesterPrincipalType { get; init; }
    public required string RequesterPrincipalName { get; init; }
    public required long IssueTimestampMs { get; init; }
    public long ExpiryTimestampMs { get; set; }
    public required long MaxTimestampMs { get; init; }
    public required List<TokenRenewer> Renewers { get; init; }
}
