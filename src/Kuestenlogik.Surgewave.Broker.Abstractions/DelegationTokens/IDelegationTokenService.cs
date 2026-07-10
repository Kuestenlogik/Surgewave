namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Protocol-neutral seam over the delegation token manager (HMAC-signed bearer tokens).
/// Implemented by the broker's <c>DelegationTokenManager</c>.
/// </summary>
public interface IDelegationTokenService
{
    /// <summary>
    /// Configuration for delegation tokens.
    /// </summary>
    DelegationTokenConfig Config { get; }

    /// <summary>
    /// Create a new delegation token.
    /// </summary>
    DelegationToken CreateToken(
        string ownerPrincipalType,
        string ownerPrincipalName,
        string? requesterPrincipalType,
        string? requesterPrincipalName,
        List<TokenRenewer>? renewers,
        long maxLifetimeMs);

    /// <summary>
    /// Renew a delegation token by its HMAC.
    /// </summary>
    (DelegationToken? Token, string? Error) RenewToken(byte[] hmac, long renewPeriodMs);

    /// <summary>
    /// Expire a delegation token by its HMAC.
    /// </summary>
    (long ExpiryTimestamp, string? Error) ExpireToken(byte[] hmac, long expiryTimePeriodMs);

    /// <summary>
    /// Describe delegation tokens, optionally filtered by owner.
    /// </summary>
    List<DelegationToken> DescribeTokens(List<TokenOwner>? owners);
}
