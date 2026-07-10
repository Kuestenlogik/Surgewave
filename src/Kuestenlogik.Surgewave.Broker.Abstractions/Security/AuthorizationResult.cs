namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Result of an authorization check
/// </summary>
public readonly struct AuthorizationResult : IEquatable<AuthorizationResult>
{
    public bool IsAllowed { get; }
    public string Reason { get; }

    private AuthorizationResult(bool isAllowed, string reason)
    {
        IsAllowed = isAllowed;
        Reason = reason;
    }

    public static AuthorizationResult Allowed(string reason) => new(true, reason);
    public static AuthorizationResult Denied(string reason) => new(false, reason);

    public bool Equals(AuthorizationResult other) =>
        IsAllowed == other.IsAllowed && Reason == other.Reason;

    public override bool Equals(object? obj) =>
        obj is AuthorizationResult other && Equals(other);

    public override int GetHashCode() =>
        HashCode.Combine(IsAllowed, Reason);

    public static bool operator ==(AuthorizationResult left, AuthorizationResult right) =>
        left.Equals(right);

    public static bool operator !=(AuthorizationResult left, AuthorizationResult right) =>
        !left.Equals(right);
}
