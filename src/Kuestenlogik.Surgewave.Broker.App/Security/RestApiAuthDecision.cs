namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>Outcome of a <see cref="RestApiAuthPolicy"/> evaluation.</summary>
public enum RestApiAuthDecision
{
    /// <summary>Request may proceed (anonymous path, or authenticated + authorized).</summary>
    Allow,

    /// <summary>Protected path with no authenticated identity → 401.</summary>
    Unauthenticated,

    /// <summary>Authenticated but lacking the required role for a mutating call → 403.</summary>
    Forbidden,
}
