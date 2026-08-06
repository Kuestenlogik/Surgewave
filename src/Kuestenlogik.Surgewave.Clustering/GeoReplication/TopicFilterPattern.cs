using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Clustering.GeoReplication;

/// <summary>
/// Compiles a cluster-link <c>topicFilter</c> into a <see cref="Regex"/>.
/// </summary>
/// <remarks>
/// <para>
/// Both the REST validation path (<c>ClusterLinkRestApi</c>) and the
/// topic-discovery sink (<see cref="ClusterLinkManager"/>) must go through
/// here. If the two sides construct their regexes with different options, a
/// pattern the API accepts can still fail at the sink — where the failure is
/// swallowed by a broad <c>catch</c> and silently disables replication.
/// </para>
/// <para>
/// The pattern is operator-supplied and is matched against topic names taken
/// from the <em>remote</em> cluster's metadata response, i.e. from a host the
/// operator does not control. Without a match timeout, a catastrophic-
/// backtracking pattern such as <c>^(a+)+b$</c> paired with a long remote
/// topic name pins the shared metadata-sync loop indefinitely. The 100 ms
/// budget mirrors the existing convention in <c>AclEntry</c> and
/// <c>TenantValidator</c>.
/// </para>
/// </remarks>
public static class TopicFilterPattern
{
    /// <summary>Upper bound on the pattern itself, independent of the match timeout.</summary>
    public const int MaxLength = 256;

    private static readonly TimeSpan MatchTimeout = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Compiles <paramref name="pattern"/>. Throws <see cref="ArgumentException"/>
    /// for an over-long pattern and for invalid regex syntax, so callers can
    /// map both onto a 400 response.
    /// </summary>
    public static Regex Compile(string pattern)
    {
        ArgumentNullException.ThrowIfNull(pattern);

        if (pattern.Length > MaxLength)
        {
            throw new ArgumentException(
                $"topicFilter must be at most {MaxLength} characters (was {pattern.Length}).",
                nameof(pattern));
        }

        return new Regex(pattern, RegexOptions.CultureInvariant, MatchTimeout);
    }
}
