using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Schema.Registry.Linking;

/// <summary>
/// Matches subject names against glob-style patterns for schema linking filtering.
/// Supports <c>*</c> (any characters) and <c>?</c> (single character) wildcards.
/// </summary>
public static class SubjectPatternMatcher
{
    /// <summary>
    /// Returns <c>true</c> if the subject name matches at least one of the patterns.
    /// </summary>
    /// <param name="subject">The subject name to test.</param>
    /// <param name="patterns">List of glob patterns to match against.</param>
    public static bool MatchesAny(string subject, IReadOnlyList<string> patterns)
    {
        return GlobMatcher.MatchesAny(subject, patterns);
    }

    /// <summary>
    /// Returns <c>true</c> if the subject name matches the glob pattern.
    /// </summary>
    /// <param name="subject">The subject name to test.</param>
    /// <param name="pattern">Glob pattern (e.g., <c>"orders-*"</c>, <c>"*-value"</c>, <c>"*"</c>).</param>
    public static bool Matches(string subject, string pattern)
    {
        ArgumentNullException.ThrowIfNull(subject);
        ArgumentNullException.ThrowIfNull(pattern);

        return GlobMatcher.Matches(subject, pattern);
    }
}
