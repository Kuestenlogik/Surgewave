using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Shared utility for matching strings against glob-style patterns.
/// Supports <c>*</c> (single segment), <c>**</c> (multiple segments), and <c>?</c> (single character) wildcards.
/// </summary>
public static class GlobMatcher
{
    /// <summary>
    /// Returns <c>true</c> if the input matches the glob pattern.
    /// </summary>
    /// <param name="input">The string to test.</param>
    /// <param name="pattern">Glob pattern (e.g., <c>"orders.*"</c>, <c>"*"</c>).</param>
    /// <param name="dotIsSeparator">If true, <c>*</c> does not match across dots. Default false (dots are normal chars).</param>
    public static bool Matches(string input, string pattern, bool dotIsSeparator = false)
    {
        if (pattern == "*") return true;
        if (pattern == input) return true;

        var singleStar = dotIsSeparator ? "[^./]*" : "[^/]*";
        var regexPattern = "^" + Regex.Escape(pattern)
            .Replace("\\*\\*", ".*")   // ** matches everything including separators
            .Replace("\\*", singleStar) // * matches within segment
            .Replace("\\?", ".")       // ? matches single char
            + "$";
        return Regex.IsMatch(input, regexPattern, RegexOptions.None, TimeSpan.FromMilliseconds(100));
    }

    /// <summary>
    /// Returns <c>true</c> if the input matches any of the specified patterns.
    /// </summary>
    /// <param name="input">The string to test.</param>
    /// <param name="patterns">Collection of glob patterns to match against.</param>
    public static bool MatchesAny(string input, IEnumerable<string> patterns)
    {
        foreach (var pattern in patterns)
            if (Matches(input, pattern)) return true;
        return false;
    }
}
