using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// A dependency on another Surgewave plugin.
/// </summary>
public sealed record PluginDependency
{
    [JsonPropertyName("id")]
    public required string Id { get; init; }

    [JsonPropertyName("version")]
    public required string Version { get; init; }

    [JsonPropertyName("optional")]
    public bool Optional { get; init; }

    /// <summary>
    /// Checks if the given version satisfies this dependency's version constraint.
    /// </summary>
    public bool IsSatisfiedBy(string candidateVersion)
    {
        if (string.IsNullOrEmpty(Version) || Version == "*")
            return true;

        if (!System.Version.TryParse(candidateVersion, out var candidate))
            return false;

        if (Version.StartsWith(">=", StringComparison.Ordinal))
            return System.Version.TryParse(Version[2..], out var min) && candidate >= min;
        if (Version.StartsWith('>'))
            return System.Version.TryParse(Version[1..], out var gt) && candidate > gt;
        if (Version.StartsWith("<=", StringComparison.Ordinal))
            return System.Version.TryParse(Version[2..], out var max) && candidate <= max;
        if (Version.StartsWith('<'))
            return System.Version.TryParse(Version[1..], out var lt) && candidate < lt;

        if (Version.StartsWith('^'))
        {
            // Compatible with: same major, >= minor
            if (!System.Version.TryParse(Version[1..], out var caret)) return false;
            return candidate.Major == caret.Major && candidate >= caret;
        }

        if (Version.StartsWith('~'))
        {
            // Approximately: same major.minor, >= patch
            if (!System.Version.TryParse(Version[1..], out var tilde)) return false;
            return candidate.Major == tilde.Major && candidate.Minor == tilde.Minor && candidate >= tilde;
        }

        // Exact match
        return System.Version.TryParse(Version, out var exact) && candidate == exact;
    }
}
