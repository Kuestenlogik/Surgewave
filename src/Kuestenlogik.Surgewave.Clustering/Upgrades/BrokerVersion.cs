using System.Globalization;
using System.Reflection;

namespace Kuestenlogik.Surgewave.Clustering.Upgrades;

/// <summary>
/// Represents a broker version using semantic versioning.
/// Used to track version compatibility during rolling upgrades.
/// </summary>
public sealed record BrokerVersion
{
    /// <summary>
    /// Major version number. Different major versions are incompatible (breaking changes).
    /// </summary>
    public required int Major { get; init; }

    /// <summary>
    /// Minor version number. Minor version differences are allowed within the same major version.
    /// </summary>
    public required int Minor { get; init; }

    /// <summary>
    /// Patch version number.
    /// </summary>
    public required int Patch { get; init; }

    /// <summary>
    /// Optional pre-release label (e.g., "alpha", "beta.1", "rc.2").
    /// </summary>
    public string? PreRelease { get; init; }

    /// <summary>
    /// The current broker version, derived from the assembly version.
    /// </summary>
    public static BrokerVersion Current { get; } = FromAssembly(typeof(BrokerVersion).Assembly);

    /// <summary>
    /// Reads the version from the assembly's informational version (SemVer
    /// including prerelease, build metadata after '+' stripped), falling back
    /// to the plain assembly version when the attribute is missing or invalid.
    /// </summary>
    internal static BrokerVersion FromAssembly(Assembly assembly)
    {
        var informational = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (informational is not null)
        {
            var metadataIndex = informational.IndexOf('+');
            if (metadataIndex >= 0)
                informational = informational[..metadataIndex];

            if (TryParse(informational, out var parsed) && parsed is not null)
                return parsed;
        }

        var fallback = assembly.GetName().Version;
        return new BrokerVersion
        {
            Major = fallback?.Major ?? 0,
            Minor = fallback?.Minor ?? 0,
            Patch = fallback?.Build ?? 0
        };
    }

    /// <summary>
    /// Parses a version string in the format "major.minor.patch[-prerelease]".
    /// </summary>
    /// <param name="version">The version string to parse.</param>
    /// <returns>A <see cref="BrokerVersion"/> instance.</returns>
    /// <exception cref="FormatException">Thrown when the version string is invalid.</exception>
    public static BrokerVersion Parse(string version)
    {
        ArgumentNullException.ThrowIfNull(version);

        var span = version.AsSpan().Trim();
        if (span.IsEmpty)
            throw new FormatException("Version string cannot be empty.");

        // Strip leading 'v' or 'V' if present
        if (span[0] is 'v' or 'V')
            span = span[1..];

        string? preRelease = null;
        var hyphenIndex = span.IndexOf('-');
        if (hyphenIndex >= 0)
        {
            preRelease = span[(hyphenIndex + 1)..].ToString();
            span = span[..hyphenIndex];
        }

        var parts = span.ToString().Split('.');
        if (parts.Length < 2 || parts.Length > 3)
            throw new FormatException($"Invalid version format: '{version}'. Expected 'major.minor.patch' or 'major.minor'.");

        if (!int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var major) || major < 0)
            throw new FormatException($"Invalid major version in '{version}'.");

        if (!int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var minor) || minor < 0)
            throw new FormatException($"Invalid minor version in '{version}'.");

        var patch = 0;
        if (parts.Length == 3)
        {
            if (!int.TryParse(parts[2], NumberStyles.None, CultureInfo.InvariantCulture, out patch) || patch < 0)
                throw new FormatException($"Invalid patch version in '{version}'.");
        }

        return new BrokerVersion
        {
            Major = major,
            Minor = minor,
            Patch = patch,
            PreRelease = preRelease
        };
    }

    /// <summary>
    /// Attempts to parse a version string. Returns false if parsing fails.
    /// </summary>
    public static bool TryParse(string? version, out BrokerVersion? result)
    {
        result = null;
        if (string.IsNullOrWhiteSpace(version))
            return false;

        try
        {
            result = Parse(version);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    /// <summary>
    /// Checks whether this version is compatible with another version.
    /// Compatibility rules:
    /// - Same major version = compatible
    /// - Different major version = incompatible (breaking changes)
    /// - Minor version differences allowed (forward/backward compatible within same major)
    /// </summary>
    public bool IsCompatibleWith(BrokerVersion other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return Major == other.Major;
    }

    /// <summary>
    /// Returns true if this version is newer than the other version.
    /// </summary>
    public bool IsNewerThan(BrokerVersion other)
    {
        ArgumentNullException.ThrowIfNull(other);

        if (Major != other.Major) return Major > other.Major;
        if (Minor != other.Minor) return Minor > other.Minor;
        if (Patch != other.Patch) return Patch > other.Patch;

        // Pre-release versions are considered older than release versions
        if (PreRelease is null && other.PreRelease is not null) return true;
        if (PreRelease is not null && other.PreRelease is null) return false;

        return string.Compare(PreRelease, other.PreRelease, StringComparison.OrdinalIgnoreCase) > 0;
    }

    /// <inheritdoc />
    public override string ToString() =>
        PreRelease is null
            ? $"{Major}.{Minor}.{Patch}"
            : $"{Major}.{Minor}.{Patch}-{PreRelease}";

    /// <summary>
    /// Custom equality: compare all version components.
    /// </summary>
    public bool Equals(BrokerVersion? other)
    {
        if (other is null) return false;
        return Major == other.Major
            && Minor == other.Minor
            && Patch == other.Patch
            && string.Equals(PreRelease, other.PreRelease, StringComparison.OrdinalIgnoreCase);
    }

    /// <inheritdoc />
    public override int GetHashCode() =>
        HashCode.Combine(Major, Minor, Patch, PreRelease?.ToUpperInvariant());
}
