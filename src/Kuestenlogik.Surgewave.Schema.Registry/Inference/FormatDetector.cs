using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Schema.Registry.Inference;

/// <summary>
/// Detects common string formats (date-time, email, URI, UUID, IP addresses)
/// using compiled regular expressions for performance.
/// </summary>
internal static partial class FormatDetector
{
    // Compiled regex patterns for format detection

    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", RegexOptions.Compiled)]
    private static partial Regex DateTimePattern();

    [GeneratedRegex(@"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", RegexOptions.Compiled)]
    private static partial Regex EmailPattern();

    [GeneratedRegex(@"^https?://", RegexOptions.Compiled | RegexOptions.IgnoreCase)]
    private static partial Regex UriPattern();

    [GeneratedRegex(@"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", RegexOptions.Compiled)]
    private static partial Regex UuidPattern();

    [GeneratedRegex(@"^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$", RegexOptions.Compiled)]
    private static partial Regex Ipv4Pattern();

    [GeneratedRegex(@"^([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}$", RegexOptions.Compiled)]
    private static partial Regex Ipv6Pattern();

    /// <summary>
    /// Detect the format of a string value.
    /// Returns null if no specific format is detected.
    /// </summary>
    public static string? DetectFormat(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return null;
        }

        // Check UUID first (most distinctive pattern)
        if (value.Length == 36 && UuidPattern().IsMatch(value))
        {
            return "uuid";
        }

        // Check date-time
        if (value.Length >= 19 && DateTimePattern().IsMatch(value))
        {
            return "date-time";
        }

        // Check URI
        if (UriPattern().IsMatch(value))
        {
            return "uri";
        }

        // Check email
        if (value.Contains('@') && EmailPattern().IsMatch(value))
        {
            return "email";
        }

        // Check IPv4
        if (value.Length is >= 7 and <= 15 && Ipv4Pattern().IsMatch(value))
        {
            return "ipv4";
        }

        // Check IPv6
        if (value.Contains(':') && Ipv6Pattern().IsMatch(value))
        {
            return "ipv6";
        }

        return null;
    }
}
