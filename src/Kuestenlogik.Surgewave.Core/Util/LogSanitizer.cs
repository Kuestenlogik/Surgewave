using System.Text;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Sanitises strings before they are written to a log sink.
/// Protects against log-forging (CWE-117): an attacker who controls
/// part of a logged value can otherwise inject CR/LF and smuggle
/// fake log lines past a downstream log reader, fooling parsers and
/// audit tooling.
/// </summary>
/// <remarks>
/// Replaces newline characters (CR, LF), all ASCII control codes,
/// and the Unicode line/paragraph separators (NEL U+0085, LS U+2028,
/// PS U+2029) with a single underscore. The output is otherwise
/// untouched — printable ASCII and non-control Unicode pass
/// through verbatim.
/// </remarks>
public static class LogSanitizer
{
    /// <summary>
    /// Returns a copy of <paramref name="input"/> with newline and
    /// control characters replaced. Returns the empty string for
    /// <c>null</c>.
    /// </summary>
    public static string Sanitize(string? input)
    {
        if (input is null)
        {
            return string.Empty;
        }

        // Fast-path: scan once, allocate only if we hit a bad char.
        var i = 0;
        for (; i < input.Length; i++)
        {
            if (NeedsReplace(input[i]))
            {
                break;
            }
        }

        if (i == input.Length)
        {
            return input;
        }

        var sb = new StringBuilder(input.Length);
        sb.Append(input, 0, i);
        for (; i < input.Length; i++)
        {
            var c = input[i];
            sb.Append(NeedsReplace(c) ? '_' : c);
        }
        return sb.ToString();
    }

    /// <summary>
    /// Sanitises an arbitrary <c>object</c> for logging by routing it
    /// through <see cref="object.ToString"/> and then the string overload.
    /// </summary>
    public static string Sanitize(object? input) =>
        Sanitize(input?.ToString());

    private static bool NeedsReplace(char c) =>
        c == '\r' || c == '\n'
        || c < 0x20
        || c == 0x7F          // DEL
        || c == 0x85          // NEL
        || c == 0x2028        // LINE SEPARATOR
        || c == 0x2029;       // PARAGRAPH SEPARATOR
}
