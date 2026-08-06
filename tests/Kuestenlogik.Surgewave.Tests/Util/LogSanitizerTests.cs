using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Tests.Helpers;
using Xunit;

namespace Kuestenlogik.Surgewave.Tests.Util;

/// <summary>
/// Tests for the log-forging guard (CWE-117). Fifteen call sites across the
/// broker, marketplace and clustering assemblies depend on this class, so its
/// behaviour is worth pinning explicitly rather than inferring from those.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class LogSanitizerTests
{
    [Theory]
    [InlineData("\n")]
    [InlineData("\r")]
    [InlineData("\r\n")]
    [InlineData("\u0085")]   // NEL
    [InlineData("\u2028")]   // LINE SEPARATOR
    [InlineData("\u2029")]   // PARAGRAPH SEPARATOR
    [InlineData("\0")]       // NUL
    [InlineData("\u001b")]   // ESC — terminal escape sequences
    [InlineData("\u007f")]   // DEL
    public void LineBreakingAndControlCharacters_AreReplaced(string bad)
    {
        var result = LogSanitizer.Sanitize($"before{bad}after");

        Assert.DoesNotContain(bad, result, StringComparison.Ordinal);
        Assert.StartsWith("before", result, StringComparison.Ordinal);
        Assert.EndsWith("after", result, StringComparison.Ordinal);
    }

    [Fact]
    public void ForgedLogLine_IsNeutralised()
    {
        // The actual attack: smuggle a second, fake record past a line-oriented
        // log reader.
        const string attack = "alice\ninfo: Surgewave.Broker[0] All checks passed";

        var result = LogSanitizer.Sanitize(attack);

        Assert.DoesNotContain('\n', result);
        Assert.DoesNotContain('\r', result);
        Assert.Equal("alice_info: Surgewave.Broker[0] All checks passed", result);
    }

    [Fact]
    public void CleanInput_IsReturnedUnchanged()
    {
        // Fast path: scans once and returns the same instance, no allocation.
        const string clean = "orders.eu-west-1";

        Assert.Same(clean, LogSanitizer.Sanitize(clean));
    }

    [Fact]
    public void Null_BecomesEmptyString()
    {
        Assert.Equal(string.Empty, LogSanitizer.Sanitize((string?)null));
    }

    [Fact]
    public void EmptyString_StaysEmpty()
    {
        Assert.Equal(string.Empty, LogSanitizer.Sanitize(string.Empty));
    }

    [Theory]
    [InlineData("täglich")]
    [InlineData("orders/eu")]
    [InlineData("emoji 🚀")]
    [InlineData("~!@#$%^&*()_+{}|:\"<>?")]
    public void PrintableContent_PassesThroughVerbatim(string input)
    {
        Assert.Equal(input, LogSanitizer.Sanitize(input));
    }

    [Fact]
    public void Tab_IsReplacedToo()
    {
        // Deliberate: tab is the field separator in several of our own log and
        // CLI output formats, so letting it through would allow column forging.
        Assert.Equal("a_b", LogSanitizer.Sanitize("a\tb"));
    }

    [Fact]
    public void EveryReplacementIsExactlyOneCharacter()
    {
        // Length preservation proves no character is silently dropped or
        // expanded, which a naive replace-with-escape-sequence would do.
        const string input = "a\r\n\0b";

        var result = LogSanitizer.Sanitize(input);

        Assert.Equal(input.Length, result.Length);
        Assert.Equal("a___b", result);
    }

    [Fact]
    public void ObjectOverload_RoutesThroughToString()
    {
        Assert.Equal("_", LogSanitizer.Sanitize((object?)'\n'));
        Assert.Equal("42", LogSanitizer.Sanitize((object?)42));
    }

    [Fact]
    public void ObjectOverload_Null_BecomesEmptyString()
    {
        Assert.Equal(string.Empty, LogSanitizer.Sanitize((object?)null));
    }
}
