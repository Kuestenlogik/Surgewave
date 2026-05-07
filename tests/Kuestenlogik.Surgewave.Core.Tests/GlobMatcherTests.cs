using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for GlobMatcher utility - glob pattern matching with *, **, and ? wildcards.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class GlobMatcherTests
{
    #region Wildcard * (star) Tests

    [Fact]
    public void Matches_Star_MatchesEverything()
    {
        Assert.True(GlobMatcher.Matches("anything", "*"));
        Assert.True(GlobMatcher.Matches("", "*"));
        Assert.True(GlobMatcher.Matches("long.dotted.name", "*"));
    }

    [Fact]
    public void Matches_ExactMatch_ReturnsTrue()
    {
        Assert.True(GlobMatcher.Matches("orders", "orders"));
        Assert.True(GlobMatcher.Matches("my-topic.v2", "my-topic.v2"));
    }

    [Fact]
    public void Matches_ExactMismatch_ReturnsFalse()
    {
        Assert.False(GlobMatcher.Matches("orders", "events"));
        Assert.False(GlobMatcher.Matches("a", "b"));
    }

    [Theory]
    [InlineData("orders.created", "orders.*", true)]
    [InlineData("orders.deleted", "orders.*", true)]
    [InlineData("events.created", "orders.*", false)]
    [InlineData("prefix-topic", "prefix-*", true)]
    [InlineData("prefix-", "prefix-*", true)]
    [InlineData("other-topic", "prefix-*", false)]
    public void Matches_TrailingStar_MatchesCorrectly(string input, string pattern, bool expected)
    {
        Assert.Equal(expected, GlobMatcher.Matches(input, pattern));
    }

    [Theory]
    [InlineData("my-orders", "*-orders", true)]
    [InlineData("your-orders", "*-orders", true)]
    [InlineData("my-events", "*-orders", false)]
    public void Matches_LeadingStar_MatchesCorrectly(string input, string pattern, bool expected)
    {
        Assert.Equal(expected, GlobMatcher.Matches(input, pattern));
    }

    [Theory]
    [InlineData("prefix-middle-suffix", "prefix-*-suffix", true)]
    [InlineData("prefix-x-suffix", "prefix-*-suffix", true)]
    [InlineData("prefix--suffix", "prefix-*-suffix", true)]
    [InlineData("prefix-middle-other", "prefix-*-suffix", false)]
    public void Matches_MiddleStar_MatchesCorrectly(string input, string pattern, bool expected)
    {
        Assert.Equal(expected, GlobMatcher.Matches(input, pattern));
    }

    #endregion

    #region Wildcard ** (double star) Tests

    [Theory]
    [InlineData("a/b/c", "a/**", true)]
    [InlineData("a/b/c/d/e", "a/**", true)]
    [InlineData("a", "a/**", false)]
    [InlineData("b/c", "a/**", false)]
    public void Matches_DoubleStar_MatchesMultipleSegments(string input, string pattern, bool expected)
    {
        Assert.Equal(expected, GlobMatcher.Matches(input, pattern));
    }

    #endregion

    #region Wildcard ? (question mark) Tests

    [Theory]
    [InlineData("abc", "a?c", true)]
    [InlineData("aXc", "a?c", true)]
    [InlineData("ac", "a?c", false)]
    [InlineData("abbc", "a?c", false)]
    public void Matches_QuestionMark_MatchesSingleChar(string input, string pattern, bool expected)
    {
        Assert.Equal(expected, GlobMatcher.Matches(input, pattern));
    }

    [Fact]
    public void Matches_MultipleQuestionMarks_MatchCorrectCount()
    {
        Assert.True(GlobMatcher.Matches("ab", "??"));
        Assert.False(GlobMatcher.Matches("abc", "??"));
        Assert.False(GlobMatcher.Matches("a", "??"));
    }

    #endregion

    #region DotIsSeparator Mode Tests

    [Fact]
    public void Matches_DotIsSeparator_StarDoesNotMatchDot()
    {
        Assert.True(GlobMatcher.Matches("orders.created", "orders.*", dotIsSeparator: true));
        // Star should not match across dots in dot-separator mode
        Assert.False(GlobMatcher.Matches("orders.nested.created", "orders.*", dotIsSeparator: true));
    }

    [Fact]
    public void Matches_DotIsSeparator_DoubleStarMatchesDot()
    {
        Assert.True(GlobMatcher.Matches("orders.nested.created", "orders.**", dotIsSeparator: true));
    }

    #endregion

    #region MatchesAny Tests

    [Fact]
    public void MatchesAny_OnePatternMatches_ReturnsTrue()
    {
        Assert.True(GlobMatcher.MatchesAny("orders", ["events", "orders", "alerts"]));
    }

    [Fact]
    public void MatchesAny_NoPatternMatches_ReturnsFalse()
    {
        Assert.False(GlobMatcher.MatchesAny("logs", ["events", "orders", "alerts"]));
    }

    [Fact]
    public void MatchesAny_WildcardPattern_MatchesAll()
    {
        Assert.True(GlobMatcher.MatchesAny("anything", ["*"]));
        Assert.True(GlobMatcher.MatchesAny("anything", ["no-match", "*"]));
    }

    [Fact]
    public void MatchesAny_EmptyPatterns_ReturnsFalse()
    {
        Assert.False(GlobMatcher.MatchesAny("anything", Array.Empty<string>()));
    }

    [Fact]
    public void MatchesAny_WithGlobPatterns_MatchesCorrectly()
    {
        var patterns = new[] { "orders.*", "events.*", "alerts" };

        Assert.True(GlobMatcher.MatchesAny("orders.created", patterns));
        Assert.True(GlobMatcher.MatchesAny("events.updated", patterns));
        Assert.True(GlobMatcher.MatchesAny("alerts", patterns));
        Assert.False(GlobMatcher.MatchesAny("logs.info", patterns));
    }

    #endregion

    #region Edge Cases

    [Fact]
    public void Matches_EmptyInput_EmptyPattern_ReturnsTrue()
    {
        Assert.True(GlobMatcher.Matches("", ""));
    }

    [Fact]
    public void Matches_EmptyInput_NonEmptyPattern_ReturnsFalse()
    {
        Assert.False(GlobMatcher.Matches("", "abc"));
    }

    [Fact]
    public void Matches_SpecialRegexChars_AreEscaped()
    {
        // Dots, brackets, parens, etc. in the pattern should be literal
        Assert.True(GlobMatcher.Matches("a.b", "a.b"));
        Assert.False(GlobMatcher.Matches("aXb", "a.b"));
    }

    #endregion
}
