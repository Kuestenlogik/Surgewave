using System.Diagnostics;
using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Clustering.GeoReplication;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Regression tests for the cluster-link topic filter (CodeQL cs/regex-injection,
/// alerts #119 / #120). The filter is operator-supplied over REST and is matched
/// against topic names taken from the remote cluster's metadata response, so an
/// unbounded regex could pin the shared metadata-sync loop indefinitely.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class TopicFilterPatternTests
{
    [Fact]
    public void Compile_CatastrophicBacktracking_TimesOutInsteadOfHanging()
    {
        // Classic exponential-backtracking pattern; before the fix this ran with
        // Regex.InfiniteMatchTimeout and never returned.
        var filter = TopicFilterPattern.Compile("^(a+)+b$");
        var hostileTopicName = new string('a', 5000);

        var sw = Stopwatch.StartNew();
        Assert.Throws<RegexMatchTimeoutException>(() => filter.IsMatch(hostileTopicName));
        sw.Stop();

        Assert.True(
            sw.Elapsed < TimeSpan.FromSeconds(2),
            $"Match should abort on the 100 ms budget, took {sw.Elapsed}.");
    }

    [Fact]
    public void Compile_OverlongPattern_IsRejected()
    {
        var tooLong = new string('a', TopicFilterPattern.MaxLength + 1);

        var ex = Assert.Throws<ArgumentException>(() => TopicFilterPattern.Compile(tooLong));
        Assert.Contains(TopicFilterPattern.MaxLength.ToString(), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Compile_PatternAtMaxLength_IsAccepted()
    {
        var atLimit = new string('a', TopicFilterPattern.MaxLength);

        var filter = TopicFilterPattern.Compile(atLimit);

        Assert.Matches(filter, atLimit);
    }

    [Fact]
    public void Compile_InvalidSyntax_ThrowsArgumentException()
    {
        // The REST layer maps ArgumentException onto 400, so syntax errors must
        // surface as that type and not as something the endpoint lets escape.
        // ThrowsAny, because RegexParseException derives from ArgumentException —
        // which is exactly why the endpoint's catch block already handles it.
        Assert.ThrowsAny<ArgumentException>(() => TopicFilterPattern.Compile("(unclosed"));
    }

    [Theory]
    [InlineData("^orders\\..*$", "orders.eu", true)]
    [InlineData("^orders\\..*$", "payments.eu", false)]
    [InlineData(".*", "anything", true)]
    public void Compile_OrdinaryPattern_MatchesAsExpected(string pattern, string topic, bool expected)
    {
        var filter = TopicFilterPattern.Compile(pattern);

        Assert.Equal(expected, filter.IsMatch(topic));
    }

    [Fact]
    public void Compile_RestValidationAndSink_AgreeOnTheSamePattern()
    {
        // ClusterLinkRestApi validates with this factory and ClusterLinkManager
        // compiles with it. If the two ever diverge, a pattern accepted with 200
        // would fail at the sink, where the exception is swallowed and replication
        // goes quiet. Lookarounds are the case that a NonBacktracking engine would
        // reject — pin that they stay accepted on both sides.
        const string withLookahead = "^(?!__)orders\\..*$";

        var validation = TopicFilterPattern.Compile(withLookahead);
        var sink = TopicFilterPattern.Compile(withLookahead);

        Assert.Matches(validation, "orders.eu");
        Assert.DoesNotMatch(sink, "__consumer_offsets");
    }
}
