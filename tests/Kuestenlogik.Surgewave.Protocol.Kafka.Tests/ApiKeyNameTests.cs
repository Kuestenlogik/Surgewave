using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// ApiKeyName replaces one Enum.ToString() allocation per request on the metrics tap (#83),
/// so its output must stay identical to ToString() for every defined key.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ApiKeyNameTests
{
    [Fact]
    public void Of_MatchesToString_ForEveryDefinedApiKey()
    {
        foreach (var apiKey in Enum.GetValues<ApiKey>())
        {
            Assert.Equal(apiKey.ToString(), ApiKeyName.Of(apiKey));
        }
    }

    [Fact]
    public void Of_UndefinedApiKey_FallsBackToToString()
    {
        var unknown = (ApiKey)9999;

        Assert.Equal(unknown.ToString(), ApiKeyName.Of(unknown));
    }

    [Fact]
    public void Of_ReturnsCachedInstance()
    {
        Assert.Same(ApiKeyName.Of(ApiKey.Produce), ApiKeyName.Of(ApiKey.Produce));
    }
}
