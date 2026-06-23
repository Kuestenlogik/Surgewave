using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-1161 — stricter validation rules and default values for LIST-type
/// configurations, plus a new lower bound of 1 for
/// <c>num.replica.fetchers</c>.
///
/// Surgewave's LIST-type configs (<see cref="SecurityConfig.SaslMechanisms"/>,
/// <see cref="SecurityConfig.Users"/>, <see cref="SecurityConfig.SuperUsers"/>
/// and <see cref="OAuth2Config.AllowedAlgorithms"/>) are validated by
/// <see cref="BrokerConfig.Validate"/>:
/// <list type="bullet">
///   <item>Null or whitespace entries are rejected outright (KIP-1161 forbids
///         null entries in LIST-type configs)</item>
///   <item>Duplicate entries are deduplicated in-place and the dropped count
///         is surfaced as a warning-shaped error so admins notice on
///         startup (upstream Kafka logs a warning; Surgewave routes it
///         through the same validation channel)</item>
/// </list>
///
/// The <c>num.replica.fetchers &gt;= 1</c> lower bound lives in
/// <see cref="DynamicBrokerConfig.SetConfig"/>.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1161ListConfigValidationTests
{
    private static DynamicBrokerConfig NewDynamicConfig(out BrokerConfig staticConfig)
    {
        staticConfig = new BrokerConfig { DataDirectory = Path.Combine(Path.GetTempPath(), "kip1161-" + Guid.NewGuid().ToString("N")) };
        return new DynamicBrokerConfig(staticConfig, NullLogger<DynamicBrokerConfig>.Instance);
    }

    [Fact]
    public void NumReplicaFetchers_ZeroValue_IsRejected()
    {
        var dyn = NewDynamicConfig(out _);
        var error = dyn.SetConfig("num.replica.fetchers", "0");
        Assert.NotNull(error);
        Assert.Contains("at least 1", error, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void NumReplicaFetchers_NegativeValue_IsRejected()
    {
        var dyn = NewDynamicConfig(out _);
        var error = dyn.SetConfig("num.replica.fetchers", "-3");
        Assert.NotNull(error);
        Assert.Contains("at least 1", error, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void NumReplicaFetchers_PositiveValue_IsAccepted()
    {
        var dyn = NewDynamicConfig(out _);
        var error = dyn.SetConfig("num.replica.fetchers", "4");
        Assert.Null(error);
    }

    [Fact]
    public void ListConfig_BlankEntry_IsRejected()
    {
        var config = new BrokerConfig
        {
            DataDirectory = "/tmp",
            Security = new SecurityConfig { SaslMechanisms = ["PLAIN", "", "SCRAM-SHA-256"] },
        };
        var errors = config.Validate();
        Assert.Contains(errors, e => e.Contains("SaslMechanisms", StringComparison.Ordinal) && e.Contains("blank", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ListConfig_DuplicateEntries_AreDeduplicatedAndReported()
    {
        var config = new BrokerConfig
        {
            DataDirectory = "/tmp",
            Security = new SecurityConfig { SaslMechanisms = ["PLAIN", "SCRAM-SHA-256", "plain"] }, // 'PLAIN' == 'plain' case-folded
        };
        var errors = config.Validate();
        Assert.Contains(errors, e => e.Contains("SaslMechanisms", StringComparison.Ordinal) && e.Contains("duplicate", StringComparison.OrdinalIgnoreCase));
        // Verify in-place dedup: only two entries survive.
        Assert.Equal(2, config.Security.SaslMechanisms.Length);
    }

    [Fact]
    public void ListConfig_NoDuplicates_PassesCleanly()
    {
        var config = new BrokerConfig
        {
            DataDirectory = "/tmp",
            Security = new SecurityConfig { SaslMechanisms = ["PLAIN", "SCRAM-SHA-512"] },
        };
        var errors = config.Validate();
        Assert.DoesNotContain(errors, e => e.Contains("SaslMechanisms", StringComparison.Ordinal));
    }

    [Fact]
    public void ListConfig_DedupRespectsCaseSensitivityPerConfig()
    {
        // Users is Ordinal-cased (distinct credentials per user) — 'alice'
        // and 'Alice' are two different users and must NOT be deduplicated.
        var config = new BrokerConfig
        {
            DataDirectory = "/tmp",
            Security = new SecurityConfig { Users = ["alice", "Alice", "bob"] },
        };
        var errors = config.Validate();
        Assert.DoesNotContain(errors, e => e.Contains("Users", StringComparison.Ordinal) && e.Contains("duplicate", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(3, config.Security.Users.Length);
    }

    [Fact]
    public void ListConfig_AllowedAlgorithms_DeduplicatesCaseInsensitively()
    {
        // OAuth2.AllowedAlgorithms is OrdinalIgnoreCase (RS256 == rs256 in JWT).
        var config = new BrokerConfig
        {
            DataDirectory = "/tmp",
            Security = new SecurityConfig
            {
                OAuth2 = new OAuth2Config { AllowedAlgorithms = ["RS256", "rs256", "ES256"] },
            },
        };
        var errors = config.Validate();
        Assert.Contains(errors, e => e.Contains("AllowedAlgorithms", StringComparison.Ordinal) && e.Contains("duplicate", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(2, config.Security.OAuth2.AllowedAlgorithms.Length);
    }
}
