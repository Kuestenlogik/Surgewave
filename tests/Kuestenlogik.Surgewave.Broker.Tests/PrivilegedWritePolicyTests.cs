using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Decision tests for the loopback gate on privilege-granting management writes
/// (GHSA-2fv9-qr54-gjhp). REST auth is opt-in, so with it off an anonymous
/// network client could otherwise upload a plugin signing key or repoint the
/// package feed — both equivalent to code execution at plugin load.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PrivilegedWritePolicyTests
{
    private static PrivilegedWritePolicy AuthDisabled() => new(new RestApiAuthConfig());

    private static PrivilegedWritePolicy AuthEnabled() =>
        new(new RestApiAuthConfig { Enabled = true });

    private static PrivilegedWritePolicy OptedOut() =>
        new(new RestApiAuthConfig { AllowUnauthenticatedRemoteWrites = true });

    [Theory]
    [InlineData("/api/plugins/trusted-keys/upload")]
    [InlineData("/api/plugins/trusted-keys/generate")]
    [InlineData("/api/plugins/trusted-keys/alice")]
    [InlineData("/api/plugins/repositories/")]
    [InlineData("/api/cluster-links")]
    public void RemoteWriteToPrivilegedPath_IsBlocked(string path)
    {
        Assert.True(AuthDisabled().ShouldBlock(path, "POST", isLoopback: false));
    }

    [Theory]
    [InlineData("/api/plugins/trusted-keys/upload")]
    [InlineData("/api/plugins/repositories/")]
    [InlineData("/api/cluster-links")]
    public void LoopbackWriteToPrivilegedPath_IsAllowed(string path)
    {
        // The all-in-one deployment (broker + Control on one host) must keep working.
        Assert.False(AuthDisabled().ShouldBlock(path, "POST", isLoopback: true));
    }

    [Theory]
    [InlineData("POST")]
    [InlineData("PUT")]
    [InlineData("DELETE")]
    [InlineData("PATCH")]
    public void EveryMutatingMethod_IsCovered(string method)
    {
        Assert.True(AuthDisabled().ShouldBlock("/api/plugins/trusted-keys/upload", method, isLoopback: false));
    }

    [Theory]
    [InlineData("GET")]
    [InlineData("HEAD")]
    [InlineData("OPTIONS")]
    public void Reads_AreNotBlocked(string method)
    {
        // Reads expose configuration, not privilege — blocking them would break
        // Control's read-only views for no security gain.
        Assert.False(AuthDisabled().ShouldBlock("/api/plugins/trusted-keys", method, isLoopback: false));
    }

    [Theory]
    [InlineData("/api/topics")]
    [InlineData("/api/plugins")]
    [InlineData("/api/consumer-groups")]
    [InlineData("/admin/anything")]
    public void NonPrivilegedPaths_AreNotBlocked(string path)
    {
        Assert.False(AuthDisabled().ShouldBlock(path, "POST", isLoopback: false));
    }

    [Fact]
    public void PathMatchIsCaseInsensitive()
    {
        Assert.True(AuthDisabled().ShouldBlock("/API/Plugins/Trusted-Keys/upload", "POST", isLoopback: false));
    }

    [Fact]
    public void WhenRestAuthIsEnabled_ThisGateStandsDown()
    {
        // RestApiAuthPolicy is strictly stronger there; two gates on one request
        // would only make the failure mode harder to read.
        Assert.False(AuthEnabled().ShouldBlock("/api/plugins/trusted-keys/upload", "POST", isLoopback: false));
    }

    [Fact]
    public void ExplicitOptOut_AllowsRemoteWrites()
    {
        Assert.False(OptedOut().ShouldBlock("/api/plugins/trusted-keys/upload", "POST", isLoopback: false));
    }

    [Fact]
    public void PrivilegedPrefixes_CoverTheAdvisorySurface()
    {
        var policy = AuthDisabled();

        Assert.True(policy.IsPrivileged("/api/plugins/trusted-keys/upload"));
        Assert.True(policy.IsPrivileged("/api/plugins/repositories/nuget"));
        Assert.True(policy.IsPrivileged("/api/cluster-links/link-1/pause"));
        Assert.False(policy.IsPrivileged("/api/plugins/installed"));
    }

    [Fact]
    public void EmptyPrefixEntry_DoesNotMatchEverything()
    {
        // A stray empty string in configuration must not turn into a match-all,
        // which would block every mutating request on the broker.
        var policy = new PrivilegedWritePolicy(new RestApiAuthConfig
        {
            PrivilegedWritePathPrefixes = ["", "/api/cluster-links"],
        });

        Assert.False(policy.ShouldBlock("/api/topics", "POST", isLoopback: false));
        Assert.True(policy.ShouldBlock("/api/cluster-links", "POST", isLoopback: false));
    }
}
