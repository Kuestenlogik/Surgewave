using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Decision tests for the broker REST auth gate (#37). Default-deny: everything
/// except the anonymous allowlist requires authentication — including the gRPC
/// transcoding (/v3), Schema Registry (/subjects) and /bowire surfaces that a
/// naive /admin+/api allowlist would have left open. Mutating REST methods also
/// require the role; gRPC requires authentication only.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RestApiAuthPolicyTests
{
    private static readonly RestApiAuthPolicy Policy = new(new RestApiAuthConfig());

    private static RestApiAuthDecision Rest(string path, string method, bool auth, bool role) =>
        Policy.Evaluate(path, method, isGrpc: false, isAuthenticated: auth, isInRequiredRole: role);

    private static RestApiAuthDecision Grpc(string path, bool auth, bool role) =>
        Policy.Evaluate(path, "POST", isGrpc: true, isAuthenticated: auth, isInRequiredRole: role);

    [Theory]
    [InlineData("/health")]
    [InlineData("/health/ready")]
    [InlineData("/health/live")]
    [InlineData("/metrics")]
    [InlineData("/sd-targets")]
    public void AnonymousPaths_AreAllowedWithoutAuth(string path)
    {
        Assert.Equal(RestApiAuthDecision.Allow, Rest(path, "GET", auth: false, role: false));
    }

    [Theory]
    [InlineData("/admin/acls")]
    [InlineData("/api/kv/buckets")]
    [InlineData("/api/plugins")]
    [InlineData("/v3/topics")]           // gRPC-JSON transcoding — was bypassable
    [InlineData("/subjects")]            // Schema Registry — was bypassable
    [InlineData("/subjects/orders/versions")]
    [InlineData("/config")]
    [InlineData("/bowire")]              // interactive gRPC console — was bypassable
    [InlineData("/api/license")]         // no longer an anonymous carve-out
    [InlineData("/")]                    // unknown paths default-deny too
    public void ProtectedPath_NoAuth_IsUnauthenticated(string path)
    {
        Assert.Equal(RestApiAuthDecision.Unauthenticated, Rest(path, "GET", auth: false, role: false));
    }

    [Fact]
    public void TranscodedMutation_NoAuth_IsUnauthenticated()
    {
        // POST /v3/topics (create topic) must not be reachable unauthenticated.
        Assert.Equal(RestApiAuthDecision.Unauthenticated, Rest("/v3/topics", "POST", auth: false, role: false));
    }

    [Fact]
    public void ProtectedPath_Get_AuthenticatedWithoutRole_IsAllowed()
    {
        Assert.Equal(RestApiAuthDecision.Allow, Rest("/api/kv/buckets", "GET", auth: true, role: false));
    }

    [Theory]
    [InlineData("POST")]
    [InlineData("PUT")]
    [InlineData("DELETE")]
    [InlineData("PATCH")]
    public void ProtectedPath_MutatingRest_AuthenticatedWithoutRole_IsForbidden(string method)
    {
        Assert.Equal(RestApiAuthDecision.Forbidden, Rest("/admin/acls", method, auth: true, role: false));
    }

    [Theory]
    [InlineData("POST")]
    [InlineData("DELETE")]
    public void ProtectedPath_MutatingRest_AuthenticatedWithRole_IsAllowed(string method)
    {
        Assert.Equal(RestApiAuthDecision.Allow, Rest("/admin/acls", method, auth: true, role: true));
    }

    [Fact]
    public void Grpc_NoAuth_IsUnauthenticated()
    {
        Assert.Equal(RestApiAuthDecision.Unauthenticated, Grpc("/klsurgewave.TopicService/DeleteTopic", auth: false, role: false));
    }

    [Fact]
    public void Grpc_Authenticated_IsAllowed_EvenWithoutRole()
    {
        // gRPC is always POST; method-based role granularity is impossible, so a
        // valid token is the bar (Control does the fine-grained authz).
        Assert.Equal(RestApiAuthDecision.Allow, Grpc("/klsurgewave.AdminService/AlterConfig", auth: true, role: false));
    }

    [Fact]
    public void IsProtected_EverythingExceptAnonymousAllowlist()
    {
        Assert.True(Policy.IsProtected("/api/kv/buckets"));
        Assert.True(Policy.IsProtected("/v3/topics"));
        Assert.True(Policy.IsProtected("/subjects"));
        Assert.True(Policy.IsProtected("/bowire"));
        Assert.False(Policy.IsProtected("/health"));
        Assert.False(Policy.IsProtected("/metrics"));
    }

    [Fact]
    public void AnonymousPrefix_SegmentBoundary_DoesNotOverMatch()
    {
        // "/health-internal" must NOT be treated as anonymous just by prefix.
        Assert.True(Policy.IsProtected("/health-internal"));
    }

    [Fact]
    public void CustomAnonymousPrefixes_AreHonored()
    {
        var policy = new RestApiAuthPolicy(new RestApiAuthConfig { AnonymousPathPrefixes = ["/public"] });

        Assert.False(policy.IsProtected("/public/thing"));
        Assert.True(policy.IsProtected("/health")); // no longer anonymous under custom config
    }
}
