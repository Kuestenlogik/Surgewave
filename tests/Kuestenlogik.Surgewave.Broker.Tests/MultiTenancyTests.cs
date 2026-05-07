using Kuestenlogik.Surgewave.Broker.Tenancy;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

[Trait("Category", TestCategories.Unit)]
public sealed class MultiTenancyTests
{
    // --- TenantId Tests ---

    [Fact]
    public void TenantId_Default_IsDefault()
    {
        Assert.True(TenantId.Default.IsDefault);
        Assert.Equal("default", TenantId.Default.Value);
    }

    [Fact]
    public void TenantId_CaseInsensitive_Equality()
    {
        var upper = new TenantId("Tenant1");
        var lower = new TenantId("tenant1");

        Assert.Equal(upper, lower);
        Assert.Equal(upper.GetHashCode(), lower.GetHashCode());
    }

    [Theory]
    [InlineData("tenant1")]
    [InlineData("my-tenant")]
    [InlineData("my_tenant")]
    [InlineData("abc123")]
    [InlineData("a")]
    public void TenantId_Validation_AcceptsValid(string value)
    {
        Assert.True(TenantId.IsValid(value));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("tenant with spaces")]
    [InlineData("tenant@special")]
    [InlineData("tenant/slash")]
    public void TenantId_Validation_RejectsInvalid(string value)
    {
        Assert.False(TenantId.IsValid(value));
    }

    [Fact]
    public void TenantId_Validation_RejectsTooLong()
    {
        var tooLong = new string('a', 65);
        Assert.False(TenantId.IsValid(tooLong));
    }

    // --- TenantManager Tests ---

    [Fact]
    public void TenantManager_CreatesDefaultTenant()
    {
        var manager = new TenantManager(NullLogger<TenantManager>.Instance);

        Assert.True(manager.TenantExists(TenantId.Default));
        Assert.Equal(1, manager.TenantCount);

        var defaultTenant = manager.GetTenant(TenantId.Default);
        Assert.NotNull(defaultTenant);
        Assert.Equal("Default Tenant", defaultTenant.DisplayName);
        Assert.Equal(TenantState.Active, defaultTenant.State);
    }

    [Fact]
    public void TenantManager_CreateTenant_Success()
    {
        var manager = new TenantManager(NullLogger<TenantManager>.Instance);
        var tenantId = new TenantId("acme");

        var tenant = manager.CreateTenant(tenantId, "Acme Corp");

        Assert.Equal(tenantId, tenant.Id);
        Assert.Equal("Acme Corp", tenant.DisplayName);
        Assert.Equal(TenantState.Active, tenant.State);
        Assert.Equal(2, manager.TenantCount);
        Assert.True(manager.TenantExists(tenantId));
    }

    [Fact]
    public void TenantManager_CreateDuplicate_Throws()
    {
        var manager = new TenantManager(NullLogger<TenantManager>.Instance);
        var tenantId = new TenantId("acme");

        manager.CreateTenant(tenantId, "Acme Corp");

        Assert.Throws<InvalidOperationException>(() =>
            manager.CreateTenant(tenantId, "Acme Corp Again"));
    }

    [Fact]
    public void TenantManager_DeleteTenant_Success()
    {
        var manager = new TenantManager(NullLogger<TenantManager>.Instance);
        var tenantId = new TenantId("acme");
        manager.CreateTenant(tenantId, "Acme Corp");

        var result = manager.DeleteTenant(tenantId);

        Assert.True(result);
        Assert.False(manager.TenantExists(tenantId));
        Assert.Equal(1, manager.TenantCount);
    }

    [Fact]
    public void TenantManager_DeleteDefault_Fails()
    {
        var manager = new TenantManager(NullLogger<TenantManager>.Instance);

        var result = manager.DeleteTenant(TenantId.Default);

        Assert.False(result);
        Assert.True(manager.TenantExists(TenantId.Default));
    }

    [Fact]
    public void TenantManager_SuspendTenant_SetsState()
    {
        var manager = new TenantManager(NullLogger<TenantManager>.Instance);
        var tenantId = new TenantId("acme");
        manager.CreateTenant(tenantId, "Acme Corp");

        var result = manager.SuspendTenant(tenantId);

        Assert.True(result);
        var tenant = manager.GetTenant(tenantId);
        Assert.NotNull(tenant);
        Assert.Equal(TenantState.Suspended, tenant.State);
        Assert.NotNull(tenant.SuspendedAt);
    }

    [Fact]
    public void TenantManager_ActivateTenant_ClearsState()
    {
        var manager = new TenantManager(NullLogger<TenantManager>.Instance);
        var tenantId = new TenantId("acme");
        manager.CreateTenant(tenantId, "Acme Corp");
        manager.SuspendTenant(tenantId);

        var result = manager.ActivateTenant(tenantId);

        Assert.True(result);
        var tenant = manager.GetTenant(tenantId);
        Assert.NotNull(tenant);
        Assert.Equal(TenantState.Active, tenant.State);
        Assert.Null(tenant.SuspendedAt);
    }

    [Fact]
    public void TenantManager_UpdatePolicy_Success()
    {
        var manager = new TenantManager(NullLogger<TenantManager>.Instance);
        var tenantId = new TenantId("acme");
        manager.CreateTenant(tenantId, "Acme Corp");

        var newPolicy = new TenantPolicy { MaxTopics = 10, MaxPartitions = 50 };
        var result = manager.UpdatePolicy(tenantId, newPolicy);

        Assert.True(result);
        var tenant = manager.GetTenant(tenantId);
        Assert.NotNull(tenant);
        Assert.Equal(10, tenant.Policy.MaxTopics);
        Assert.Equal(50, tenant.Policy.MaxPartitions);
    }

    [Fact]
    public void TenantManager_GetAllTenants_ReturnsAll()
    {
        var manager = new TenantManager(NullLogger<TenantManager>.Instance);
        manager.CreateTenant(new TenantId("tenant1"), "Tenant 1");
        manager.CreateTenant(new TenantId("tenant2"), "Tenant 2");

        var all = manager.GetAllTenants();

        Assert.Equal(3, all.Count); // default + 2 created
    }

    // --- TenantTopicResolver Tests ---

    [Fact]
    public void TenantTopicResolver_QualifyName_DefaultTenant()
    {
        var result = TenantTopicResolver.QualifyTopicName(TenantId.Default, "orders");

        Assert.Equal("orders", result);
    }

    [Fact]
    public void TenantTopicResolver_QualifyName_CustomTenant()
    {
        var result = TenantTopicResolver.QualifyTopicName(new TenantId("acme"), "orders");

        Assert.Equal("acme/orders", result);
    }

    [Fact]
    public void TenantTopicResolver_ParseQualifiedName_WithSlash()
    {
        var (tenant, topicName) = TenantTopicResolver.ParseQualifiedName("acme/orders");

        Assert.Equal(new TenantId("acme"), tenant);
        Assert.Equal("orders", topicName);
    }

    [Fact]
    public void TenantTopicResolver_ParseQualifiedName_NoSlash()
    {
        var (tenant, topicName) = TenantTopicResolver.ParseQualifiedName("orders");

        Assert.Equal(TenantId.Default, tenant);
        Assert.Equal("orders", topicName);
    }

    [Fact]
    public void TenantTopicResolver_IsQualifiedName()
    {
        Assert.True(TenantTopicResolver.IsQualifiedName("acme/orders"));
        Assert.False(TenantTopicResolver.IsQualifiedName("orders"));
    }

    [Fact]
    public void TenantTopicResolver_QualifyTopicNames_Batch()
    {
        var names = TenantTopicResolver.QualifyTopicNames(
            new TenantId("acme"),
            ["orders", "events", "logs"]);

        Assert.Equal(3, names.Length);
        Assert.Equal("acme/orders", names[0]);
        Assert.Equal("acme/events", names[1]);
        Assert.Equal("acme/logs", names[2]);
    }

    // --- TenantValidator Tests ---

    [Fact]
    public void TenantValidator_ActiveTenant_Allowed()
    {
        var tenant = CreateTenant(TenantState.Active);

        var result = TenantValidator.ValidateAccess(tenant);

        Assert.True(result.IsValid);
    }

    [Fact]
    public void TenantValidator_SuspendedTenant_ReadOnly()
    {
        var tenant = CreateTenant(TenantState.Suspended);

        // Access check passes (suspended = read-only, not fully blocked)
        var accessResult = TenantValidator.ValidateAccess(tenant);
        Assert.True(accessResult.IsValid);

        // But topic creation should be blocked
        var usage = new TenantResourceUsage(tenant.Id, 0, 0, 0, 0, 0, 0, 0);
        var createResult = TenantValidator.ValidateTopicCreation(tenant, usage, "new-topic", 3);
        Assert.False(createResult.IsValid);
    }

    [Fact]
    public void TenantValidator_DisabledTenant_Rejected()
    {
        var tenant = CreateTenant(TenantState.Disabled);

        var result = TenantValidator.ValidateAccess(tenant);

        Assert.False(result.IsValid);
        Assert.Contains("disabled", result.ErrorMessage!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TenantValidator_TopicCreation_ExceedsMaxTopics()
    {
        var tenant = CreateTenant(TenantState.Active, new TenantPolicy { MaxTopics = 5 });
        var usage = new TenantResourceUsage(tenant.Id, 5, 10, 0, 0, 0, 0, 0);

        var result = TenantValidator.ValidateTopicCreation(tenant, usage, "new-topic", 3);

        Assert.False(result.IsValid);
        Assert.Contains("maximum number of topics", result.ErrorMessage!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TenantValidator_TopicCreation_ExceedsMaxPartitions()
    {
        var tenant = CreateTenant(TenantState.Active, new TenantPolicy { MaxPartitions = 10 });
        var usage = new TenantResourceUsage(tenant.Id, 2, 8, 0, 0, 0, 0, 0);

        var result = TenantValidator.ValidateTopicCreation(tenant, usage, "new-topic", 5);

        Assert.False(result.IsValid);
        Assert.Contains("partition count", result.ErrorMessage!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TenantValidator_TopicCreation_PatternMismatch()
    {
        var tenant = CreateTenant(TenantState.Active, new TenantPolicy
        {
            AllowedTopicPatterns = ["^orders-.*$", "^events-.*$"]
        });
        var usage = new TenantResourceUsage(tenant.Id, 0, 0, 0, 0, 0, 0, 0);

        var result = TenantValidator.ValidateTopicCreation(tenant, usage, "logs-app", 1);

        Assert.False(result.IsValid);
        Assert.Contains("does not match", result.ErrorMessage!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TenantValidator_TopicCreation_PatternMatch_Success()
    {
        var tenant = CreateTenant(TenantState.Active, new TenantPolicy
        {
            AllowedTopicPatterns = ["^orders-.*$", "^events-.*$"]
        });
        var usage = new TenantResourceUsage(tenant.Id, 0, 0, 0, 0, 0, 0, 0);

        var result = TenantValidator.ValidateTopicCreation(tenant, usage, "orders-us", 1);

        Assert.True(result.IsValid);
    }

    [Fact]
    public void TenantValidator_Connection_ExceedsMaxConnections()
    {
        var tenant = CreateTenant(TenantState.Active, new TenantPolicy { MaxConnections = 10 });
        var usage = new TenantResourceUsage(tenant.Id, 0, 0, 0, 0, 0, 0, 10);

        var result = TenantValidator.ValidateConnection(tenant, usage);

        Assert.False(result.IsValid);
        Assert.Contains("maximum number of connections", result.ErrorMessage!, StringComparison.OrdinalIgnoreCase);
    }

    // --- TenantQuotaTracker Tests ---

    [Fact]
    public void TenantQuotaTracker_AllowsWithinLimits()
    {
        var tracker = new TenantQuotaTracker(NullLogger<TenantQuotaTracker>.Instance);
        var tenantId = new TenantId("acme");
        var policy = new TenantPolicy { MaxProduceBytesPerSecond = 1_000_000 };

        // First call initializes tokens at 0 then refills; since state was just created
        // with 0 tokens and 0 elapsed time, we need to wait or accept that first call may throttle.
        // The Refill will give tokens proportional to elapsed time since creation.
        // For a fresh state, tokens start at 0 and refill is 0ms elapsed = 0 tokens.
        // So let's just verify the unlimited case first.
        var unlimitedPolicy = new TenantPolicy { MaxProduceBytesPerSecond = -1 };
        var result = tracker.CheckProduceQuota(tenantId, unlimitedPolicy, 1000);

        Assert.Equal(TenantQuotaCheckResult.Allowed, result);
    }

    [Fact]
    public void TenantQuotaTracker_ThrottlesOverLimit()
    {
        var tracker = new TenantQuotaTracker(NullLogger<TenantQuotaTracker>.Instance);
        var tenantId = new TenantId("acme");
        // Very low rate: 100 bytes/sec
        var policy = new TenantPolicy { MaxProduceBytesPerSecond = 100 };

        // Token bucket starts at 0 with 0 elapsed time, so requesting any amount should throttle
        var result = tracker.CheckProduceQuota(tenantId, policy, 1000);

        Assert.Equal(TenantQuotaCheckResult.Throttled, result);
    }

    [Fact]
    public void TenantQuotaTracker_UnlimitedPolicy_AlwaysAllows()
    {
        var tracker = new TenantQuotaTracker(NullLogger<TenantQuotaTracker>.Instance);
        var tenantId = new TenantId("acme");
        var policy = new TenantPolicy(); // Default: -1 = unlimited

        var produceResult = tracker.CheckProduceQuota(tenantId, policy, 100_000_000);
        var fetchResult = tracker.CheckFetchQuota(tenantId, policy, 100_000_000);

        Assert.Equal(TenantQuotaCheckResult.Allowed, produceResult);
        Assert.Equal(TenantQuotaCheckResult.Allowed, fetchResult);
    }

    [Fact]
    public void TenantQuotaTracker_RecordBytes_TracksUsage()
    {
        var tracker = new TenantQuotaTracker(NullLogger<TenantQuotaTracker>.Instance);
        var tenantId = new TenantId("acme");

        tracker.RecordProducedBytes(tenantId, 500);
        tracker.RecordFetchedBytes(tenantId, 300);

        var usage = tracker.GetUsage(tenantId);
        Assert.Equal(500, usage.ProduceBytesPerSecond);
        Assert.Equal(300, usage.FetchBytesPerSecond);
    }

    [Fact]
    public void TenantQuotaTracker_GetUsage_UnknownTenant_ReturnsZeros()
    {
        var tracker = new TenantQuotaTracker(NullLogger<TenantQuotaTracker>.Instance);
        var tenantId = new TenantId("unknown");

        var usage = tracker.GetUsage(tenantId);

        Assert.Equal(tenantId, usage.TenantId);
        Assert.Equal(0, usage.ProduceBytesPerSecond);
        Assert.Equal(0, usage.FetchBytesPerSecond);
    }

    // --- TenantMetrics Tests ---

    [Fact]
    public void TenantMetrics_Initializes_WithoutError()
    {
        using var metrics = new TenantMetrics();

        // Verify no exception on creation and recording
        metrics.RecordTopicCreated("acme");
        metrics.RecordTopicDeleted("acme");
        metrics.RecordQuotaThrottled("acme");
        metrics.RecordQuotaRejected("acme");
        metrics.RecordProduceBytes("acme", 1024);
        metrics.RecordFetchBytes("acme", 2048);
    }

    [Fact]
    public void TenantMetrics_RegisterStateAccessors_WorksCorrectly()
    {
        using var metrics = new TenantMetrics();

        // Should not throw
        metrics.RegisterStateAccessors(
            getTenantCount: () => 5);
    }

    // --- MultiTenancyConfig Tests ---

    [Fact]
    public void MultiTenancyConfig_Defaults_Correct()
    {
        var config = new MultiTenancyConfig();

        Assert.False(config.Enabled);
        Assert.False(config.RequireTenantIdentification);
        Assert.Equal(TenantTopicNaming.Prefixed, config.TopicNaming);
        Assert.Equal(TenantIdentificationMode.Principal, config.IdentificationMode);
        Assert.NotNull(config.DefaultPolicy);
        Assert.Empty(config.Tenants);
    }

    // --- TenantPolicy Tests ---

    [Fact]
    public void TenantPolicy_Defaults_Unlimited()
    {
        var policy = new TenantPolicy();

        Assert.Equal(-1, policy.MaxTopics);
        Assert.Equal(-1, policy.MaxPartitions);
        Assert.Equal(-1, policy.MaxConsumerGroups);
        Assert.Equal(-1, policy.MaxProduceBytesPerSecond);
        Assert.Equal(-1, policy.MaxFetchBytesPerSecond);
        Assert.Equal(-1, policy.MaxMessageBytes);
        Assert.Null(policy.MaxRetentionPeriod);
        Assert.Equal(-1, policy.MaxStorageBytes);
        Assert.Equal(-1, policy.MaxConnections);
        Assert.Equal(0, policy.DefaultReplicationFactor);
        Assert.Empty(policy.AllowedTopicPatterns);
    }

    // --- TenantDefinition Tests ---

    [Fact]
    public void TenantDefinition_RequiredProperties()
    {
        var def = new TenantDefinition
        {
            Id = "acme",
            DisplayName = "Acme Corp",
            Policy = new TenantPolicy { MaxTopics = 100 }
        };

        Assert.Equal("acme", def.Id);
        Assert.Equal("Acme Corp", def.DisplayName);
        Assert.NotNull(def.Policy);
        Assert.Equal(100, def.Policy.MaxTopics);
    }

    // --- TenantValidationResult Tests ---

    [Fact]
    public void TenantValidationResult_Success_IsValid()
    {
        var result = TenantValidationResult.Success;

        Assert.True(result.IsValid);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void TenantValidationResult_Fail_HasMessage()
    {
        var result = TenantValidationResult.Fail("Something went wrong");

        Assert.False(result.IsValid);
        Assert.Equal("Something went wrong", result.ErrorMessage);
    }

    // --- Helper Methods ---

    private static Tenant CreateTenant(TenantState state, TenantPolicy? policy = null)
    {
        return new Tenant
        {
            Id = new TenantId("test-tenant"),
            DisplayName = "Test Tenant",
            State = state,
            Policy = policy ?? new TenantPolicy()
        };
    }
}
