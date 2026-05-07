using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Broker.Tenancy;

/// <summary>
/// Validates tenant operations against policies.
/// </summary>
public static class TenantValidator
{
    private static readonly TimeSpan RegexTimeout = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Validates whether a tenant is allowed to create a topic with the given name and partition count.
    /// Checks MaxTopics, MaxPartitions, and AllowedTopicPatterns.
    /// </summary>
    public static TenantValidationResult ValidateTopicCreation(
        Tenant tenant, TenantResourceUsage usage, string topicName, int partitions)
    {
        var accessResult = ValidateAccess(tenant);
        if (!accessResult.IsValid)
            return accessResult;

        if (tenant.State == TenantState.Suspended)
            return TenantValidationResult.Fail($"Tenant '{tenant.Id}' is suspended and cannot create topics.");

        var policy = tenant.Policy;

        // Check max topics
        if (policy.MaxTopics >= 0 && usage.TopicCount >= policy.MaxTopics)
            return TenantValidationResult.Fail(
                $"Tenant '{tenant.Id}' has reached the maximum number of topics ({policy.MaxTopics}).");

        // Check max partitions
        if (policy.MaxPartitions >= 0 && usage.PartitionCount + partitions > policy.MaxPartitions)
            return TenantValidationResult.Fail(
                $"Tenant '{tenant.Id}' would exceed the maximum partition count ({policy.MaxPartitions}). " +
                $"Current: {usage.PartitionCount}, Requested: {partitions}.");

        // Check allowed topic patterns
        if (policy.AllowedTopicPatterns.Count > 0)
        {
            var matchesPattern = false;
            foreach (var pattern in policy.AllowedTopicPatterns)
            {
                try
                {
                    if (Regex.IsMatch(topicName, pattern, RegexOptions.None, RegexTimeout))
                    {
                        matchesPattern = true;
                        break;
                    }
                }
                catch (RegexMatchTimeoutException)
                {
                    // Skip timed-out pattern
                }
            }

            if (!matchesPattern)
                return TenantValidationResult.Fail(
                    $"Topic name '{topicName}' does not match any allowed patterns for tenant '{tenant.Id}'.");
        }

        return TenantValidationResult.Success;
    }

    /// <summary>
    /// Validates whether a tenant is allowed to access resources based on its state.
    /// Active = full access, Suspended = read-only, Disabled = no access.
    /// </summary>
    public static TenantValidationResult ValidateAccess(Tenant tenant)
    {
        return tenant.State switch
        {
            TenantState.Active => TenantValidationResult.Success,
            TenantState.Suspended => TenantValidationResult.Success, // Read-only, caller checks write ops
            TenantState.Disabled => TenantValidationResult.Fail($"Tenant '{tenant.Id}' is disabled."),
            _ => TenantValidationResult.Fail($"Tenant '{tenant.Id}' has unknown state '{tenant.State}'.")
        };
    }

    /// <summary>
    /// Validates whether a tenant can accept a new connection based on MaxConnections policy.
    /// </summary>
    public static TenantValidationResult ValidateConnection(Tenant tenant, TenantResourceUsage usage)
    {
        var accessResult = ValidateAccess(tenant);
        if (!accessResult.IsValid)
            return accessResult;

        var policy = tenant.Policy;

        if (policy.MaxConnections >= 0 && usage.ActiveConnections >= policy.MaxConnections)
            return TenantValidationResult.Fail(
                $"Tenant '{tenant.Id}' has reached the maximum number of connections ({policy.MaxConnections}).");

        return TenantValidationResult.Success;
    }
}
