using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Tenancy;

/// <summary>
/// Thread-safe tenant CRUD manager. Pre-populates with the default tenant.
/// </summary>
public sealed partial class TenantManager
{
    private readonly ConcurrentDictionary<TenantId, Tenant> _tenants = new();
    private readonly ILogger<TenantManager> _logger;

    public TenantManager(ILogger<TenantManager> logger)
    {
        _logger = logger;

        // Pre-populate with default tenant
        _tenants[TenantId.Default] = new Tenant
        {
            Id = TenantId.Default,
            DisplayName = "Default Tenant"
        };

        LogDefaultTenantCreated();
    }

    /// <summary>
    /// Creates a new tenant. Throws <see cref="InvalidOperationException"/> if a tenant with the same ID already exists.
    /// </summary>
    public Tenant CreateTenant(TenantId id, string displayName, TenantPolicy? policy = null)
    {
        if (!TenantId.IsValid(id.Value))
            throw new ArgumentException($"Invalid tenant ID: '{id.Value}'", nameof(id));

        var tenant = new Tenant
        {
            Id = id,
            DisplayName = displayName,
            Policy = policy ?? new TenantPolicy()
        };

        if (!_tenants.TryAdd(id, tenant))
            throw new InvalidOperationException($"Tenant '{id}' already exists.");

        LogTenantCreated(id.Value, displayName);
        return tenant;
    }

    /// <summary>
    /// Gets a tenant by ID, or null if not found.
    /// </summary>
    public Tenant? GetTenant(TenantId id) =>
        _tenants.TryGetValue(id, out var tenant) ? tenant : null;

    /// <summary>
    /// Returns all tenants as a read-only list.
    /// </summary>
    public IReadOnlyList<Tenant> GetAllTenants() =>
        _tenants.Values.ToList().AsReadOnly();

    /// <summary>
    /// Updates the policy for a tenant. Returns false if tenant not found.
    /// </summary>
    public bool UpdatePolicy(TenantId id, TenantPolicy policy)
    {
        if (!_tenants.TryGetValue(id, out var tenant))
            return false;

        tenant.Policy = policy;
        LogPolicyUpdated(id.Value);
        return true;
    }

    /// <summary>
    /// Suspends a tenant. Sets state to Suspended and records timestamp.
    /// Returns false if tenant not found.
    /// </summary>
    public bool SuspendTenant(TenantId id)
    {
        if (!_tenants.TryGetValue(id, out var tenant))
            return false;

        tenant.State = TenantState.Suspended;
        tenant.SuspendedAt = DateTime.UtcNow;
        LogTenantSuspended(id.Value);
        return true;
    }

    /// <summary>
    /// Activates a tenant. Sets state to Active and clears suspended timestamp.
    /// Returns false if tenant not found.
    /// </summary>
    public bool ActivateTenant(TenantId id)
    {
        if (!_tenants.TryGetValue(id, out var tenant))
            return false;

        tenant.State = TenantState.Active;
        tenant.SuspendedAt = null;
        LogTenantActivated(id.Value);
        return true;
    }

    /// <summary>
    /// Disables a tenant. Sets state to Disabled.
    /// Returns false if tenant not found.
    /// </summary>
    public bool DisableTenant(TenantId id)
    {
        if (!_tenants.TryGetValue(id, out var tenant))
            return false;

        tenant.State = TenantState.Disabled;
        LogTenantDisabled(id.Value);
        return true;
    }

    /// <summary>
    /// Deletes a tenant. Cannot delete the default tenant.
    /// Returns false if tenant not found or is default.
    /// </summary>
    public bool DeleteTenant(TenantId id)
    {
        if (id.IsDefault)
        {
            LogCannotDeleteDefault();
            return false;
        }

        if (_tenants.TryRemove(id, out _))
        {
            LogTenantDeleted(id.Value);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Checks if a tenant exists.
    /// </summary>
    public bool TenantExists(TenantId id) => _tenants.ContainsKey(id);

    /// <summary>
    /// Gets the current tenant count.
    /// </summary>
    public int TenantCount => _tenants.Count;

    [LoggerMessage(Level = LogLevel.Information, Message = "Default tenant created")]
    private partial void LogDefaultTenantCreated();

    [LoggerMessage(Level = LogLevel.Information, Message = "Tenant '{TenantId}' created: {DisplayName}")]
    private partial void LogTenantCreated(string tenantId, string displayName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Policy updated for tenant '{TenantId}'")]
    private partial void LogPolicyUpdated(string tenantId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Tenant '{TenantId}' suspended")]
    private partial void LogTenantSuspended(string tenantId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Tenant '{TenantId}' activated")]
    private partial void LogTenantActivated(string tenantId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Tenant '{TenantId}' disabled")]
    private partial void LogTenantDisabled(string tenantId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Cannot delete default tenant")]
    private partial void LogCannotDeleteDefault();

    [LoggerMessage(Level = LogLevel.Information, Message = "Tenant '{TenantId}' deleted")]
    private partial void LogTenantDeleted(string tenantId);
}
