namespace Kuestenlogik.Surgewave.Broker.Tenancy;

public sealed class MultiTenancyConfig
{
    /// <summary>Enable multi-tenancy. When disabled, all operations use default tenant.</summary>
    public bool Enabled { get; init; }

    /// <summary>Require tenant identification for all requests. If false, unidentified requests use default tenant.</summary>
    public bool RequireTenantIdentification { get; init; }

    /// <summary>Topic naming strategy.</summary>
    public TenantTopicNaming TopicNaming { get; init; } = TenantTopicNaming.Prefixed;

    /// <summary>How tenant is identified from client requests.</summary>
    public TenantIdentificationMode IdentificationMode { get; init; } = TenantIdentificationMode.Principal;

    /// <summary>Default policy for new tenants.</summary>
    public TenantPolicy DefaultPolicy { get; init; } = new();

    /// <summary>Pre-configured tenants loaded at startup.</summary>
    public List<TenantDefinition> Tenants { get; init; } = [];
}

public enum TenantTopicNaming
{
    /// <summary>Topics prefixed with tenant: "tenantId/topicName"</summary>
    Prefixed,
    /// <summary>Topics use flat names but are isolated by ACL only</summary>
    Flat
}

public enum TenantIdentificationMode
{
    /// <summary>Derive tenant from SASL principal (e.g., "User:tenant1_alice" -> tenant1)</summary>
    Principal,
    /// <summary>Client sends tenant ID via client.id prefix (e.g., "tenant1.myapp")</summary>
    ClientId,
    /// <summary>Custom header-based identification</summary>
    Header
}
