namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Overview info for a single cluster in multi-cluster view.
/// </summary>
public sealed class ClusterOverviewInfo
{
    public string ClusterId { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public string Status { get; set; } = "Unknown";
    public int BrokerCount { get; set; }
    public int TopicCount { get; set; }
    public int PartitionCount { get; set; }
    public int ConsumerGroupCount { get; set; }
    public int ControllerId { get; set; }
    public bool RaftEnabled { get; set; }
    public string? RaftState { get; set; }
    public double ThroughputMessagesPerSec { get; set; }
    public double ThroughputBytesPerSec { get; set; }
    public DateTimeOffset LastChecked { get; set; }
}

/// <summary>
/// Broker configuration entry with source and default tracking.
/// </summary>
public sealed class BrokerConfigEntry
{
    public string Key { get; set; } = "";
    public string Value { get; set; } = "";
    public string? DefaultValue { get; set; }
    public ConfigSource Source { get; set; } = ConfigSource.Default;
    public bool IsReadOnly { get; set; }
    public bool IsSensitive { get; set; }
    public string? Documentation { get; set; }
    public string Category { get; set; } = "General";
    public bool IsModified => Source != ConfigSource.Default && Value != DefaultValue;
}

public enum ConfigSource
{
    Default,
    StaticBroker,
    DynamicBroker,
    DynamicCluster,
    DynamicTopic
}

/// <summary>
/// Role definition for UI-level role management.
/// </summary>
public sealed class RoleDefinition
{
    public string Name { get; set; } = "";
    public string? Description { get; set; }
    public List<string> Permissions { get; set; } = [];
    public List<string> Members { get; set; } = [];
    public bool IsBuiltIn { get; set; }
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Effective permission result for a user.
/// </summary>
public sealed class EffectivePermission
{
    public string Resource { get; set; } = "";
    public string ResourceType { get; set; } = "";
    public List<string> AllowedOperations { get; set; } = [];
    public List<string> DeniedOperations { get; set; } = [];
    public string GrantedBy { get; set; } = "";
}

/// <summary>
/// Role management state persisted in LocalStorage.
/// </summary>
public sealed class RoleManagementState
{
    public List<RoleDefinition> Roles { get; set; } = [];
    public Dictionary<string, List<string>> UserRoles { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}
