namespace Kuestenlogik.Surgewave.Broker.Tenancy;

public sealed class Tenant
{
    public required TenantId Id { get; init; }
    public required string DisplayName { get; init; }
    public TenantState State { get; set; } = TenantState.Active;
    public TenantPolicy Policy { get; set; } = new();
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;
    public DateTime? SuspendedAt { get; set; }
    public Dictionary<string, string> Metadata { get; init; } = [];
}
