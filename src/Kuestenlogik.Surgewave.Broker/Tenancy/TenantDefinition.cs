namespace Kuestenlogik.Surgewave.Broker.Tenancy;

public sealed class TenantDefinition
{
    public required string Id { get; init; }
    public required string DisplayName { get; init; }
    public TenantPolicy? Policy { get; init; }
}
