namespace Kuestenlogik.Surgewave.Broker.Tenancy;

public enum TenantState
{
    Active,
    Suspended,   // Can read but not write
    Disabled     // No access at all
}
