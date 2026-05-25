namespace Kuestenlogik.Surgewave.Broker.Tenancy;

public sealed record TenantValidationResult(bool IsValid, string? ErrorMessage = null)
{
    public static readonly TenantValidationResult Success = new(true);
    public static TenantValidationResult Fail(string message) => new(false, message);
}
