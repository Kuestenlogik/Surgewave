namespace Kuestenlogik.Surgewave.Tests.Helpers;

/// <summary>
/// Constants for test category traits to enable filtering tests by type.
/// Usage: [Trait("Category", TestCategories.Unit)]
/// Run specific categories: dotnet test --filter "Category=Unit"
/// </summary>
public static class TestCategories
{
    public const string Unit = "Unit";
    public const string Integration = "Integration";
    public const string Performance = "Performance";
    public const string Compatibility = "Compatibility";
    public const string Slow = "Slow";
}
