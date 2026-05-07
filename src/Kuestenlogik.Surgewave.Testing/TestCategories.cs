namespace Kuestenlogik.Surgewave.Testing;

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

    /// <summary>
    /// On-demand diagnostic tests — never run in CI. Use to capture wire traces or
    /// reproduce environment-sensitive bugs without affecting the green-test bar.
    /// </summary>
    public const string Diagnostic = "Diagnostic";
}
