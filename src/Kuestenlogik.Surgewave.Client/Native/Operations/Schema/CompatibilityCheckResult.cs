namespace Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

/// <summary>
/// Result of compatibility check.
/// </summary>
public record CompatibilityCheckResult(bool IsCompatible, IReadOnlyList<string> Messages);
