namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Protocol-neutral seam over the dynamic broker configuration store (runtime config
/// overrides via AlterConfigs). Implemented by the broker's <c>DynamicBrokerConfig</c>.
/// </summary>
public interface IDynamicBrokerConfig
{
    /// <summary>
    /// Get the effective value for a config, checking dynamic overrides first.
    /// </summary>
    string? GetConfig(string name);

    /// <summary>
    /// Set a dynamic config value. Returns error message if config is read-only or invalid.
    /// </summary>
    string? SetConfig(string name, string? value);

    /// <summary>
    /// Check if a config has been dynamically overridden.
    /// </summary>
    bool IsDynamicallySet(string name);

    /// <summary>
    /// Broker configs that can be modified at runtime (changes take effect without restart).
    /// </summary>
    IReadOnlySet<string> DynamicConfigKeys { get; }

    /// <summary>
    /// Read-only broker configs that require restart to change.
    /// </summary>
    IReadOnlySet<string> ReadOnlyConfigKeys { get; }
}
