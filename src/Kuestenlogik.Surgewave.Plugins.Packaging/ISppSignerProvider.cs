namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Factory over <see cref="ISppSigner"/>. Each concrete signer ships alongside a provider so
/// the registry can instantiate it from a flat options dictionary without knowing the
/// signer's specific configuration shape. This is the entry point consumed by
/// <see cref="PluginPackageSignerRegistry"/> during discovery.
/// </summary>
/// <remarks>
/// <para>
/// A provider declares its name (matched against <c>--signer</c> on the CLI or
/// <c>Surgewave:Plugins:Signer:Name</c> in config) and knows how to translate a
/// <see cref="IReadOnlyDictionary{TKey, TValue}"/> of string options into a configured
/// <see cref="ISppSigner"/>.
/// </para>
/// <para>
/// Providers MUST be parameterless-constructible so the registry can activate them via
/// <c>Activator.CreateInstance</c> after discovering the type in a loaded assembly.
/// </para>
/// </remarks>
public interface ISppSignerProvider
{
    /// <summary>Short stable identifier, e.g. <c>builtin-ecdsa</c> or <c>sealbolt</c>.</summary>
    string Name { get; }

    /// <summary>
    /// Builds an <see cref="ISppSigner"/> from a flat option bag. Missing or extra options
    /// are handled by the provider — throw <see cref="ArgumentException"/> for required values
    /// that are absent or malformed.
    /// </summary>
    ISppSigner Create(IReadOnlyDictionary<string, string> options);
}
