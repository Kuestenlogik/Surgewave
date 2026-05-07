namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// <see cref="ISppSignerProvider"/> for <see cref="BuiltinEcdsaSigner"/>. Always registered —
/// no discovery needed because it lives in the same assembly as <see cref="PluginPackageSignerRegistry"/>.
/// </summary>
/// <remarks>
/// Options:
/// <list type="bullet">
/// <item><c>private-key</c> — path to the signing key (EC PRIVATE KEY PEM). Required for signing.</item>
/// <item><c>trusted-keys-dir</c> — directory of <c>*.pub</c> files. Required for verification.</item>
/// </list>
/// At least one option must be set — passing neither is a misconfiguration and throws.
/// </remarks>
public sealed class BuiltinEcdsaSignerProvider : ISppSignerProvider
{
    public string Name => "builtin-ecdsa";

    public ISppSigner Create(IReadOnlyDictionary<string, string> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.TryGetValue("private-key", out var privateKeyPath);
        options.TryGetValue("trusted-keys-dir", out var trustedKeysDir);

        return new BuiltinEcdsaSigner(privateKeyPath, trustedKeysDir);
    }
}
