using Kuestenlogik.Surgewave.Plugins.Packaging;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Plugins.Packaging.Hosting;

/// <summary>
/// <see cref="ISppSigner"/> wrapper that rebuilds the underlying concrete signer
/// whenever <see cref="SignerOptions"/> changes via <see cref="IOptionsMonitor{TOptions}"/>.
/// Closes the gap the original DI factory's doc comment promised but the implementation
/// (snapshot <see cref="IOptions{TOptions}"/>) never delivered: appsettings reloads now
/// propagate to the live verifier instance without restarting the host. The instance
/// itself stays a singleton — only the inner signer swaps — so callers caching the
/// <see cref="ISppSigner"/> reference keep working.
/// </summary>
public sealed class OptionsTrackingSigner : ISppSigner, IDisposable
{
    private readonly PluginPackageSignerRegistry _registry;
    private readonly IDisposable? _subscription;
    private volatile ISppSigner _current;

    public OptionsTrackingSigner(PluginPackageSignerRegistry registry, IOptionsMonitor<SignerOptions> monitor)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(monitor);
        _registry = registry;
        _current = Build(monitor.CurrentValue);
        // OnChange returns a subscription token; disposing it on host shutdown
        // prevents the lambda from outliving the service provider.
        _subscription = monitor.OnChange(opts => _current = Build(opts));
    }

    public string Name => _current.Name;

    public bool HasSignature(string packagePath) => _current.HasSignature(packagePath);

    public Task<string> SignAsync(string packagePath, CancellationToken ct = default)
        => _current.SignAsync(packagePath, ct);

    public Task<SignatureVerification> VerifyAsync(string packagePath, CancellationToken ct = default)
        => _current.VerifyAsync(packagePath, ct);

    public void Dispose()
    {
        _subscription?.Dispose();
        (_current as IDisposable)?.Dispose();
    }

    private ISppSigner Build(SignerOptions options)
    {
        // Same logic as the original inline factory: empty Options dict means
        // "operator hasn't configured a signer" → no-op BuiltinEcdsaSigner.
        if (options.Options.Count == 0) return new BuiltinEcdsaSigner();
        var provider = _registry.GetProvider(options.Name);
        return provider.Create(options.Options);
    }
}
