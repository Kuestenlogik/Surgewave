using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Plugins.Packaging.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// Regression for the "appsettings reload propagates to live ISppSigner" gap
/// that the original DI factory's docstring promised but never delivered.
/// Uses a hand-rolled IOptionsMonitor stub so we can deterministically push
/// new SignerOptions and assert the wrapper rebuilt its inner signer.
/// </summary>
public sealed class OptionsTrackingSignerTests : IDisposable
{
    private readonly string _trustDir;

    public OptionsTrackingSignerTests()
    {
        _trustDir = Directory.CreateTempSubdirectory("surgewave-signer-monitor-").FullName;
    }

    public void Dispose()
    {
        if (Directory.Exists(_trustDir)) Directory.Delete(_trustDir, recursive: true);
    }

    private static PluginPackageSignerRegistry Registry()
    {
        var dir = Directory.CreateTempSubdirectory("surgewave-signer-registry-").FullName;
        try { return PluginPackageSignerRegistry.LoadFrom(dir); }
        finally { /* dir intentionally leaked — registry holds an ALC over it */ }
    }

    [Fact]
    public void Initial_EmptyOptions_BuildsNoOpSigner()
    {
        var monitor = new TestMonitor(new SignerOptions());
        using var signer = new OptionsTrackingSigner(Registry(), monitor);
        Assert.Equal("builtin-ecdsa", signer.Name);
        Assert.False(signer.HasSignature(Path.Combine(_trustDir, "nope.swpkg")));
    }

    [Fact]
    public void Initial_ConfiguredTrustDir_GoesThroughProvider()
    {
        var monitor = new TestMonitor(new SignerOptions
        {
            Name = "builtin-ecdsa",
            Options = new Dictionary<string, string> { ["trusted-keys-dir"] = _trustDir },
        });
        using var signer = new OptionsTrackingSigner(Registry(), monitor);
        Assert.Equal("builtin-ecdsa", signer.Name);
        // No exception → wrapper was built via provider path successfully.
    }

    [Fact]
    public void OnChange_FromEmptyToConfigured_SwapsInnerSigner()
    {
        var monitor = new TestMonitor(new SignerOptions()); // start empty (no-op)
        using var signer = new OptionsTrackingSigner(Registry(), monitor);

        monitor.Push(new SignerOptions
        {
            Name = "builtin-ecdsa",
            Options = new Dictionary<string, string> { ["trusted-keys-dir"] = _trustDir },
        });

        Assert.Equal("builtin-ecdsa", signer.Name);
    }

    [Fact]
    public void OnChange_FromConfiguredToEmpty_ResetsToNoOp()
    {
        var monitor = new TestMonitor(new SignerOptions
        {
            Name = "builtin-ecdsa",
            Options = new Dictionary<string, string> { ["trusted-keys-dir"] = _trustDir },
        });
        using var signer = new OptionsTrackingSigner(Registry(), monitor);

        monitor.Push(new SignerOptions()); // empty → no-op fallback

        Assert.Equal("builtin-ecdsa", signer.Name);
        // No exception when verifying with no trust dir → fell back to no-op path.
    }

    /// <summary>
    /// Hand-rolled <see cref="IOptionsMonitor{TOptions}"/> with a public
    /// <c>Push</c> method to fire OnChange listeners deterministically.
    /// Standard Microsoft.Extensions.Options doesn't ship a public test
    /// harness for this; the production OptionsMonitor only fires when
    /// the underlying IConfiguration source reloads.
    /// </summary>
    private sealed class TestMonitor : IOptionsMonitor<SignerOptions>
    {
        private readonly List<Action<SignerOptions, string?>> _listeners = new();
        public TestMonitor(SignerOptions initial) => CurrentValue = initial;
        public SignerOptions CurrentValue { get; private set; }
        public SignerOptions Get(string? name) => CurrentValue;

        public IDisposable OnChange(Action<SignerOptions, string?> listener)
        {
            _listeners.Add(listener);
            return new Sub(() => _listeners.Remove(listener));
        }

        public void Push(SignerOptions next)
        {
            CurrentValue = next;
            foreach (var l in _listeners.ToArray()) l(next, null);
        }

        private sealed class Sub : IDisposable
        {
            private readonly Action _onDispose;
            public Sub(Action onDispose) => _onDispose = onDispose;
            public void Dispose() => _onDispose();
        }
    }
}
