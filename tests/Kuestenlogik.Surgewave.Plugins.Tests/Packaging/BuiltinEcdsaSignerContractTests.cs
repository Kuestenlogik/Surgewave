using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Plugins.Packaging.Testing;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// Confirms <see cref="BuiltinEcdsaSigner"/> satisfies the shared <see cref="PluginPackageSignerContract"/>.
/// A per-class temp directory holds the generated key pair so every test in the fixture sees a
/// consistent signer/verifier pair rooted at the same trust store.
/// </summary>
public sealed class BuiltinEcdsaSignerContractTests : PluginPackageSignerContract, IClassFixture<BuiltinEcdsaSignerContractTests.Fixture>
{
    private readonly Fixture _fx;

    public BuiltinEcdsaSignerContractTests(Fixture fx) => _fx = fx;

    protected override string ProviderName => "builtin-ecdsa";

    protected override ISppSigner CreateSigner()
        => new BuiltinEcdsaSigner(privateKeyPath: _fx.PrivateKeyPath);

    protected override ISppSigner CreateVerifier()
        => new BuiltinEcdsaSigner(trustedKeysDir: _fx.TrustDir);

    public sealed class Fixture : IDisposable
    {
        public string RootDir { get; }
        public string PrivateKeyPath { get; }
        public string TrustDir { get; }

        public Fixture()
        {
            RootDir = Path.Combine(Path.GetTempPath(), "builtin-contract-" + Guid.NewGuid().ToString("N")[..8]);
            Directory.CreateDirectory(RootDir);
            TrustDir = Path.Combine(RootDir, "trusted-keys");
            Directory.CreateDirectory(TrustDir);

            var (keyPath, pubPath) = BuiltinEcdsaSigner.GenerateKeyPair(RootDir, "contract");
            PrivateKeyPath = keyPath;
            File.Copy(pubPath, Path.Combine(TrustDir, Path.GetFileName(pubPath)));
        }

        public void Dispose()
        {
            if (Directory.Exists(RootDir))
                Directory.Delete(RootDir, recursive: true);
        }
    }
}
