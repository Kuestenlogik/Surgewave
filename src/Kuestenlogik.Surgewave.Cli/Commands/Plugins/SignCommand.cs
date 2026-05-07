using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

public class SignCommand : CommandBase
{
    private readonly Argument<string> _packageArg = new("package") { Description = "Path to .swpkg package" };
    private readonly Option<string> _signerOpt = new("--signer", "-s")
    {
        Description = "Signer provider name (default: builtin-ecdsa). Additional providers are discovered under --plugins-dir.",
        DefaultValueFactory = _ => "builtin-ecdsa"
    };
    private readonly Option<string> _pluginsDirOpt = new("--plugins-dir", "-d")
    {
        Description = "Plugins directory scanned for signer providers",
        DefaultValueFactory = _ => "plugins"
    };
    private readonly Option<string?> _keyOpt = new("--key", "-k")
    {
        Description = "builtin-ecdsa: private key file path"
    };
    private readonly Option<string?> _certOpt = new("--cert")
    {
        Description = "charter: PFX/P12 signing certificate path"
    };
    private readonly Option<string?> _certPasswordOpt = new("--cert-password")
    {
        Description = "charter: password for the PFX certificate"
    };

    public SignCommand() : base("sign", "Sign an .swpkg plugin package")
    {
        Arguments.Add(_packageArg);
        Options.Add(_signerOpt);
        Options.Add(_pluginsDirOpt);
        Options.Add(_keyOpt);
        Options.Add(_certOpt);
        Options.Add(_certPasswordOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var packagePath = parseResult.GetValue(_packageArg)!;
        var signerName = parseResult.GetValue(_signerOpt) ?? "builtin-ecdsa";
        var pluginsDir = parseResult.GetValue(_pluginsDirOpt) ?? "plugins";

        if (!File.Exists(packagePath))
        {
            WriteError($"Package not found: {packagePath}");
            return 1;
        }

        var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (parseResult.GetValue(_keyOpt) is { Length: > 0 } key) options["private-key"] = key;
        if (parseResult.GetValue(_certOpt) is { Length: > 0 } cert) options["cert-path"] = cert;
        if (parseResult.GetValue(_certPasswordOpt) is { Length: > 0 } pw) options["cert-password"] = pw;

        using var registry = PluginPackageSignerRegistry.LoadFrom(pluginsDir);
        ISppSignerProvider provider;
        try
        {
            provider = registry.GetProvider(signerName);
        }
        catch (KeyNotFoundException ex)
        {
            WriteError(ex.Message);
            return 1;
        }

        try
        {
            var signer = provider.Create(options);
            var sigPath = await signer.SignAsync(packagePath, ct);
            WriteSuccess($"Package signed by {provider.Name}: {sigPath}");
            return 0;
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or FileNotFoundException)
        {
            WriteError($"Signing failed: {ex.Message}");
            return 1;
        }
    }
}
