using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

public class VerifyCommand : CommandBase
{
    private readonly Argument<string> _packageArg = new("package") { Description = "Path to .swpkg package" };
    private readonly Option<string> _signerOpt = new("--signer", "-s")
    {
        Description = "Signer provider name (default: builtin-ecdsa). Additional providers are discovered under --plugins-dir.",
        DefaultValueFactory = _ => "builtin-ecdsa"
    };
    private readonly Option<string> _pluginsDirOpt = new("--plugins-dir", "-d")
    {
        Description = "Plugins directory scanned for signer providers. For builtin-ecdsa, trusted-keys/ is resolved relative to this.",
        DefaultValueFactory = _ => "plugins"
    };
    private readonly Option<string?> _rootsOpt = new("--roots")
    {
        Description = "sealbolt: comma- or semicolon-separated trusted root certificate paths"
    };
    private readonly Option<bool?> _revocationOpt = new("--require-revocation-check")
    {
        Description = "sealbolt: require OCSP/CRL revocation checks (default true)"
    };

    public VerifyCommand() : base("verify", "Verify the signature of an .swpkg package")
    {
        Arguments.Add(_packageArg);
        Options.Add(_signerOpt);
        Options.Add(_pluginsDirOpt);
        Options.Add(_rootsOpt);
        Options.Add(_revocationOpt);
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

        var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            // builtin-ecdsa convention: trusted keys live under {pluginsDir}/trusted-keys
            ["trusted-keys-dir"] = Path.Combine(pluginsDir, "trusted-keys")
        };
        if (parseResult.GetValue(_rootsOpt) is { Length: > 0 } roots) options["roots"] = roots;
        if (parseResult.GetValue(_revocationOpt) is bool rev) options["require-revocation-check"] = rev ? "true" : "false";

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

        ISppSigner signer;
        try
        {
            signer = provider.Create(options);
        }
        catch (ArgumentException ex)
        {
            WriteError($"Invalid signer configuration: {ex.Message}");
            return 1;
        }

        if (!signer.HasSignature(packagePath))
        {
            WriteWarning($"Package is unsigned (no {provider.Name} signature found).");
            return 1;
        }

        var result = await signer.VerifyAsync(packagePath, ct);
        if (result.IsValid)
        {
            WriteSuccess($"Signature verified by {provider.Name} (signed by: {result.SignerIdentity})");
            return 0;
        }

        WriteError($"Signature invalid ({provider.Name}): {result.Reason}");
        return 1;
    }
}
