using System.CommandLine;
using System.CommandLine.Parsing;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

public class TrustCommand : CommandBase
{
    private readonly Argument<string> _keyArg = new("keyfile") { Description = "Path to public key file (.pub)" };
    private readonly Option<string> _dirOpt = new("--plugins-dir", "-d")
    {
        Description = "Plugins directory",
        DefaultValueFactory = _ => "plugins"
    };

    public TrustCommand() : base("trust", "Add a publisher's public key to the trust store")
    {
        Arguments.Add(_keyArg);
        Options.Add(_dirOpt);
        this.SetAction(ExecuteAsync);
    }

    private Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var keyFile = parseResult.GetValue(_keyArg)!;
        var pluginsDir = parseResult.GetValue(_dirOpt) ?? "plugins";

        if (!File.Exists(keyFile))
        {
            WriteError($"Key file not found: {keyFile}");
            return Task.FromResult(1);
        }

        var trustedDir = Path.Combine(pluginsDir, "trusted-keys");
        Directory.CreateDirectory(trustedDir);

        var destPath = Path.Combine(trustedDir, Path.GetFileName(keyFile));
        File.Copy(keyFile, destPath, overwrite: true);

        WriteSuccess($"Trusted: {Path.GetFileNameWithoutExtension(keyFile)}");
        WriteLine($"  Key stored at: {destPath}");
        return Task.FromResult(0);
    }
}
