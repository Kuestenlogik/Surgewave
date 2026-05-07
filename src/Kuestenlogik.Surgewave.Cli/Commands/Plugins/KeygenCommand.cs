using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

public class KeygenCommand : CommandBase
{
    private readonly Argument<string> _nameArg = new("name") { Description = "Key pair name (e.g. publisher name)" };
    private readonly Option<string> _outputOpt = new("--output", "-o")
    {
        Description = "Output directory",
        DefaultValueFactory = _ => "."
    };

    public KeygenCommand() : base("keygen", "Generate a signing key pair for plugin packages")
    {
        Arguments.Add(_nameArg);
        Options.Add(_outputOpt);
        this.SetAction(ExecuteAsync);
    }

    private Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var name = parseResult.GetValue(_nameArg)!;
        var output = parseResult.GetValue(_outputOpt) ?? ".";

        var (privatePath, publicPath) = BuiltinEcdsaSigner.GenerateKeyPair(output, name);

        WriteSuccess($"Key pair generated:");
        WriteLine($"  Private key: {privatePath}");
        WriteLine($"  Public key:  {publicPath}");
        WriteLine();
        WriteLine("Sign packages:  surgewave plugins sign <package.swpkg> --key " + privatePath);
        WriteLine("Trust publisher: surgewave plugins trust " + publicPath);

        return Task.FromResult(0);
    }
}
