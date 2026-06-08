namespace Kuestenlogik.Surgewave.Cli.Commands.Sdk;

/// <summary>
/// <c>surgewave sdk ...</c> — plugin-author tools for pulling the
/// Surgewave SDK at a specific version. See <see cref="InstallSdkCommand"/>.
/// </summary>
public sealed class SdkCommand : CommandBase
{
    public SdkCommand() : base("sdk", "Plugin-author tools: install a versioned local SDK feed")
    {
        Subcommands.Add(new InstallSdkCommand());
    }
}
