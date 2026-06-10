namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Command group for managing local plugin packages (surgewave plugins ...)
/// </summary>
public class PluginCommand : CommandBase
{
    public PluginCommand() : base("plugins", "Manage local plugin packages")
    {
        Subcommands.Add(new ListPluginsCommand());
        Subcommands.Add(new ShowPluginCommand());
        Subcommands.Add(new PluginDefaultsCommand());
        Subcommands.Add(new PluginDiffCommand());
        Subcommands.Add(new SearchPluginsCommand());
        Subcommands.Add(new RecommendPluginsCommand());
        Subcommands.Add(new InstallPluginCommand());
        Subcommands.Add(new UninstallPluginCommand());
        Subcommands.Add(new PackPluginCommand());
        Subcommands.Add(new ValidatePluginCommand());
        Subcommands.Add(new PublishPluginCommand());
        Subcommands.Add(new RepoCommand());
        Subcommands.Add(new SourceCommand());
        Subcommands.Add(new DepsCommand());
        Subcommands.Add(new KeygenCommand());
        Subcommands.Add(new SignCommand());
        Subcommands.Add(new VerifyCommand());
        Subcommands.Add(new TrustCommand());
    }
}
