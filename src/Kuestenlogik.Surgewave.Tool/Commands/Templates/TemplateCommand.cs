namespace Kuestenlogik.Surgewave.Cli.Commands.Templates;

/// <summary>
/// Command group for managing pipeline templates (surgewave templates ...)
/// </summary>
public class TemplateCommand : CommandBase
{
    public TemplateCommand() : base("templates", "Manage pipeline templates")
    {
        Subcommands.Add(new ListTemplatesCommand());
        Subcommands.Add(new SearchTemplatesCommand());
        Subcommands.Add(new ShowTemplateCommand());
        Subcommands.Add(new InstallTemplateCommand());
    }
}
