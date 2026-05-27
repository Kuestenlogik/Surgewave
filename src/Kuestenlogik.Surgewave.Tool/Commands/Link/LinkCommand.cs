namespace Kuestenlogik.Surgewave.Cli.Commands.Link;

/// <summary>
/// Command for managing cluster links for geo-replication (surgewave link ...)
/// </summary>
public class LinkCommand : CommandBase
{
    public LinkCommand() : base("link", "Manage cluster links for geo-replication")
    {
        Subcommands.Add(new CreateLinkCommand());
        Subcommands.Add(new ListLinksCommand());
        Subcommands.Add(new DescribeLinkCommand());
        Subcommands.Add(new DeleteLinkCommand());
        Subcommands.Add(new PauseLinkCommand());
        Subcommands.Add(new ResumeLinkCommand());
    }
}
