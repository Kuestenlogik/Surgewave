namespace Kuestenlogik.Surgewave.Cli.Commands.Messages;

/// <summary>
/// Command group for message operations (surgewave message ...)
/// </summary>
public class MessageCommand : CommandBase
{
    public MessageCommand() : base("message", "Inspect individual messages")
    {
        Subcommands.Add(new GetMessageCommand());
    }
}
