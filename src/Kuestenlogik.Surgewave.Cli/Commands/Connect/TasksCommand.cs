namespace Kuestenlogik.Surgewave.Cli.Commands.Connect;

/// <summary>
/// Manage connector tasks (surgewave connect tasks)
/// </summary>
public class TasksCommand : CommandBase
{
    public TasksCommand() : base("tasks", "Manage connector tasks")
    {
        Subcommands.Add(new ListTasksCommand());
        Subcommands.Add(new RestartTaskCommand());
    }
}
