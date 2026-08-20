namespace Kuestenlogik.Surgewave.Cli.Commands.Pipelines;

/// <summary>
/// Pipeline management command group (surgewave pipelines)
/// </summary>
public class PipelinesCommand : CommandBase
{
    public PipelinesCommand() : base("pipelines", "Manage broker pipelines — deploy pipeline-as-code builds, list, export, start and stop")
    {
        Subcommands.Add(new ListPipelinesCommand());
        Subcommands.Add(new DeployPipelineCommand());
        Subcommands.Add(new ExportPipelineCommand());
        Subcommands.Add(new StartPipelineCommand());
        Subcommands.Add(new StopPipelineCommand());
    }
}
