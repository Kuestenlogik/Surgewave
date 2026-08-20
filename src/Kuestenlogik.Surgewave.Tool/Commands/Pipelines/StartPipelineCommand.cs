using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Pipelines.Publishing;

namespace Kuestenlogik.Surgewave.Cli.Commands.Pipelines;

/// <summary>
/// Start a pipeline (surgewave pipelines start)
/// </summary>
public class StartPipelineCommand : CommandBase
{
    private readonly Argument<string> _pipelineArg = new("pipeline")
    {
        Description = "Pipeline id or name"
    };

    public StartPipelineCommand() : base("start", "Start a pipeline")
    {
        Arguments.Add(_pipelineArg);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var idOrName = parseResult.GetValue(_pipelineArg)!;

        try
        {
            using var publisher = PipelineCli.CreatePublisher(parseResult);
            var pipeline = await PipelineCli.ResolveAsync(publisher, idOrName, ct);
            if (pipeline is null)
            {
                WriteError($"Pipeline '{idOrName}' not found.");
                return 1;
            }

            await publisher.StartAsync(pipeline.Id, ct);
            WriteSuccess($"Pipeline '{pipeline.Name}' ({pipeline.Id}) started.");
            return 0;
        }
        catch (PipelinePublishException ex)
        {
            WriteError(ex.Message);
            return 1;
        }
    }
}
