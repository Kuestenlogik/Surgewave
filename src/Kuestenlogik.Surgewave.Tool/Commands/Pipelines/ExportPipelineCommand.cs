using System.CommandLine;
using System.CommandLine.Parsing;
using Kuestenlogik.Surgewave.Pipelines.Publishing;
using Kuestenlogik.Surgewave.Pipelines.Serialization;

namespace Kuestenlogik.Surgewave.Cli.Commands.Pipelines;

/// <summary>
/// Export a pipeline as JSON (surgewave pipelines export)
/// </summary>
public class ExportPipelineCommand : CommandBase
{
    private readonly Argument<string> _pipelineArg = new("pipeline")
    {
        Description = "Pipeline id or name"
    };

    private readonly Option<string?> _outputOpt = new("--output", "-o")
    {
        Description = "File to write the export to (stdout when omitted)"
    };

    public ExportPipelineCommand() : base("export", "Export a pipeline as portable JSON")
    {
        Arguments.Add(_pipelineArg);
        Options.Add(_outputOpt);
        this.SetAction(ExecuteAsync);
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken ct)
    {
        var idOrName = parseResult.GetValue(_pipelineArg)!;
        var outputPath = parseResult.GetValue(_outputOpt);

        try
        {
            using var publisher = PipelineCli.CreatePublisher(parseResult);
            var pipeline = await PipelineCli.ResolveAsync(publisher, idOrName, ct);
            if (pipeline is null)
            {
                WriteError($"Pipeline '{idOrName}' not found.");
                return 1;
            }

            var export = await publisher.ExportAsync(pipeline.Id, ct);
            var json = PipelineJson.Write(export);

            if (outputPath is null)
            {
                Console.WriteLine(json);
            }
            else
            {
                await File.WriteAllTextAsync(outputPath, json, ct);
                WriteSuccess($"Pipeline '{pipeline.Name}' exported to {outputPath}.");
            }

            return 0;
        }
        catch (PipelinePublishException ex)
        {
            WriteError(ex.Message);
            return 1;
        }
    }
}
