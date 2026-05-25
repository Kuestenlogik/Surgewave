namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using System.Text;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Sub-pipeline node that routes records through another pipeline.
/// Enables pipeline composition and reuse.
/// </summary>
[ConnectorMetadata(
    Name = "SubPipeline",
    Description = "Execute another pipeline inline",
    Tags = "logic,subpipeline,nested,compose")]
public sealed class SubPipelineNode : ProcessorConnector
{
    public override Type TaskClass => typeof(SubProcessorTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("pipeline.id", ConfigType.String, "", Importance.High,
            "ID of the pipeline to execute")
        .Define("input.topic.override", ConfigType.String, "", Importance.Medium,
            "Override the sub-pipeline's input topic (if empty, uses first node's input)")
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Topic to receive sub-pipeline output")
        .Define("pass.through.headers", ConfigType.Boolean, "true", Importance.Low,
            "Pass original headers to sub-pipeline");
}

internal sealed class SubProcessorTask : ProcessorTask
{
    private string _pipelineId = "";
    private string _inputTopicOverride = "";
    private bool _passThroughHeaders = true;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _pipelineId = config.TryGetValue("pipeline.id", out var p) ? p : "";
        _inputTopicOverride = config.TryGetValue("input.topic.override", out var i) ? i : "";
        _passThroughHeaders = !config.TryGetValue("pass.through.headers", out var h) || !bool.TryParse(h, out var b) || b;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(OutputTopic))
            return Task.CompletedTask;

        // The sub-pipeline's input topic is either explicitly configured
        // or derived from the pipeline ID (convention: _pipeline-{id}-input)
        var inputTopic = !string.IsNullOrEmpty(_inputTopicOverride)
            ? _inputTopicOverride
            : $"_pipeline-{_pipelineId}-input";

        foreach (var record in records)
        {
            var headers = _passThroughHeaders
                ? ConvertHeaders(record.Headers) ?? []
                : new Dictionary<string, string>();

            // Add routing metadata
            headers["_subpipeline_id"] = _pipelineId;
            headers["_subpipeline_return_topic"] = OutputTopic;
            headers["_subpipeline_original_topic"] = record.Topic;
            headers["_subpipeline_original_partition"] = record.Partition.ToString();
            headers["_subpipeline_original_offset"] = record.Offset.ToString();

            // Route to sub-pipeline input
            EmitRecordTo(inputTopic, GetKeyString(record), record.Value, headers);
        }

        return Task.CompletedTask;
    }
}

/// <summary>
/// Output collector for sub-pipeline that routes results back to the parent pipeline.
/// Place this as the last node in a sub-pipeline to return results.
/// </summary>
[ConnectorMetadata(
    Name = "SubPipelineOutput",
    Description = "Return results from sub-pipeline",
    Tags = "logic,subpipeline,output,return")]
public sealed class SubPipelineOutputNode : ProcessorConnector
{
    public override Type TaskClass => typeof(SubPipelineOutputNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.Medium,
            "Default output topic (overridden by _subpipeline_return_topic header)")
        .Define("preserve.metadata", ConfigType.Boolean, "true", Importance.Low,
            "Preserve original record metadata from parent pipeline");
}

internal sealed class SubPipelineOutputNodeTask : ProcessorTask
{
    private bool _preserveMetadata = true;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _preserveMetadata = !config.TryGetValue("preserve.metadata", out var p) || !bool.TryParse(p, out var b) || b;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var headers = ConvertHeaders(record.Headers) ?? [];

            // Get return topic from header (set by SubPipelineNode)
            var returnTopic = headers.TryGetValue("_subpipeline_return_topic", out var rt)
                ? rt
                : OutputTopic;

            if (string.IsNullOrEmpty(returnTopic))
                continue;

            // Build result headers
            Dictionary<string, string>? resultHeaders = null;

            if (_preserveMetadata)
            {
                resultHeaders = [];

                // Copy original metadata if present
                if (headers.TryGetValue("_subpipeline_original_topic", out var origTopic))
                    resultHeaders["_original_topic"] = origTopic;
                if (headers.TryGetValue("_subpipeline_original_partition", out var origPartition))
                    resultHeaders["_original_partition"] = origPartition;
                if (headers.TryGetValue("_subpipeline_original_offset", out var origOffset))
                    resultHeaders["_original_offset"] = origOffset;
                if (headers.TryGetValue("_subpipeline_id", out var subId))
                    resultHeaders["_processed_by_pipeline"] = subId;

                // Copy non-subpipeline headers
                foreach (var (key, value) in headers)
                {
                    if (!key.StartsWith("_subpipeline_", StringComparison.Ordinal))
                    {
                        resultHeaders[key] = value;
                    }
                }
            }

            EmitRecordTo(returnTopic, GetKeyString(record), record.Value, resultHeaders);
        }

        return Task.CompletedTask;
    }
}
