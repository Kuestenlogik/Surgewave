namespace Kuestenlogik.Surgewave.Connect.Pipelines;

using System.Text;
using Kuestenlogik.Surgewave.Connect.Nodes;

/// <summary>
/// Executes a pipeline with sample data without producing to real topics.
/// Uses topological sorting to process nodes in dependency order.
/// </summary>
public sealed class PipelineDryRunner
{
    /// <summary>
    /// Run a pipeline dry run with the given inputs.
    /// </summary>
    public async Task<DryRunResult> RunAsync(
        PipelineDefinition pipeline,
        List<DryRunInput> inputs,
        CancellationToken ct)
    {
        try
        {
            // Topological sort of nodes
            var sortedNodeIds = TopologicalSort(pipeline.Nodes, pipeline.Connections);

            // Build graph: which node outputs feed into which nodes
            var nodeOutputTargets = new Dictionary<string, List<string>>();
            foreach (var conn in pipeline.Connections)
            {
                if (!nodeOutputTargets.ContainsKey(conn.SourceNodeId))
                    nodeOutputTargets[conn.SourceNodeId] = [];
                nodeOutputTargets[conn.SourceNodeId].Add(conn.TargetNodeId);
            }

            // Node input records accumulator
            var nodeInputs = new Dictionary<string, List<SinkRecord>>();
            foreach (var node in pipeline.Nodes)
                nodeInputs[node.Id] = [];

            // Seed with user-provided dry run inputs
            var inputMap = inputs.ToDictionary(i => i.NodeId);
            foreach (var (nodeId, dryInput) in inputMap)
            {
                foreach (var rec in dryInput.Records)
                {
                    nodeInputs.TryAdd(nodeId, []);
                    nodeInputs[nodeId].Add(ToSinkRecord(rec, $"dryrun-{nodeId}"));
                }
            }

            // Build variable resolution context
            var variableContext = new PipelineVariableContext
            {
                PipelineId = pipeline.Id,
                PipelineName = pipeline.Name,
                Parameters = pipeline.Parameters ?? new Dictionary<string, string>()
            };

            var traces = new Dictionary<string, DryRunNodeTrace>();
            var nodeMap = pipeline.Nodes.ToDictionary(n => n.Id);

            foreach (var nodeId in sortedNodeIds)
            {
                if (!nodeMap.TryGetValue(nodeId, out var node))
                    continue;

                var inputRecords = nodeInputs.TryGetValue(nodeId, out var recs) ? recs : [];

                var trace = await ExecuteNodeAsync(node, inputRecords, variableContext, ct);
                traces[nodeId] = trace;

                // Feed outputs to downstream nodes
                if (nodeOutputTargets.TryGetValue(nodeId, out var targets))
                {
                    foreach (var targetId in targets)
                    {
                        nodeInputs.TryAdd(targetId, []);
                        foreach (var output in trace.Outputs)
                        {
                            nodeInputs[targetId].Add(ToSinkRecord(output, $"dryrun-{nodeId}->{targetId}"));
                        }
                    }
                }
            }

            return new DryRunResult
            {
                Success = true,
                NodeTraces = traces
            };
        }
        catch (Exception ex)
        {
            return new DryRunResult
            {
                Success = false,
                NodeTraces = new Dictionary<string, DryRunNodeTrace>(),
                Error = ex.Message
            };
        }
    }

    private static async Task<DryRunNodeTrace> ExecuteNodeAsync(
        PipelineNode node,
        List<SinkRecord> inputs,
        PipelineVariableContext variableContext,
        CancellationToken ct)
    {
        var errors = new List<string>();
        var outputs = new List<DryRunRecord>();

        try
        {
            var connectorType = Type.GetType(node.ConnectorType);
            if (connectorType == null)
            {
                return new DryRunNodeTrace
                {
                    NodeId = node.Id,
                    ConnectorType = node.ConnectorType,
                    InputCount = inputs.Count,
                    OutputCount = 0,
                    Outputs = [],
                    Errors = [$"Cannot resolve connector type: {node.ConnectorType}"]
                };
            }

            // Check if it's a ProcessorConnector (sink-based pipeline node)
            if (typeof(ProcessorConnector).IsAssignableFrom(connectorType))
            {
                var connector = (ProcessorConnector)Activator.CreateInstance(connectorType)!;
                var taskType = connector.TaskClass;
                var task = (ProcessorTask)Activator.CreateInstance(taskType)!;

                var config = new Dictionary<string, string>(node.Config)
                {
                    ["node.id"] = node.Id,
                    ["output.topic"] = $"dryrun-output-{node.Id}"
                };

                // Resolve variables
                config = PipelineVariableResolver.Resolve(config, variableContext with { NodeId = node.Id });

                task.Start(config);

                try
                {
                    if (inputs.Count > 0)
                    {
                        await task.PutAsync(inputs, ct);
                    }

                    // Collect emitted records
                    foreach (var emitted in task.EmittedRecords)
                    {
                        outputs.Add(FromSourceRecord(emitted));
                    }
                }
                catch (Exception ex)
                {
                    errors.Add(ex.Message);
                }
                finally
                {
                    task.Stop();
                }
            }
            else
            {
                errors.Add($"Dry run only supports ProcessorConnector types, got: {connectorType.Name}");
            }
        }
        catch (Exception ex)
        {
            errors.Add($"Failed to create node instance: {ex.Message}");
        }

        return new DryRunNodeTrace
        {
            NodeId = node.Id,
            ConnectorType = node.ConnectorType,
            InputCount = inputs.Count,
            OutputCount = outputs.Count,
            Outputs = outputs,
            Errors = errors
        };
    }

    /// <summary>
    /// Topological sort using Kahn's algorithm.
    /// </summary>
    internal static List<string> TopologicalSort(List<PipelineNode> nodes, List<PipelineConnection> connections)
    {
        var inDegree = new Dictionary<string, int>();
        var adjacency = new Dictionary<string, List<string>>();

        foreach (var node in nodes)
        {
            inDegree[node.Id] = 0;
            adjacency[node.Id] = [];
        }

        foreach (var conn in connections)
        {
            if (adjacency.TryGetValue(conn.SourceNodeId, out var adj) &&
                inDegree.TryGetValue(conn.TargetNodeId, out var degree))
            {
                adj.Add(conn.TargetNodeId);
                inDegree[conn.TargetNodeId] = degree + 1;
            }
        }

        var queue = new Queue<string>(inDegree.Where(kv => kv.Value == 0).Select(kv => kv.Key));
        var result = new List<string>();

        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            result.Add(current);

            foreach (var neighbor in adjacency[current])
            {
                inDegree[neighbor]--;
                if (inDegree[neighbor] == 0)
                    queue.Enqueue(neighbor);
            }
        }

        // Add any remaining nodes (in case of cycles or disconnected)
        foreach (var node in nodes)
        {
            if (!result.Contains(node.Id))
                result.Add(node.Id);
        }

        return result;
    }

    private static SinkRecord ToSinkRecord(DryRunRecord record, string topic)
    {
        return new SinkRecord
        {
            Topic = topic,
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow,
            Key = record.Key != null ? Encoding.UTF8.GetBytes(record.Key) : null,
            Value = record.Value != null ? Encoding.UTF8.GetBytes(record.Value) : [],
            Headers = record.Headers?.ToDictionary(h => h.Key, h => Encoding.UTF8.GetBytes(h.Value))
        };
    }

    private static DryRunRecord FromSourceRecord(SourceRecord record)
    {
        return new DryRunRecord
        {
            Key = record.Key != null ? Encoding.UTF8.GetString(record.Key) : null,
            Value = record.Value != null ? Encoding.UTF8.GetString(record.Value) : null,
            Headers = record.Headers?.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.Value))
        };
    }
}
