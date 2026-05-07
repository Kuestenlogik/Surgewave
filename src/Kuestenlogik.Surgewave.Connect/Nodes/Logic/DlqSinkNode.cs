namespace Kuestenlogik.Surgewave.Connect.Nodes.Logic;

using System.Text;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Dead Letter Queue sink node that receives error records and writes them to a DLQ topic.
/// Error records arrive already enriched with error metadata from upstream nodes.
/// </summary>
[ConnectorMetadata(
    Name = "DLQ Sink",
    Description = "Routes failed records to a Dead Letter Queue topic",
    Tags = "logic,dlq,error")]
public sealed class DlqSinkNode : ProcessorConnector
{
    public override Type TaskClass => typeof(DlqSinkNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "DLQ output topic name")
        .Define("include.stack.trace", ConfigType.Boolean, "false", Importance.Medium,
            "Include full stack trace in error records")
        .Define("max.value.bytes", ConfigType.Int, "1048576", Importance.Low,
            "Maximum value size in bytes (default 1MB). Larger values are truncated.");
}

internal sealed class DlqSinkNodeTask : ProcessorTask
{
    private bool _includeStackTrace;
    private int _maxValueBytes = 1048576;

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        _includeStackTrace = config.TryGetValue("include.stack.trace", out var ist) &&
                             bool.TryParse(ist, out var v) && v;
        if (config.TryGetValue("max.value.bytes", out var maxBytes) && int.TryParse(maxBytes, out var mb))
        {
            _maxValueBytes = mb;
        }
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var value = record.Value;

            // Truncate if exceeds max size
            if (value != null && value.Length > _maxValueBytes)
            {
                value = value[.._maxValueBytes];
            }

            var headers = ConvertHeaders(record.Headers);

            // Remove stack trace header if not configured
            if (!_includeStackTrace)
            {
                headers?.Remove("_error_stack_trace");
            }

            EmitRecord(GetKeyString(record), value, headers);
        }

        return Task.CompletedTask;
    }
}
