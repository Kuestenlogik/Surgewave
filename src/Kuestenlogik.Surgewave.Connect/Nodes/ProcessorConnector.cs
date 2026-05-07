namespace Kuestenlogik.Surgewave.Connect.Nodes;

using System.Diagnostics;
using System.Globalization;
using System.Text;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Connect.Configuration;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Kuestenlogik.Surgewave.Plugins.Pipeline;

/// <summary>
/// Base class for pipeline processing nodes that read from input topics and write to output topics.
/// These nodes act as sink connectors that can emit records to other topics.
/// </summary>
public abstract class ProcessorConnector : Connector, IProcessorNode
{
    /// <summary>Processor nodes have one input port.</summary>
    public override int InputPorts => 1;

    /// <summary>Processor nodes have one output port.</summary>
    public override int OutputPorts => 1;

    public override string Version => "1.0.0";

    protected IDictionary<string, string> ConnectorConfig { get; private set; } = new Dictionary<string, string>();

    public override void Start(IDictionary<string, string> config)
    {
        ConnectorConfig = config;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(ConnectorConfig)];
    }
}

/// <summary>
/// Base class for pipeline node tasks.
/// </summary>
public abstract class ProcessorTask : SinkTask
{
    public override string Version => "1.0.0";

    protected IDictionary<string, string> TaskConfig { get; private set; } = new Dictionary<string, string>();
    protected string? OutputTopic { get; private set; }
    protected string? ErrorTopic { get; private set; }
    protected string NodeId { get; private set; } = "";
    private string _pipelineId = "";

    // Per-instance services resolved from TaskContext (preferred)
    private PipelineMetricsCollector? _metricsCollector;
    private PipelineDebugger? _debugger;
    private ISchemaRegistryOperations? _schemaRegistry;

    /// <summary>
    /// Metrics collector for this task. Resolved from TaskContext.
    /// </summary>
    protected PipelineMetricsCollector? MetricsCollector => _metricsCollector;

    /// <summary>
    /// Debugger for this task. Resolved from TaskContext.
    /// </summary>
    protected PipelineDebugger? Debugger => _debugger;

    /// <summary>
    /// Schema registry for this task. Resolved from TaskContext.
    /// </summary>
    protected ISchemaRegistryOperations? SchemaRegistry => _schemaRegistry;




    /// <summary>
    /// Records emitted by this node task during processing.
    /// </summary>
    public List<SourceRecord> EmittedRecords { get; } = [];

    public override void Initialize(TaskContext context)
    {
        base.Initialize(context);
        _metricsCollector = context.MetricsCollector;
        _debugger = context.Debugger;
        _schemaRegistry = context.SchemaRegistry;
    }

    public override void Start(IDictionary<string, string> config)
    {
        TaskConfig = config;
        OutputTopic = config.TryGetValue("output.topic", out var topic) ? topic : null;
        ErrorTopic = config.TryGetValue("error.topic", out var errorTopic) ? errorTopic : null;
        NodeId = config.TryGetValue("node.id", out var nodeId) ? nodeId : "";
        _pipelineId = config.TryGetValue("pipeline.id", out var pid) ? pid : "";
    }

    public override void Stop()
    {
    }

    /// <summary>
    /// Emit a record to the output topic.
    /// </summary>
    protected void EmitRecord(string? key, object? value, Dictionary<string, string>? headers = null)
    {
        if (OutputTopic is null)
            return;

        EmitRecordTo(OutputTopic, key, value, headers);
    }

    /// <summary>
    /// Emit a record to a specific topic and partition.
    /// </summary>
    protected void EmitRecordTo(string topic, int? partition, string? key, object? value, Dictionary<string, string>? headers = null)
    {
        var keyBytes = key != null ? Encoding.UTF8.GetBytes(key) : null;
        var valueBytes = value switch
        {
            null => [],
            byte[] bytes => bytes,
            string str => Encoding.UTF8.GetBytes(str),
            _ => Encoding.UTF8.GetBytes(value.ToString() ?? "")
        };

        var record = new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["topic"] = topic },
            SourceOffset = new Dictionary<string, object> { ["offset"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() },
            Topic = topic,
            Partition = partition,
            Key = keyBytes,
            Value = valueBytes,
            Headers = ConvertToByteHeaders(headers)
        };

        EmittedRecords.Add(record);

        if (!string.IsNullOrEmpty(_pipelineId))
            MetricsCollector?.RecordProcessed(_pipelineId, NodeId, 0);
    }

    /// <summary>
    /// Emit a record to a specific topic (for branching nodes).
    /// </summary>
    protected void EmitRecordTo(string topic, string? key, object? value, Dictionary<string, string>? headers = null)
    {
        var keyBytes = key != null ? Encoding.UTF8.GetBytes(key) : null;
        var valueBytes = value switch
        {
            null => [],
            byte[] bytes => bytes,
            string str => Encoding.UTF8.GetBytes(str),
            _ => Encoding.UTF8.GetBytes(value.ToString() ?? "")
        };

        var byteHeaders = ConvertToByteHeaders(headers) ?? new Dictionary<string, byte[]>();

        // Append provenance if enabled
        if (ProvenanceTracker.Enabled && !string.IsNullOrEmpty(NodeId))
        {
            ProvenanceTracker.AppendProvenance(byteHeaders, NodeId, DateTimeOffset.UtcNow);
        }

        var record = new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["topic"] = topic },
            SourceOffset = new Dictionary<string, object> { ["offset"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() },
            Topic = topic,
            Key = keyBytes,
            Value = valueBytes,
            Headers = byteHeaders.Count > 0 ? byteHeaders : null
        };

        EmittedRecords.Add(record);

        if (!string.IsNullOrEmpty(_pipelineId))
            MetricsCollector?.RecordProcessed(_pipelineId, NodeId, 0);
    }

    protected static Dictionary<string, string>? ConvertHeaders(IReadOnlyDictionary<string, byte[]>? headers)
    {
        if (headers is null || headers.Count == 0)
            return null;

        return headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.Value));
    }

    protected static Dictionary<string, byte[]>? ConvertToByteHeaders(Dictionary<string, string>? headers)
    {
        if (headers is null || headers.Count == 0)
            return null;

        return headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetBytes(h.Value));
    }

    /// <summary>
    /// Emit a record to the error topic with error metadata.
    /// No-op if no error topic is configured (backward compatible).
    /// </summary>
    protected void EmitError(SinkRecord record, Exception ex)
    {
        if (ErrorTopic is null)
            return;

        if (!string.IsNullOrEmpty(_pipelineId))
            MetricsCollector?.RecordError(_pipelineId, NodeId);

        var (value, headers) = PipelineErrorRecord.Create(record, NodeId, ex);
        var stringHeaders = headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.Value));
        EmitRecordTo(ErrorTopic, GetKeyString(record), value, stringHeaders);
    }

    protected static System.Text.Json.JsonDocument? ParseJsonValue(SinkRecord record)
    {
        if (record.Value is null || record.Value.Length == 0)
            return null;

        try
        {
            return System.Text.Json.JsonDocument.Parse(record.Value);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Parse JSON value or emit error record. Returns null on failure.
    /// If ErrorTopic is configured, errors are routed there; otherwise behavior matches ParseJsonValue.
    /// </summary>
    protected System.Text.Json.JsonDocument? ParseJsonValueOrError(SinkRecord record)
    {
        if (record.Value is null || record.Value.Length == 0)
            return null;

        try
        {
            return System.Text.Json.JsonDocument.Parse(record.Value);
        }
        catch (System.Text.Json.JsonException ex)
        {
            EmitError(record, ex);
            return null;
        }
    }

    protected static string GetKeyString(SinkRecord record)
    {
        return record.Key is not null ? Encoding.UTF8.GetString(record.Key) : "";
    }

    // Retry policy fields
    private bool _retryEnabled;
    private int _retryMaxAttempts = 3;
    private long _retryBackoffMs = 1000;
    private double _retryBackoffMultiplier = 2.0;
    private long _retryMaxBackoffMs = 30000;

    /// <summary>
    /// Initialize retry policy from config keys.
    /// Call from Start() in derived classes.
    /// </summary>
    protected void InitRetryPolicy(IDictionary<string, string> config)
    {
        _retryEnabled = config.TryGetValue("_retry.enabled", out var e) && bool.TryParse(e, out var eb) && eb;
        if (config.TryGetValue("_retry.max.attempts", out var ma) && int.TryParse(ma, out var mai))
            _retryMaxAttempts = mai;
        if (config.TryGetValue("_retry.backoff.ms", out var bo) && long.TryParse(bo, out var bol))
            _retryBackoffMs = bol;
        if (config.TryGetValue("_retry.backoff.multiplier", out var bm) && double.TryParse(bm, System.Globalization.CultureInfo.InvariantCulture, out var bmd))
            _retryBackoffMultiplier = bmd;
        if (config.TryGetValue("_retry.max.backoff.ms", out var mb) && long.TryParse(mb, out var mbl))
            _retryMaxBackoffMs = mbl;
    }

    /// <summary>
    /// Process a record with retry. Falls back to EmitError after exhaustion.
    /// </summary>
    protected async Task ProcessWithRetryAsync(SinkRecord record, Func<SinkRecord, Task> processFunc)
    {
        if (!_retryEnabled)
        {
            try
            {
                await processFunc(record);
            }
            catch (Exception ex)
            {
                EmitError(record, ex);
            }
            return;
        }

        var pipelineId = TaskConfig.TryGetValue("pipeline.id", out var pid) ? pid : "";
        var currentBackoff = (double)_retryBackoffMs;

        for (var attempt = 1; attempt <= _retryMaxAttempts; attempt++)
        {
            try
            {
                await processFunc(record);
                return;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception) when (attempt < _retryMaxAttempts)
            {
                MetricsCollector?.RecordRetryAttempt(pipelineId, NodeId);
                var delay = (int)Math.Min(currentBackoff, _retryMaxBackoffMs);
                await Task.Delay(delay);
                currentBackoff *= _retryBackoffMultiplier;
            }
            catch (Exception ex)
            {
                MetricsCollector?.RecordRetryAttempt(pipelineId, NodeId);
                MetricsCollector?.RecordRetryExhausted(pipelineId, NodeId);
                EmitError(record, ex);
            }
        }
    }
}
