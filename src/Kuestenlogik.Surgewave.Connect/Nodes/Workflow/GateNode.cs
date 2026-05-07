namespace Kuestenlogik.Surgewave.Connect.Nodes.Workflow;

using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// Opens or closes the data flow based on an external signal.
/// When closed, records can be buffered or dropped depending on configuration.
/// </summary>
[ConnectorMetadata(
    Name = "Gate",
    Description = "Control data flow with an open/close gate signal",
    Tags = "workflow,gate,control,flow",
    Icon = "DoorSliding")]
public sealed class GateNode : ProcessorConnector
{
    public override Type TaskClass => typeof(GateNodeTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("signal.topic", ConfigType.String, "", Importance.High,
            "Topic for open/close signals")
        .Define("default.state", ConfigType.String, "open", Importance.Medium,
            "Initial gate state: open or closed")
        .Define("buffer.when.closed", ConfigType.Boolean, "true", Importance.Medium,
            "Buffer records when gate is closed, or drop them")
        .Define("max.buffer.size", ConfigType.Int, "1000", Importance.Medium,
            "Maximum buffered records when gate is closed")
        .Define("output.topic", ConfigType.String, "", Importance.High,
            "Output topic for records passing through the gate");
}

internal sealed class GateNodeTask : ProcessorTask
{
    private string _signalTopic = "";
    private volatile bool _isOpen = true;
    private bool _bufferWhenClosed = true;
    private int _maxBufferSize = 1000;

    private readonly Channel<BufferedRecord> _buffer =
        Channel.CreateBounded<BufferedRecord>(new BoundedChannelOptions(10000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = false,
            SingleWriter = false
        });

    public override void Start(IDictionary<string, string> config)
    {
        base.Start(config);
        if (config.TryGetValue("signal.topic", out var st))
            _signalTopic = st;
        if (config.TryGetValue("default.state", out var ds))
            _isOpen = !string.Equals(ds, "closed", StringComparison.OrdinalIgnoreCase);
        if (config.TryGetValue("buffer.when.closed", out var bwc) && bool.TryParse(bwc, out var bufferWhenClosed))
            _bufferWhenClosed = bufferWhenClosed;
        if (config.TryGetValue("max.buffer.size", out var mbs) && int.TryParse(mbs, out var maxBufferSize))
            _maxBufferSize = maxBufferSize;
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Topic == _signalTopic)
            {
                HandleSignal(record);
                continue;
            }

            if (_isOpen)
            {
                // Gate is open — pass through
                EmitRecord(GetKeyString(record), record.Value, ConvertHeaders(record.Headers));
            }
            else if (_bufferWhenClosed)
            {
                // Gate is closed — buffer the record
                if (_buffer.Reader.Count < _maxBufferSize)
                {
                    _buffer.Writer.TryWrite(new BufferedRecord(
                        GetKeyString(record),
                        record.Value,
                        ConvertHeaders(record.Headers)));
                }
                // Else: buffer full, silently drop
            }
            // Else: gate closed and no buffering — drop the record
        }

        return Task.CompletedTask;
    }

    private void HandleSignal(SinkRecord record)
    {
        using var doc = ParseJsonValue(record);
        if (doc is null)
            return;

        if (doc.RootElement.TryGetProperty("gate", out var gateElement))
        {
            var gateValue = gateElement.GetString();
            if (string.Equals(gateValue, "open", StringComparison.OrdinalIgnoreCase))
            {
                _isOpen = true;
                FlushBuffer();
            }
            else if (string.Equals(gateValue, "close", StringComparison.OrdinalIgnoreCase))
            {
                _isOpen = false;
            }
        }
    }

    private void FlushBuffer()
    {
        while (_buffer.Reader.TryRead(out var buffered))
        {
            EmitRecord(buffered.Key, buffered.Value, buffered.Headers);
        }
    }

    /// <summary>
    /// Exposes current gate state for testing.
    /// </summary>
    internal bool IsOpen => _isOpen;

    /// <summary>
    /// Exposes buffered record count for testing.
    /// </summary>
    internal int BufferedCount => _buffer.Reader.Count;

    /// <summary>
    /// Inject a gate state change for testing.
    /// </summary>
    internal void SetGateState(bool open)
    {
        _isOpen = open;
        if (open)
            FlushBuffer();
    }
}

internal sealed class BufferedRecord(string? key, object? value, Dictionary<string, string>? headers)
{
    public string? Key { get; } = key;
    public object? Value { get; } = value;
    public Dictionary<string, string>? Headers { get; } = headers;
}
