# Tutorial 04: Kafka Connect

Build and run custom connectors to stream data between Surgewave and external systems.

## Prerequisites

- Completed [Tutorial 01](01-getting-started.md) or have a Surgewave broker running on `localhost:9092`
- .NET 10 SDK installed
- Familiarity with Surgewave topics and message flow

## What You Will Build

Two custom connectors:
1. A **FileSource** connector that reads lines from a file and produces them as messages
2. A **ConsoleSink** connector that consumes messages and writes them to a log file

## Concepts

| Term | Description |
|------|-------------|
| **Connector** | Manages configuration and creates tasks |
| **Task** | Performs the actual data movement |
| **SourceConnector** | Imports data into Surgewave topics |
| **SinkConnector** | Exports data from Surgewave topics |
| **ConfigDef** | Declares and validates connector configuration |

## Step 1: Create the Project

```bash
mkdir surgewave-connectors-tutorial && cd surgewave-connectors-tutorial
dotnet new classlib -n MyConnectors
cd MyConnectors
dotnet add package Kuestenlogik.Surgewave.Connect
```

## Step 2: Build a Source Connector

### Connector Class

Create `FileSourceConnector.cs`:

```csharp
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Configuration;

namespace MyConnectors;

public sealed class FileSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(FileSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("file.path", ConfigType.String, Importance.High,
            "Path to the file to read")
        .Define("topic", ConfigType.String, Importance.High,
            "Destination Surgewave topic")
        .Define("poll.interval.ms", ConfigType.Long, 5000L, Importance.Medium,
            "How often to check the file for new lines");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue("file.path", out var path) ||
            string.IsNullOrWhiteSpace(path))
        {
            throw new ArgumentException("Missing required config: file.path");
        }

        if (!config.TryGetValue("topic", out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("Missing required config: topic");
        }

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
```

### Task Class

Create `FileSourceTask.cs`:

```csharp
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace MyConnectors;

public sealed class FileSourceTask : SourceTask
{
    private string _filePath = "";
    private string _topic = "";
    private long _pollIntervalMs = 5000;
    private long _lastLineRead;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _filePath = config["file.path"];
        _topic = config["topic"];

        if (config.TryGetValue("poll.interval.ms", out var interval) &&
            long.TryParse(interval, out var ms))
        {
            _pollIntervalMs = ms;
        }

        // Restore offset from previous run
        var partition = new Dictionary<string, object> { ["file"] = _filePath };
        var storedOffset = Context.OffsetStorageReader?.Offset(partition);
        if (storedOffset?.TryGetValue("line", out var line) == true)
        {
            _lastLineRead = Convert.ToInt64(line);
        }
    }

    public override void Stop() { }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(
        CancellationToken cancellationToken)
    {
        await Task.Delay((int)_pollIntervalMs, cancellationToken);

        if (!File.Exists(_filePath))
            return [];

        var lines = await File.ReadAllLinesAsync(_filePath, cancellationToken);

        if (lines.Length <= _lastLineRead)
            return [];

        var records = new List<SourceRecord>();
        var partition = new Dictionary<string, object> { ["file"] = _filePath };

        for (var i = (int)_lastLineRead; i < lines.Length; i++)
        {
            var line = lines[i];
            if (string.IsNullOrWhiteSpace(line))
                continue;

            _lastLineRead = i + 1;

            records.Add(new SourceRecord
            {
                SourcePartition = partition,
                SourceOffset = new Dictionary<string, object> { ["line"] = _lastLineRead },
                Topic = _topic,
                Key = Encoding.UTF8.GetBytes($"line-{i}"),
                Value = Encoding.UTF8.GetBytes(line),
                Timestamp = DateTimeOffset.UtcNow
            });
        }

        return records;
    }
}
```

## Step 3: Build a Sink Connector

### Connector Class

Create `ConsoleSinkConnector.cs`:

```csharp
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Configuration;

namespace MyConnectors;

public sealed class ConsoleSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(ConsoleSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("output.file", ConfigType.String, "output.log", Importance.Medium,
            "Path to the output log file")
        .Define("topics", ConfigType.String, Importance.High,
            "Source topics to consume (comma-separated)");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue("topics", out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException("Missing required config: topics");
        }

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
```

### Task Class

Create `ConsoleSinkTask.cs`:

```csharp
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace MyConnectors;

public sealed class ConsoleSinkTask : SinkTask
{
    private string _outputFile = "output.log";
    private StreamWriter? _writer;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        if (config.TryGetValue("output.file", out var file) &&
            !string.IsNullOrWhiteSpace(file))
        {
            _outputFile = file;
        }

        _writer = new StreamWriter(_outputFile, append: true) { AutoFlush = true };
    }

    public override void Stop()
    {
        _writer?.Dispose();
        _writer = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _writer?.Dispose();
            _writer = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records,
        CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            var key = record.Key != null
                ? Encoding.UTF8.GetString(record.Key) : "(null)";
            var value = record.Value != null
                ? Encoding.UTF8.GetString(record.Value) : "(null)";

            var line = $"[{record.Timestamp:HH:mm:ss}] " +
                       $"Topic={record.Topic} Key={key} Value={value}";

            if (_writer != null)
                await _writer.WriteLineAsync(line);

            Console.WriteLine(line);
        }
    }

    public override Task FlushAsync(
        IDictionary<TopicPartition, long> currentOffsets,
        CancellationToken cancellationToken)
    {
        _writer?.Flush();
        return Task.CompletedTask;
    }
}
```

## Step 4: Deploy the Connector

Build the project and copy the DLL to the Surgewave plugins directory:

```bash
dotnet build -c Release
```

Copy the output to the plugins directory:

```bash
# Windows
mkdir C:\surgewave\plugins\MyConnectors
copy bin\Release\net10.0\MyConnectors.dll C:\surgewave\plugins\MyConnectors\

# Linux/macOS
mkdir -p /surgewave/plugins/MyConnectors
cp bin/Release/net10.0/MyConnectors.dll /surgewave/plugins/MyConnectors/
```

## Step 5: Run the Connectors

### Via REST API

Start the source connector:

```bash
curl -X POST https://localhost:9093/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "file-source",
    "config": {
      "connector.class": "MyConnectors.FileSourceConnector",
      "file.path": "/tmp/input.txt",
      "topic": "file-lines",
      "poll.interval.ms": "2000"
    }
  }'
```

Start the sink connector:

```bash
curl -X POST https://localhost:9093/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "console-sink",
    "config": {
      "connector.class": "MyConnectors.ConsoleSinkConnector",
      "topics": "file-lines",
      "output.file": "/tmp/output.log"
    }
  }'
```

### Via CLI

```bash
surgewave connect create file-source --config file-source.json
surgewave connect create console-sink --config console-sink.json
```

## Step 6: Test It

Write some lines to the input file:

```bash
echo "First message" >> /tmp/input.txt
echo "Second message" >> /tmp/input.txt
echo "Third message" >> /tmp/input.txt
```

Check the output:

```bash
cat /tmp/output.log
```

Expected output:

```
[10:30:01] Topic=file-lines Key=line-0 Value=First message
[10:30:01] Topic=file-lines Key=line-1 Value=Second message
[10:30:01] Topic=file-lines Key=line-2 Value=Third message
```

## Managing Connectors

```bash
# Check connector status
surgewave connect status file-source

# Pause a connector
surgewave connect pause file-source

# Resume a connector
surgewave connect resume file-source

# List all connectors
surgewave connect list

# Delete a connector
surgewave connect delete file-source
```

## Next Steps

- [Tutorial 05: Kafka Streams](05-kafka-streams.md) -- real-time stream processing
- [Custom Connectors Guide](../connectors/custom-connectors.md) -- advanced connector development
- [Built-in Connectors](../connectors/index.md) -- browse 100+ available connectors
- [Connect Framework Reference](../features/connect.md) -- full REST API documentation
