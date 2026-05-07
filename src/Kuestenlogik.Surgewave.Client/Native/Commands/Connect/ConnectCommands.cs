using System.Text;
using Kuestenlogik.Surgewave.Client.Native.Operations.Connect;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Connect;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Connect;

/// <summary>
/// Command to list all connectors.
/// </summary>
public sealed class ListConnectorsCommand : ISurgewaveCommand<IReadOnlyList<string>>
{
    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListConnectors;
    public void WriteRequest(ref SurgewavePayloadWriter writer) { }
    public int EstimateRequestSize() => 0;

    public IReadOnlyList<string> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"ListConnectors failed: {header.ErrorCode}");

        var result = ListConnectorsPayload.Read(ref reader);
        return result.Connectors;
    }
}

/// <summary>
/// Command to get connector information.
/// </summary>
public sealed class GetConnectorCommand : ISurgewaveCommand<ConnectorInfo?>
{
    private readonly string _name;

    public GetConnectorCommand(string name) => _name = name;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetConnector;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => writer.WriteString(_name);

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_name ?? "");

    public ConnectorInfo? ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode == SurgewaveErrorCode.ConnectorNotFound)
            return null;

        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"GetConnector failed: {header.ErrorCode}");

        var result = ConnectorInfoPayload.Read(ref reader);
        var tasks = result.Tasks.Select(t => new ConnectorTaskStatus(
            t.Id, t.State, t.WorkerId, t.Trace)).ToList();

        return new ConnectorInfo(result.Name, result.Type, result.State, result.WorkerId,
            result.Config as Dictionary<string, string> ?? new Dictionary<string, string>(result.Config),
            tasks);
    }
}

/// <summary>
/// Command to create a connector.
/// </summary>
public sealed class CreateConnectorCommand : ISurgewaveCommand<ConnectorCreateResult>
{
    private readonly string _name;
    private readonly Dictionary<string, string> _config;

    public CreateConnectorCommand(string name, Dictionary<string, string> config)
    {
        _name = name;
        _config = config;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CreateConnector;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_name);
        writer.WriteInt32(_config.Count);
        foreach (var (key, value) in _config)
        {
            writer.WriteString(key);
            writer.WriteString(value);
        }
    }

    public int EstimateRequestSize()
    {
        var size = 2 + Encoding.UTF8.GetByteCount(_name ?? "") + 4;
        foreach (var (key, value) in _config)
            size += 2 + Encoding.UTF8.GetByteCount(key ?? "") + 2 + Encoding.UTF8.GetByteCount(value ?? "");
        return size;
    }

    public ConnectorCreateResult ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"CreateConnector failed: {header.ErrorCode}");

        var connectorName = reader.ReadString() ?? _name;
        var taskCount = reader.ReadInt32();
        return new ConnectorCreateResult(connectorName, taskCount);
    }
}

/// <summary>
/// Command to delete a connector.
/// </summary>
public sealed class DeleteConnectorCommand : ISurgewaveVoidCommand
{
    private readonly string _name;

    public DeleteConnectorCommand(string name) => _name = name;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DeleteConnector;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => writer.WriteString(_name);

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_name ?? "");

    public Unit ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        ValidateResponse(ref reader, header);
        return Unit.Value;
    }

    public void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"DeleteConnector failed: {header.ErrorCode}");
    }
}

/// <summary>
/// Command to get connector configuration.
/// </summary>
public sealed class GetConnectorConfigCommand : ISurgewaveCommand<Dictionary<string, string>>
{
    private readonly string _name;

    public GetConnectorConfigCommand(string name) => _name = name;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetConnectorConfig;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => writer.WriteString(_name);

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_name ?? "");

    public Dictionary<string, string> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"GetConnectorConfig failed: {header.ErrorCode}");

        var result = ConnectorConfigPayload.Read(ref reader);
        return result.Config as Dictionary<string, string> ?? new Dictionary<string, string>(result.Config);
    }
}

/// <summary>
/// Command to update connector configuration.
/// </summary>
public sealed class UpdateConnectorConfigCommand : ISurgewaveVoidCommand
{
    private readonly string _name;
    private readonly Dictionary<string, string> _config;

    public UpdateConnectorConfigCommand(string name, Dictionary<string, string> config)
    {
        _name = name;
        _config = config;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.UpdateConnectorConfig;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_name);
        writer.WriteInt32(_config.Count);
        foreach (var (key, value) in _config)
        {
            writer.WriteString(key);
            writer.WriteString(value);
        }
    }

    public int EstimateRequestSize()
    {
        var size = 2 + Encoding.UTF8.GetByteCount(_name ?? "") + 4;
        foreach (var (key, value) in _config)
            size += 2 + Encoding.UTF8.GetByteCount(key ?? "") + 2 + Encoding.UTF8.GetByteCount(value ?? "");
        return size;
    }

    public Unit ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        ValidateResponse(ref reader, header);
        return Unit.Value;
    }

    public void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"UpdateConnectorConfig failed: {header.ErrorCode}");
    }
}

/// <summary>
/// Command to get connector status.
/// </summary>
public sealed class GetConnectorStatusCommand : ISurgewaveCommand<ConnectorStatus?>
{
    private readonly string _name;

    public GetConnectorStatusCommand(string name) => _name = name;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetConnectorStatus;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => writer.WriteString(_name);

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_name ?? "");

    public ConnectorStatus? ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode == SurgewaveErrorCode.ConnectorNotFound)
            return null;

        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"GetConnectorStatus failed: {header.ErrorCode}");

        var result = ConnectorStatusPayload.Read(ref reader);
        var tasks = result.Tasks.Select(t => new ConnectorTaskStatus(
            t.Id, t.State, t.WorkerId, t.Trace)).ToList();

        return new ConnectorStatus(result.Name, result.Type, result.State, result.WorkerId, tasks);
    }
}

/// <summary>
/// Command to restart a connector.
/// </summary>
public sealed class RestartConnectorCommand : ISurgewaveVoidCommand
{
    private readonly string _name;

    public RestartConnectorCommand(string name) => _name = name;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.RestartConnector;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => writer.WriteString(_name);

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_name ?? "");

    public Unit ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        ValidateResponse(ref reader, header);
        return Unit.Value;
    }

    public void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"RestartConnector failed: {header.ErrorCode}");
    }
}

/// <summary>
/// Command to pause a connector.
/// </summary>
public sealed class PauseConnectorCommand : ISurgewaveVoidCommand
{
    private readonly string _name;

    public PauseConnectorCommand(string name) => _name = name;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.PauseConnector;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => writer.WriteString(_name);

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_name ?? "");

    public Unit ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        ValidateResponse(ref reader, header);
        return Unit.Value;
    }

    public void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"PauseConnector failed: {header.ErrorCode}");
    }
}

/// <summary>
/// Command to resume a paused connector.
/// </summary>
public sealed class ResumeConnectorCommand : ISurgewaveVoidCommand
{
    private readonly string _name;

    public ResumeConnectorCommand(string name) => _name = name;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.ResumeConnector;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => writer.WriteString(_name);

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_name ?? "");

    public Unit ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        ValidateResponse(ref reader, header);
        return Unit.Value;
    }

    public void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"ResumeConnector failed: {header.ErrorCode}");
    }
}

/// <summary>
/// Command to get connector tasks.
/// </summary>
public sealed class GetConnectorTasksCommand : ISurgewaveCommand<IReadOnlyList<ConnectorTaskInfo>>
{
    private readonly string _name;

    public GetConnectorTasksCommand(string name) => _name = name;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetConnectorTasks;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => writer.WriteString(_name);

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_name ?? "");

    public IReadOnlyList<ConnectorTaskInfo> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"GetConnectorTasks failed: {header.ErrorCode}");

        var count = reader.ReadInt32();
        var tasks = new List<ConnectorTaskInfo>(count);
        for (int i = 0; i < count; i++)
            tasks.Add(new ConnectorTaskInfo(reader.ReadString() ?? _name, reader.ReadInt32()));
        return tasks;
    }
}

/// <summary>
/// Command to restart a specific connector task.
/// </summary>
public sealed class RestartConnectorTaskCommand : ISurgewaveVoidCommand
{
    private readonly string _name;
    private readonly int _taskId;

    public RestartConnectorTaskCommand(string name, int taskId)
    {
        _name = name;
        _taskId = taskId;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.RestartConnectorTask;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_name);
        writer.WriteInt32(_taskId);
    }

    public int EstimateRequestSize() => 2 + Encoding.UTF8.GetByteCount(_name ?? "") + 4;

    public Unit ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        ValidateResponse(ref reader, header);
        return Unit.Value;
    }

    public void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"RestartConnectorTask failed: {header.ErrorCode}");
    }
}

/// <summary>
/// Command to list available connector plugins.
/// </summary>
public sealed class ListConnectorPluginsCommand : ISurgewaveCommand<IReadOnlyList<PluginInfo>>
{
    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListConnectorPlugins;
    public void WriteRequest(ref SurgewavePayloadWriter writer) { }
    public int EstimateRequestSize() => 0;

    public IReadOnlyList<PluginInfo> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        if (header.ErrorCode != SurgewaveErrorCode.None)
            throw new InvalidOperationException($"ListConnectorPlugins failed: {header.ErrorCode}");

        var count = reader.ReadInt32();
        var plugins = new List<PluginInfo>(count);
        for (int i = 0; i < count; i++)
        {
            plugins.Add(new PluginInfo(
                reader.ReadString() ?? string.Empty,
                reader.ReadString() ?? string.Empty,
                reader.ReadString() ?? string.Empty));
        }
        return plugins;
    }
}
