using Kuestenlogik.Surgewave.Broker.Native.Handlers;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Connect;

namespace Kuestenlogik.Surgewave.Broker.Native.Operations.Connect;

/// <summary>
/// Request payload for connector name operations.
/// </summary>
public readonly record struct ConnectorNameRequest
{
    public required string Name { get; init; }

    public static ConnectorNameRequest Read(ref SurgewavePayloadReader reader)
        => new() { Name = reader.ReadString() ?? string.Empty };
}

/// <summary>
/// Result for list connectors operation.
/// </summary>
public readonly record struct ListConnectorsResult
{
    public required ListConnectorsPayload Response { get; init; }
}

/// <summary>
/// Operation to list all connectors.
/// </summary>
public sealed class ListConnectorsOperation : INoRequestOperationHandler<ListConnectorsResult>
{
    private readonly ConnectWorker _connectWorker;

    public ListConnectorsOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListConnectors;

    public Task<ListConnectorsResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        var connectors = _connectWorker.ListConnectors();
        var response = new ListConnectorsPayload { Connectors = connectors };
        return Task.FromResult(new ListConnectorsResult { Response = response });
    }

    public void WriteResponse(IPayloadWriter writer, in ListConnectorsResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for get connector operation.
/// </summary>
public readonly record struct GetConnectorResult
{
    public required ConnectorInfoPayload Response { get; init; }
}

/// <summary>
/// Operation to get connector info.
/// </summary>
public sealed class GetConnectorOperation : IOperationHandler<ConnectorNameRequest, GetConnectorResult>
{
    private readonly ConnectWorker _connectWorker;

    public GetConnectorOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetConnector;

    public ConnectorNameRequest ParseRequest(ref SurgewavePayloadReader reader)
        => ConnectorNameRequest.Read(ref reader);

    public void ValidateRequest(in ConnectorNameRequest request) { }

    public Task<GetConnectorResult> ExecuteAsync(ConnectorNameRequest request, CancellationToken cancellationToken)
    {
        var info = _connectWorker.GetConnectorStatus(request.Name)
            ?? throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorNotFound, $"Connector '{request.Name}' not found");

        var response = new ConnectorInfoPayload
        {
            Name = info.Name,
            Type = info.Type,
            State = info.State,
            WorkerId = info.WorkerId,
            Config = (IReadOnlyDictionary<string, string>)info.Config,
            Tasks = info.Tasks.Select(t => new ConnectorTaskStatusPayload
            {
                Id = t.Id,
                State = t.State,
                WorkerId = t.WorkerId,
                Trace = null
            }).ToList()
        };

        return Task.FromResult(new GetConnectorResult { Response = response });
    }

    public void WriteResponse(IPayloadWriter writer, in GetConnectorResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Request payload for create connector operation.
/// </summary>
public readonly record struct CreateConnectorRequest
{
    public required string Name { get; init; }
    public required Dictionary<string, string> Config { get; init; }

    public static CreateConnectorRequest Read(ref SurgewavePayloadReader reader)
    {
        var name = reader.ReadString() ?? string.Empty;
        var configCount = reader.ReadInt32();
        var config = new Dictionary<string, string>(configCount);
        for (int i = 0; i < configCount; i++)
        {
            var key = reader.ReadString() ?? string.Empty;
            var value = reader.ReadString() ?? string.Empty;
            config[key] = value;
        }
        return new CreateConnectorRequest { Name = name, Config = config };
    }
}

/// <summary>
/// Result for create connector operation.
/// </summary>
public readonly record struct CreateConnectorResult
{
    public required string Name { get; init; }
    public required int TaskCount { get; init; }
}

/// <summary>
/// Operation to create a connector.
/// </summary>
public sealed class CreateConnectorOperation : IOperationHandler<CreateConnectorRequest, CreateConnectorResult>
{
    private readonly ConnectWorker _connectWorker;

    public CreateConnectorOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CreateConnector;

    public CreateConnectorRequest ParseRequest(ref SurgewavePayloadReader reader)
        => CreateConnectorRequest.Read(ref reader);

    public void ValidateRequest(in CreateConnectorRequest request)
    {
        if (!request.Config.ContainsKey("connector.class"))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidConnectorConfig, "Missing 'connector.class' in configuration");
    }

    public async Task<CreateConnectorResult> ExecuteAsync(CreateConnectorRequest request, CancellationToken cancellationToken)
    {
        var connectorClass = request.Config["connector.class"];

        try
        {
            await _connectWorker.CreateConnectorAsync(request.Name, connectorClass, request.Config);
            var info = _connectWorker.GetConnectorStatus(request.Name);
            return new CreateConnectorResult { Name = request.Name, TaskCount = info?.Tasks.Count ?? 0 };
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("already exists"))
        {
            throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorAlreadyExists, ex.Message);
        }
        catch (Exception ex)
        {
            throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorFailed, ex.Message);
        }
    }

    public void WriteResponse(IPayloadWriter writer, in CreateConnectorResult response)
    {
        writer.WriteString(response.Name);
        writer.WriteInt32(response.TaskCount);
    }
}

/// <summary>
/// Result for delete connector operation.
/// </summary>
public readonly record struct DeleteConnectorResult
{
    public required string Name { get; init; }
}

/// <summary>
/// Operation to delete a connector.
/// </summary>
public sealed class DeleteConnectorOperation : IOperationHandler<ConnectorNameRequest, DeleteConnectorResult>
{
    private readonly ConnectWorker _connectWorker;

    public DeleteConnectorOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DeleteConnector;

    public ConnectorNameRequest ParseRequest(ref SurgewavePayloadReader reader)
        => ConnectorNameRequest.Read(ref reader);

    public void ValidateRequest(in ConnectorNameRequest request) { }

    public async Task<DeleteConnectorResult> ExecuteAsync(ConnectorNameRequest request, CancellationToken cancellationToken)
    {
        _ = _connectWorker.GetConnectorStatus(request.Name)
            ?? throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorNotFound, $"Connector '{request.Name}' not found");

        try
        {
            await _connectWorker.StopConnectorAsync(request.Name);
            return new DeleteConnectorResult { Name = request.Name };
        }
        catch (Exception ex)
        {
            throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorFailed, ex.Message);
        }
    }

    public void WriteResponse(IPayloadWriter writer, in DeleteConnectorResult response)
        => writer.WriteString(response.Name);
}

/// <summary>
/// Result for get connector config operation.
/// </summary>
public readonly record struct GetConnectorConfigResult
{
    public required ConnectorConfigPayload Response { get; init; }
}

/// <summary>
/// Operation to get connector config.
/// </summary>
public sealed class GetConnectorConfigOperation : IOperationHandler<ConnectorNameRequest, GetConnectorConfigResult>
{
    private readonly ConnectWorker _connectWorker;

    public GetConnectorConfigOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetConnectorConfig;

    public ConnectorNameRequest ParseRequest(ref SurgewavePayloadReader reader)
        => ConnectorNameRequest.Read(ref reader);

    public void ValidateRequest(in ConnectorNameRequest request) { }

    public Task<GetConnectorConfigResult> ExecuteAsync(ConnectorNameRequest request, CancellationToken cancellationToken)
    {
        var info = _connectWorker.GetConnectorStatus(request.Name)
            ?? throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorNotFound, $"Connector '{request.Name}' not found");

        var response = new ConnectorConfigPayload { Config = (IReadOnlyDictionary<string, string>)info.Config };
        return Task.FromResult(new GetConnectorConfigResult { Response = response });
    }

    public void WriteResponse(IPayloadWriter writer, in GetConnectorConfigResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Request payload for update connector config operation.
/// </summary>
public readonly record struct UpdateConnectorConfigRequest
{
    public required string Name { get; init; }
    public required Dictionary<string, string> Config { get; init; }

    public static UpdateConnectorConfigRequest Read(ref SurgewavePayloadReader reader)
    {
        var name = reader.ReadString() ?? string.Empty;
        var configCount = reader.ReadInt32();
        var config = new Dictionary<string, string>(configCount);
        for (int i = 0; i < configCount; i++)
        {
            var key = reader.ReadString() ?? string.Empty;
            var value = reader.ReadString() ?? string.Empty;
            config[key] = value;
        }
        return new UpdateConnectorConfigRequest { Name = name, Config = config };
    }
}

/// <summary>
/// Result for update connector config operation.
/// </summary>
public readonly record struct UpdateConnectorConfigResult
{
    public required string Name { get; init; }
}

/// <summary>
/// Operation to update connector config.
/// </summary>
public sealed class UpdateConnectorConfigOperation : IOperationHandler<UpdateConnectorConfigRequest, UpdateConnectorConfigResult>
{
    private readonly ConnectWorker _connectWorker;

    public UpdateConnectorConfigOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.UpdateConnectorConfig;

    public UpdateConnectorConfigRequest ParseRequest(ref SurgewavePayloadReader reader)
        => UpdateConnectorConfigRequest.Read(ref reader);

    public void ValidateRequest(in UpdateConnectorConfigRequest request) { }

    public async Task<UpdateConnectorConfigResult> ExecuteAsync(UpdateConnectorConfigRequest request, CancellationToken cancellationToken)
    {
        var info = _connectWorker.GetConnectorStatus(request.Name)
            ?? throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorNotFound, $"Connector '{request.Name}' not found");

        try
        {
            await _connectWorker.StopConnectorAsync(request.Name);
            var connectorClass = request.Config.TryGetValue("connector.class", out var cls) ? cls : info.Config["connector.class"];
            await _connectWorker.CreateConnectorAsync(request.Name, connectorClass, request.Config);
            return new UpdateConnectorConfigResult { Name = request.Name };
        }
        catch (Exception ex)
        {
            throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorFailed, ex.Message);
        }
    }

    public void WriteResponse(IPayloadWriter writer, in UpdateConnectorConfigResult response)
        => writer.WriteString(response.Name);
}

/// <summary>
/// Result for get connector status operation.
/// </summary>
public readonly record struct GetConnectorStatusResult
{
    public required ConnectorStatusPayload Response { get; init; }
}

/// <summary>
/// Operation to get connector status.
/// </summary>
public sealed class GetConnectorStatusOperation : IOperationHandler<ConnectorNameRequest, GetConnectorStatusResult>
{
    private readonly ConnectWorker _connectWorker;

    public GetConnectorStatusOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetConnectorStatus;

    public ConnectorNameRequest ParseRequest(ref SurgewavePayloadReader reader)
        => ConnectorNameRequest.Read(ref reader);

    public void ValidateRequest(in ConnectorNameRequest request) { }

    public Task<GetConnectorStatusResult> ExecuteAsync(ConnectorNameRequest request, CancellationToken cancellationToken)
    {
        var info = _connectWorker.GetConnectorStatus(request.Name)
            ?? throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorNotFound, $"Connector '{request.Name}' not found");

        var response = new ConnectorStatusPayload
        {
            Name = info.Name,
            Type = info.Type,
            State = info.State,
            WorkerId = info.WorkerId,
            Tasks = info.Tasks.Select(t => new ConnectorTaskStatusPayload
            {
                Id = t.Id,
                State = t.State,
                WorkerId = t.WorkerId,
                Trace = t.Trace
            }).ToList()
        };

        return Task.FromResult(new GetConnectorStatusResult { Response = response });
    }

    public void WriteResponse(IPayloadWriter writer, in GetConnectorStatusResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for restart connector operation.
/// </summary>
public readonly record struct RestartConnectorResult
{
    public required string Name { get; init; }
}

/// <summary>
/// Operation to restart a connector.
/// </summary>
public sealed class RestartConnectorOperation : IOperationHandler<ConnectorNameRequest, RestartConnectorResult>
{
    private readonly ConnectWorker _connectWorker;

    public RestartConnectorOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.RestartConnector;

    public ConnectorNameRequest ParseRequest(ref SurgewavePayloadReader reader)
        => ConnectorNameRequest.Read(ref reader);

    public void ValidateRequest(in ConnectorNameRequest request) { }

    public async Task<RestartConnectorResult> ExecuteAsync(ConnectorNameRequest request, CancellationToken cancellationToken)
    {
        _ = _connectWorker.GetConnectorStatus(request.Name)
            ?? throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorNotFound, $"Connector '{request.Name}' not found");

        try
        {
            await _connectWorker.RestartConnectorAsync(request.Name);
            return new RestartConnectorResult { Name = request.Name };
        }
        catch (Exception ex)
        {
            throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorFailed, ex.Message);
        }
    }

    public void WriteResponse(IPayloadWriter writer, in RestartConnectorResult response)
        => writer.WriteString(response.Name);
}

/// <summary>
/// Result for pause connector operation.
/// </summary>
public readonly record struct PauseConnectorResult
{
    public required string Name { get; init; }
}

/// <summary>
/// Operation to pause a connector.
/// </summary>
public sealed class PauseConnectorOperation : IOperationHandler<ConnectorNameRequest, PauseConnectorResult>
{
    private readonly ConnectWorker _connectWorker;

    public PauseConnectorOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.PauseConnector;

    public ConnectorNameRequest ParseRequest(ref SurgewavePayloadReader reader)
        => ConnectorNameRequest.Read(ref reader);

    public void ValidateRequest(in ConnectorNameRequest request) { }

    public async Task<PauseConnectorResult> ExecuteAsync(ConnectorNameRequest request, CancellationToken cancellationToken)
    {
        _ = _connectWorker.GetConnectorStatus(request.Name)
            ?? throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorNotFound, $"Connector '{request.Name}' not found");

        try
        {
            await _connectWorker.PauseConnectorAsync(request.Name);
            return new PauseConnectorResult { Name = request.Name };
        }
        catch (Exception ex)
        {
            throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorFailed, ex.Message);
        }
    }

    public void WriteResponse(IPayloadWriter writer, in PauseConnectorResult response)
        => writer.WriteString(response.Name);
}

/// <summary>
/// Result for resume connector operation.
/// </summary>
public readonly record struct ResumeConnectorResult
{
    public required string Name { get; init; }
}

/// <summary>
/// Operation to resume a connector.
/// </summary>
public sealed class ResumeConnectorOperation : IOperationHandler<ConnectorNameRequest, ResumeConnectorResult>
{
    private readonly ConnectWorker _connectWorker;

    public ResumeConnectorOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.ResumeConnector;

    public ConnectorNameRequest ParseRequest(ref SurgewavePayloadReader reader)
        => ConnectorNameRequest.Read(ref reader);

    public void ValidateRequest(in ConnectorNameRequest request) { }

    public async Task<ResumeConnectorResult> ExecuteAsync(ConnectorNameRequest request, CancellationToken cancellationToken)
    {
        _ = _connectWorker.GetConnectorStatus(request.Name)
            ?? throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorNotFound, $"Connector '{request.Name}' not found");

        try
        {
            await _connectWorker.ResumeConnectorAsync(request.Name);
            return new ResumeConnectorResult { Name = request.Name };
        }
        catch (Exception ex)
        {
            throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorFailed, ex.Message);
        }
    }

    public void WriteResponse(IPayloadWriter writer, in ResumeConnectorResult response)
        => writer.WriteString(response.Name);
}

/// <summary>
/// Result for get connector tasks operation.
/// </summary>
public readonly record struct GetConnectorTasksResult
{
    public required string Name { get; init; }
    public required IReadOnlyList<int> TaskIds { get; init; }
}

/// <summary>
/// Operation to get connector tasks.
/// </summary>
public sealed class GetConnectorTasksOperation : IOperationHandler<ConnectorNameRequest, GetConnectorTasksResult>
{
    private readonly ConnectWorker _connectWorker;

    public GetConnectorTasksOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.GetConnectorTasks;

    public ConnectorNameRequest ParseRequest(ref SurgewavePayloadReader reader)
        => ConnectorNameRequest.Read(ref reader);

    public void ValidateRequest(in ConnectorNameRequest request) { }

    public Task<GetConnectorTasksResult> ExecuteAsync(ConnectorNameRequest request, CancellationToken cancellationToken)
    {
        var info = _connectWorker.GetConnectorStatus(request.Name)
            ?? throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorNotFound, $"Connector '{request.Name}' not found");

        return Task.FromResult(new GetConnectorTasksResult
        {
            Name = request.Name,
            TaskIds = info.Tasks.Select(t => t.Id).ToList()
        });
    }

    public void WriteResponse(IPayloadWriter writer, in GetConnectorTasksResult response)
    {
        writer.WriteInt32(response.TaskIds.Count);
        foreach (var taskId in response.TaskIds)
        {
            writer.WriteString(response.Name);
            writer.WriteInt32(taskId);
        }
    }
}

/// <summary>
/// Request payload for restart connector task operation.
/// </summary>
public readonly record struct RestartConnectorTaskRequest
{
    public required string Name { get; init; }
    public required int TaskId { get; init; }

    public static RestartConnectorTaskRequest Read(ref SurgewavePayloadReader reader)
        => new()
        {
            Name = reader.ReadString() ?? string.Empty,
            TaskId = reader.ReadInt32()
        };
}

/// <summary>
/// Result for restart connector task operation.
/// </summary>
public readonly record struct RestartConnectorTaskResult
{
    public required string Name { get; init; }
    public required int TaskId { get; init; }
}

/// <summary>
/// Operation to restart a connector task.
/// </summary>
public sealed class RestartConnectorTaskOperation : IOperationHandler<RestartConnectorTaskRequest, RestartConnectorTaskResult>
{
    private readonly ConnectWorker _connectWorker;

    public RestartConnectorTaskOperation(ConnectWorker connectWorker) => _connectWorker = connectWorker;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.RestartConnectorTask;

    public RestartConnectorTaskRequest ParseRequest(ref SurgewavePayloadReader reader)
        => RestartConnectorTaskRequest.Read(ref reader);

    public void ValidateRequest(in RestartConnectorTaskRequest request) { }

    public async Task<RestartConnectorTaskResult> ExecuteAsync(RestartConnectorTaskRequest request, CancellationToken cancellationToken)
    {
        var info = _connectWorker.GetConnectorStatus(request.Name)
            ?? throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorNotFound, $"Connector '{request.Name}' not found");

        if (!info.Tasks.Any(t => t.Id == request.TaskId))
            throw new SurgewaveOperationException(SurgewaveErrorCode.TaskNotFound, $"Task {request.TaskId} not found in connector '{request.Name}'");

        try
        {
            await _connectWorker.RestartTaskAsync(request.Name, request.TaskId);
            return new RestartConnectorTaskResult { Name = request.Name, TaskId = request.TaskId };
        }
        catch (Exception ex)
        {
            throw new SurgewaveOperationException(SurgewaveErrorCode.ConnectorFailed, ex.Message);
        }
    }

    public void WriteResponse(IPayloadWriter writer, in RestartConnectorTaskResult response)
    {
        writer.WriteString(response.Name);
        writer.WriteInt32(response.TaskId);
    }
}

/// <summary>
/// Result for list connector plugins operation.
/// </summary>
public readonly record struct ListConnectorPluginsResult
{
    public required IReadOnlyList<(string ClassName, string Type, string Version)> Plugins { get; init; }
}

/// <summary>
/// Operation to list connector plugins.
/// </summary>
public sealed class ListConnectorPluginsOperation : INoRequestOperationHandler<ListConnectorPluginsResult>
{
    private readonly PluginDiscovery? _pluginDiscovery;

    public ListConnectorPluginsOperation(PluginDiscovery? pluginDiscovery)
    {
        _pluginDiscovery = pluginDiscovery;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListConnectorPlugins;

    public Task<ListConnectorPluginsResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        var plugins = _pluginDiscovery?.GetAllPlugins()
            .Select(p => (p.Class, p.Type, p.Version))
            .ToList() ?? [];

        return Task.FromResult(new ListConnectorPluginsResult { Plugins = plugins });
    }

    public void WriteResponse(IPayloadWriter writer, in ListConnectorPluginsResult response)
    {
        writer.WriteInt32(response.Plugins.Count);
        foreach (var (className, type, version) in response.Plugins)
        {
            writer.WriteString(className);
            writer.WriteString(type);
            writer.WriteString(version);
        }
    }
}
