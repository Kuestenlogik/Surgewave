using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Connector info DTO.
/// </summary>
public record ConnectorInfoDto(
    string Name,
    string Type,
    string State,
    string WorkerId,
    Dictionary<string, string> Config,
    List<TaskStatusDto> Tasks);

/// <summary>
/// Task status DTO.
/// </summary>
public record TaskStatusDto(int Id, string State, string WorkerId, string? Trace);

/// <summary>
/// Connector plugin DTO.
/// </summary>
public record ConnectorPluginDto(string ClassName, string Type, string Version);

/// <summary>
/// Delegate to list connectors.
/// </summary>
public delegate List<string> ListConnectorsDelegate();

/// <summary>
/// Delegate to get connector info.
/// </summary>
public delegate ConnectorInfoDto? GetConnectorDelegate(string name);

/// <summary>
/// Delegate to create a connector.
/// </summary>
public delegate Task<string> CreateConnectorDelegate(string name, Dictionary<string, string> config);

/// <summary>
/// Delegate to delete a connector.
/// </summary>
public delegate Task DeleteConnectorDelegate(string name);

/// <summary>
/// Delegate to update connector config.
/// </summary>
public delegate Task<ConnectorInfoDto?> UpdateConnectorConfigDelegate(string name, Dictionary<string, string> config);

/// <summary>
/// Delegate to restart a connector.
/// </summary>
public delegate Task RestartConnectorDelegate(string name, bool includeTasks, bool onlyFailed);

/// <summary>
/// Delegate to pause a connector.
/// </summary>
public delegate Task PauseConnectorDelegate(string name);

/// <summary>
/// Delegate to resume a connector.
/// </summary>
public delegate Task ResumeConnectorDelegate(string name);

/// <summary>
/// Delegate to restart a connector task.
/// </summary>
public delegate Task RestartConnectorTaskDelegate(string connector, int taskId);

/// <summary>
/// Delegate to list connector plugins.
/// </summary>
public delegate List<ConnectorPluginDto> ListConnectorPluginsDelegate(bool includeSink, bool includeSource);

/// <summary>
/// gRPC ConnectService implementation.
/// </summary>
public class ConnectServiceImpl : ConnectService.ConnectServiceBase
{
    private readonly ListConnectorsDelegate _listConnectors;
    private readonly GetConnectorDelegate _getConnector;
    private readonly CreateConnectorDelegate _createConnector;
    private readonly DeleteConnectorDelegate _deleteConnector;
    private readonly UpdateConnectorConfigDelegate _updateConnectorConfig;
    private readonly RestartConnectorDelegate _restartConnector;
    private readonly PauseConnectorDelegate _pauseConnector;
    private readonly ResumeConnectorDelegate _resumeConnector;
    private readonly RestartConnectorTaskDelegate _restartConnectorTask;
    private readonly ListConnectorPluginsDelegate _listConnectorPlugins;

    public ConnectServiceImpl(
        ListConnectorsDelegate listConnectors,
        GetConnectorDelegate getConnector,
        CreateConnectorDelegate createConnector,
        DeleteConnectorDelegate deleteConnector,
        UpdateConnectorConfigDelegate updateConnectorConfig,
        RestartConnectorDelegate restartConnector,
        PauseConnectorDelegate pauseConnector,
        ResumeConnectorDelegate resumeConnector,
        RestartConnectorTaskDelegate restartConnectorTask,
        ListConnectorPluginsDelegate listConnectorPlugins)
    {
        _listConnectors = listConnectors;
        _getConnector = getConnector;
        _createConnector = createConnector;
        _deleteConnector = deleteConnector;
        _updateConnectorConfig = updateConnectorConfig;
        _restartConnector = restartConnector;
        _pauseConnector = pauseConnector;
        _resumeConnector = resumeConnector;
        _restartConnectorTask = restartConnectorTask;
        _listConnectorPlugins = listConnectorPlugins;
    }

    public override Task<ListConnectorsResponse> ListConnectors(ListConnectorsRequest request, ServerCallContext context)
    {
        var connectors = _listConnectors();

        var response = new ListConnectorsResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };
        response.Connectors.AddRange(connectors);

        return Task.FromResult(response);
    }

    public override Task<GetConnectorResponse> GetConnector(GetConnectorRequest request, ServerCallContext context)
    {
        var info = _getConnector(request.Name);

        if (info == null)
        {
            return Task.FromResult(new GetConnectorResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = $"Connector '{request.Name}' not found"
                }
            });
        }

        var response = new GetConnectorResponse
        {
            Name = info.Name,
            Type = info.Type,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };
        response.Config.Add(info.Config);
        foreach (var task in info.Tasks)
        {
            response.Tasks.Add(new TaskInfo
            {
                Connector = info.Name,
                TaskId = task.Id
            });
        }

        return Task.FromResult(response);
    }

    public override async Task<CreateConnectorResponse> CreateConnector(CreateConnectorRequest request, ServerCallContext context)
    {
        try
        {
            var config = new Dictionary<string, string>(request.Config);
            var name = await _createConnector(request.Name, config);

            var info = _getConnector(name);
            if (info == null)
            {
                return new CreateConnectorResponse
                {
                    Name = name,
                    Status = new ResponseStatus { ErrorCode = ErrorCode.None }
                };
            }

            var response = new CreateConnectorResponse
            {
                Name = info.Name,
                Type = info.Type,
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
            response.Config.Add(info.Config);
            foreach (var task in info.Tasks)
            {
                response.Tasks.Add(new TaskInfo
                {
                    Connector = info.Name,
                    TaskId = task.Id
                });
            }

            return response;
        }
        catch (Exception ex)
        {
            return new CreateConnectorResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<DeleteConnectorResponse> DeleteConnector(DeleteConnectorRequest request, ServerCallContext context)
    {
        try
        {
            await _deleteConnector(request.Name);
            return new DeleteConnectorResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (Exception ex)
        {
            return new DeleteConnectorResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override Task<GetConnectorConfigResponse> GetConnectorConfig(GetConnectorConfigRequest request, ServerCallContext context)
    {
        var info = _getConnector(request.Name);

        if (info == null)
        {
            return Task.FromResult(new GetConnectorConfigResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = $"Connector '{request.Name}' not found"
                }
            });
        }

        var response = new GetConnectorConfigResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };
        response.Config.Add(info.Config);

        return Task.FromResult(response);
    }

    public override async Task<UpdateConnectorConfigResponse> UpdateConnectorConfig(UpdateConnectorConfigRequest request, ServerCallContext context)
    {
        try
        {
            var config = new Dictionary<string, string>(request.Config);
            var info = await _updateConnectorConfig(request.Name, config);

            if (info == null)
            {
                return new UpdateConnectorConfigResponse
                {
                    Status = new ResponseStatus
                    {
                        ErrorCode = ErrorCode.Unknown,
                        ErrorMessage = $"Connector '{request.Name}' not found"
                    }
                };
            }

            var response = new UpdateConnectorConfigResponse
            {
                Name = info.Name,
                Type = info.Type,
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
            response.Config.Add(info.Config);
            foreach (var task in info.Tasks)
            {
                response.Tasks.Add(new TaskInfo
                {
                    Connector = info.Name,
                    TaskId = task.Id
                });
            }

            return response;
        }
        catch (Exception ex)
        {
            return new UpdateConnectorConfigResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override Task<GetConnectorStatusResponse> GetConnectorStatus(GetConnectorStatusRequest request, ServerCallContext context)
    {
        var info = _getConnector(request.Name);

        if (info == null)
        {
            return Task.FromResult(new GetConnectorStatusResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = $"Connector '{request.Name}' not found"
                }
            });
        }

        var response = new GetConnectorStatusResponse
        {
            Name = info.Name,
            Type = info.Type,
            ConnectorState = new ConnectorState
            {
                State = info.State,
                WorkerId = info.WorkerId
            },
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        foreach (var task in info.Tasks)
        {
            response.TaskStates.Add(new TaskState
            {
                Id = task.Id,
                State = task.State,
                WorkerId = task.WorkerId,
                Trace = task.Trace ?? ""
            });
        }

        return Task.FromResult(response);
    }

    public override async Task<RestartConnectorResponse> RestartConnector(RestartConnectorRequest request, ServerCallContext context)
    {
        try
        {
            await _restartConnector(request.Name, request.IncludeTasks, request.OnlyFailed);
            return new RestartConnectorResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (Exception ex)
        {
            return new RestartConnectorResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<PauseConnectorResponse> PauseConnector(PauseConnectorRequest request, ServerCallContext context)
    {
        try
        {
            await _pauseConnector(request.Name);
            return new PauseConnectorResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (Exception ex)
        {
            return new PauseConnectorResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<ResumeConnectorResponse> ResumeConnector(ResumeConnectorRequest request, ServerCallContext context)
    {
        try
        {
            await _resumeConnector(request.Name);
            return new ResumeConnectorResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (Exception ex)
        {
            return new ResumeConnectorResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override Task<GetConnectorTasksResponse> GetConnectorTasks(GetConnectorTasksRequest request, ServerCallContext context)
    {
        var info = _getConnector(request.Name);

        if (info == null)
        {
            return Task.FromResult(new GetConnectorTasksResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = $"Connector '{request.Name}' not found"
                }
            });
        }

        var response = new GetConnectorTasksResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        foreach (var task in info.Tasks)
        {
            var connectorTask = new ConnectorTask
            {
                Id = new TaskInfo
                {
                    Connector = info.Name,
                    TaskId = task.Id
                }
            };
            // Task configs aren't exposed separately, so we use connector config
            connectorTask.Config.Add(info.Config);
            response.Tasks.Add(connectorTask);
        }

        return Task.FromResult(response);
    }

    public override async Task<RestartConnectorTaskResponse> RestartConnectorTask(RestartConnectorTaskRequest request, ServerCallContext context)
    {
        try
        {
            await _restartConnectorTask(request.Connector, request.TaskId);
            return new RestartConnectorTaskResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (Exception ex)
        {
            return new RestartConnectorTaskResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override Task<ListConnectorPluginsResponse> ListConnectorPlugins(ListConnectorPluginsRequest request, ServerCallContext context)
    {
        var plugins = _listConnectorPlugins(request.IncludeSink, request.IncludeSource);

        var response = new ListConnectorPluginsResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        foreach (var plugin in plugins)
        {
            response.Plugins.Add(new ConnectorPlugin
            {
                ClassName = plugin.ClassName,
                Type = plugin.Type,
                Version = plugin.Version
            });
        }

        return Task.FromResult(response);
    }
}
