using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;
using Kuestenlogik.Surgewave.Connect.Distributed;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Kuestenlogik.Surgewave.Connect.Plugins;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Kuestenlogik.Bowire;

namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// REST API for managing connectors, compatible with Kafka Connect REST API.
/// Uses ASP.NET Core minimal APIs with Bowire for interactive API browsing
/// (replaces Scalar — same OpenAPI source, Bowire is the Surgewave-wide
/// default workbench for both REST and gRPC surfaces).
/// </summary>
public static class ConnectRestApi
{
    /// <summary>
    /// Adds Connect services to the service collection.
    /// </summary>
    public static IServiceCollection AddSurgewaveConnect(this IServiceCollection services, ConnectWorkerConfig config)
    {
        services.AddSingleton(config);
        services.AddSingleton<PluginLoader>();
        services.AddSingleton<PluginDiscovery>();
        services.AddSingleton<AggregatedConnectorRegistry>();
        services.AddSingleton<ConnectWorker>();
        services.AddOpenApi(options =>
        {
            options.AddDocumentTransformer((document, context, cancellationToken) =>
            {
                document.Info.Title = "Surgewave Connect API";
                document.Info.Version = "1.0.0";
                document.Info.Description = "Kafka Connect compatible REST API for managing connectors";
                return Task.CompletedTask;
            });
        });

        // Initialize plugin discovery
        services.AddHostedService<PluginDiscoveryHostedService>();

        return services;
    }

    /// <summary>
    /// Maps Connect REST API endpoints.
    /// </summary>
    public static IEndpointRouteBuilder MapSurgewaveConnect(this IEndpointRouteBuilder app)
    {
        app.MapOpenApi();
        app.MapBowire("/bowire", options =>
        {
            options.Title = "Surgewave Connect API";
            options.Description = "Interactive browser for the Kafka-Connect-compatible REST API";
        });

        var group = app.MapGroup("")
            .WithTags("Connect");

        // Cluster info
        group.MapGet("/", GetClusterInfo)
            .WithName("GetClusterInfo")
            .WithSummary("Get Connect cluster info")
            .Produces<ClusterInfoResponse>();

        // Connectors
        group.MapGet("/connectors", ListConnectors)
            .WithName("ListConnectors")
            .WithSummary("List all connectors")
            .Produces<IReadOnlyList<string>>();

        group.MapPost("/connectors", CreateConnector)
            .WithName("CreateConnector")
            .WithSummary("Create a new connector")
            .Produces<ConnectorResponse>(StatusCodes.Status201Created)
            .ProducesValidationProblem();

        group.MapGet("/connectors/{name}", GetConnector)
            .WithName("GetConnector")
            .WithSummary("Get connector info")
            .Produces<ConnectorResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapDelete("/connectors/{name}", DeleteConnector)
            .WithName("DeleteConnector")
            .WithSummary("Delete a connector")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/connectors/{name}/config", GetConnectorConfig)
            .WithName("GetConnectorConfig")
            .WithSummary("Get connector configuration")
            .Produces<IDictionary<string, string>>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPut("/connectors/{name}/config", UpdateConnectorConfig)
            .WithName("UpdateConnectorConfig")
            .WithSummary("Update connector configuration")
            .Produces<ConnectorResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/connectors/{name}/status", GetConnectorStatus)
            .WithName("GetConnectorStatus")
            .WithSummary("Get connector status")
            .Produces<ConnectorStatusResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/connectors/{name}/restart", RestartConnector)
            .WithName("RestartConnector")
            .WithSummary("Restart a connector")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPut("/connectors/{name}/pause", PauseConnector)
            .WithName("PauseConnector")
            .WithSummary("Pause a connector")
            .Produces(StatusCodes.Status202Accepted)
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPut("/connectors/{name}/resume", ResumeConnector)
            .WithName("ResumeConnector")
            .WithSummary("Resume a connector")
            .Produces(StatusCodes.Status202Accepted)
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/connectors/{name}/tasks", GetConnectorTasks)
            .WithName("GetConnectorTasks")
            .WithSummary("Get connector tasks")
            .Produces<IReadOnlyList<TaskInfo>>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/connectors/{name}/tasks/{taskId:int}/restart", RestartTask)
            .WithName("RestartTask")
            .WithSummary("Restart a specific task")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Connector plugins (aggregated: local + remote worker capabilities)
        group.MapGet("/connector-plugins", GetConnectorPlugins)
            .WithName("GetConnectorPlugins")
            .WithSummary("List available connector plugins (local and remote)")
            .Produces<IReadOnlyList<AggregatedConnectorType>>();

        // Exactly-once source offsets
        group.MapGet("/connectors/{name}/offsets", GetSourceOffsets)
            .WithName("GetSourceOffsets")
            .WithSummary("Get exactly-once source offsets for a connector")
            .Produces<IReadOnlyDictionary<string, Dictionary<string, string>>>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapDelete("/connectors/{name}/offsets", DeleteSourceOffsets)
            .WithName("DeleteSourceOffsets")
            .WithSummary("Delete all exactly-once source offsets (for reprocessing)")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/connectors/{name}/offsets/reset", ResetSourceOffsets)
            .WithName("ResetSourceOffsets")
            .WithSummary("Reset exactly-once source offsets to specific values")
            .Produces(StatusCodes.Status204NoContent)
            .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }

    private static ClusterInfoResponse GetClusterInfo()
    {
        return new ClusterInfoResponse
        {
            Version = "1.0.0",
            Commit = "surgewave",
            KafkaClusterId = "surgewave-cluster"
        };
    }

    private static IReadOnlyList<string> ListConnectors(ConnectWorker worker)
    {
        return worker.ListConnectors();
    }

    private static async Task<IResult> CreateConnector(
        CreateConnectorRequest request,
        ConnectWorker worker)
    {
        if (string.IsNullOrEmpty(request.Name))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["name"] = ["Connector name is required"]
            });
        }

        if (request.Config == null || !request.Config.TryGetValue("connector.class", out var connectorClass))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["config.connector.class"] = ["connector.class is required in config"]
            });
        }

        try
        {
            await worker.CreateConnectorAsync(request.Name, connectorClass, request.Config);

            var info = worker.GetConnectorStatus(request.Name);
            var response = new ConnectorResponse
            {
                Name = request.Name,
                Config = request.Config,
                Tasks = info?.Tasks.Select(t => new TaskId { Connector = request.Name, Task = t.Id }).ToList() ?? [],
                Type = info?.Type ?? "unknown"
            };

            return Results.Created($"/connectors/{request.Name}", response);
        }
        catch (InvalidOperationException ex)
        {
            return Results.Conflict(new ProblemDetails { Detail = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return Results.BadRequest(new ProblemDetails { Detail = ex.Message });
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Forwarding errors are non-fatal")]
    private static async Task<IResult> GetConnector(
        string name,
        ConnectWorker worker,
        ConnectorRequestForwarder? forwarder = null)
    {
        // Check if connector is on a remote worker
        if (forwarder != null)
        {
            var owningWorkerId = forwarder.GetOwningWorkerId(name);
            if (owningWorkerId != null)
            {
                try
                {
                    var response = await forwarder.ForwardToWorkerAsync(owningWorkerId, $"/connectors/{name}");
                    if (response != null && response.IsSuccessStatusCode)
                    {
                        var content = await response.Content.ReadAsStringAsync();
                        return Results.Content(content, "application/json", statusCode: (int)response.StatusCode);
                    }
                }
                catch
                {
                    // Fall through
                }
            }
        }

        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        return Results.Ok(new ConnectorResponse
        {
            Name = info.Name,
            Config = new Dictionary<string, string>(info.Config),
            Tasks = info.Tasks.Select(t => new TaskId { Connector = name, Task = t.Id }).ToList(),
            Type = info.Type
        });
    }

    private static async Task<IResult> DeleteConnector(string name, ConnectWorker worker)
    {
        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        await worker.StopConnectorAsync(name);
        return Results.NoContent();
    }

    private static IResult GetConnectorConfig(string name, ConnectWorker worker)
    {
        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        return Results.Ok(info.Config);
    }

    private static async Task<IResult> UpdateConnectorConfig(
        string name,
        Dictionary<string, string> config,
        ConnectWorker worker)
    {
        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        if (!config.TryGetValue("connector.class", out var connectorClass))
        {
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["connector.class"] = ["connector.class is required in config"]
            });
        }

        // Stop and recreate with new config
        await worker.StopConnectorAsync(name);
        await worker.CreateConnectorAsync(name, connectorClass, config);

        info = worker.GetConnectorStatus(name);
        return Results.Ok(new ConnectorResponse
        {
            Name = name,
            Config = config,
            Tasks = info?.Tasks.Select(t => new TaskId { Connector = name, Task = t.Id }).ToList() ?? [],
            Type = info?.Type ?? "unknown"
        });
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Forwarding errors are non-fatal")]
    private static async Task<IResult> GetConnectorStatus(
        string name,
        ConnectWorker worker,
        ConnectorRequestForwarder? forwarder = null)
    {
        // Check if connector is on a remote worker
        if (forwarder != null)
        {
            var owningWorkerId = forwarder.GetOwningWorkerId(name);
            if (owningWorkerId != null)
            {
                try
                {
                    var response = await forwarder.ForwardToWorkerAsync(owningWorkerId, $"/connectors/{name}/status");
                    if (response != null && response.IsSuccessStatusCode)
                    {
                        var content = await response.Content.ReadAsStringAsync();
                        return Results.Content(content, "application/json", statusCode: (int)response.StatusCode);
                    }
                }
                catch
                {
                    // Fall through to local check
                }

                // Worker might be unavailable, return a placeholder status
                return Results.Ok(new ConnectorStatusResponse
                {
                    Name = name,
                    Connector = new ConnectorStateInfo
                    {
                        State = "RUNNING",
                        WorkerId = owningWorkerId
                    },
                    Tasks = [],
                    Type = "unknown"
                });
            }
        }

        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        return Results.Ok(new ConnectorStatusResponse
        {
            Name = info.Name,
            Connector = new ConnectorStateInfo
            {
                State = info.State,
                WorkerId = info.WorkerId
            },
            Tasks = info.Tasks.Select(t => new TaskStateInfo
            {
                Id = t.Id,
                State = t.State,
                WorkerId = t.WorkerId,
                Trace = t.Trace
            }).ToList(),
            Type = info.Type
        });
    }

    private static async Task<IResult> RestartConnector(string name, ConnectWorker worker)
    {
        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        var config = new Dictionary<string, string>(info.Config);
        await worker.StopConnectorAsync(name);
        await worker.CreateConnectorAsync(name, config["connector.class"], config);

        return Results.NoContent();
    }

    private static async Task<IResult> PauseConnector(string name, ConnectWorker worker)
    {
        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        await worker.PauseConnectorAsync(name);
        return Results.Accepted();
    }

    private static async Task<IResult> ResumeConnector(string name, ConnectWorker worker)
    {
        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        await worker.ResumeConnectorAsync(name);
        return Results.Accepted();
    }

    private static IResult GetConnectorTasks(string name, ConnectWorker worker)
    {
        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        return Results.Ok(info.Tasks.Select(t => new TaskInfo
        {
            Id = new TaskId { Connector = name, Task = t.Id },
            Config = new Dictionary<string, string>()
        }).ToList());
    }

    private static async Task<IResult> RestartTask(string name, int taskId, ConnectWorker worker)
    {
        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        if (!info.Tasks.Any(t => t.Id == taskId))
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Task {taskId} not found in connector '{name}'" });
        }

        await worker.RestartTaskAsync(name, taskId);
        return Results.NoContent();
    }

    private static IReadOnlyList<AggregatedConnectorType> GetConnectorPlugins(
        PluginDiscovery discovery,
        AggregatedConnectorRegistry registry)
    {
        // Ensure local plugins are up-to-date in the aggregated registry
        registry.UpdateFromLocalPlugins(discovery.GetAllPlugins());
        return registry.GetAllTypes();
    }

    private static async Task<IResult> GetSourceOffsets(string name, ConnectWorker worker)
    {
        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        var offsets = await worker.GetSourceOffsetsAsync(name);
        if (offsets == null)
        {
            return Results.Ok(new Dictionary<string, Dictionary<string, string>>());
        }

        return Results.Ok(offsets);
    }

    private static async Task<IResult> DeleteSourceOffsets(string name, ConnectWorker worker)
    {
        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        await worker.DeleteSourceOffsetsAsync(name);
        return Results.NoContent();
    }

    private static async Task<IResult> ResetSourceOffsets(
        string name,
        Dictionary<string, Dictionary<string, string>> offsets,
        ConnectWorker worker)
    {
        var info = worker.GetConnectorStatus(name);
        if (info == null)
        {
            return Results.NotFound(new ProblemDetails { Detail = $"Connector '{name}' not found" });
        }

        await worker.ResetSourceOffsetsAsync(name, offsets);
        return Results.NoContent();
    }

}
