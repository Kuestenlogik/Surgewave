using System.Diagnostics.CodeAnalysis;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Distributed;

/// <summary>
/// Forwards REST API requests for connectors running on remote workers.
/// When a connector is assigned to a remote worker, this service proxies
/// HTTP requests to that worker's REST endpoint.
/// </summary>
public sealed class ConnectorRequestForwarder : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly WorkerCoordinator _coordinator;
    private readonly TaskAssignmentTracker _assignmentTracker;
    private readonly ILogger<ConnectorRequestForwarder> _logger;

    public ConnectorRequestForwarder(
        WorkerCoordinator coordinator,
        TaskAssignmentTracker assignmentTracker,
        ILogger<ConnectorRequestForwarder> logger)
    {
        _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        _coordinator = coordinator;
        _assignmentTracker = assignmentTracker;
        _logger = logger;
    }

    /// <summary>
    /// Checks if a connector is running on a remote worker.
    /// </summary>
    public bool IsRemoteConnector(string connectorName)
    {
        return _assignmentTracker.GetAssignment(connectorName) != null;
    }

    /// <summary>
    /// Gets the worker ID that owns a connector, or null if locally owned.
    /// </summary>
    public string? GetOwningWorkerId(string connectorName)
    {
        return _assignmentTracker.GetOwningWorker(connectorName);
    }

    /// <summary>
    /// Forwards a GET request to the worker that owns the specified connector.
    /// Returns the response content as a string, or null if the worker is unavailable.
    /// </summary>
    [SuppressMessage("Design", "CA1054:URI-like parameters should not be strings", Justification = "Path segment, not a full URI")]
    public async Task<HttpResponseMessage?> ForwardToWorkerAsync(string workerId, string path)
    {
        var workerRestUrl = GetWorkerRestUrl(workerId);
        if (workerRestUrl == null)
        {
            _logger.LogWarning("Cannot forward request to worker {WorkerId}: REST URL unknown", workerId);
            return null;
        }

        try
        {
            var requestUri = new Uri(new Uri(workerRestUrl), path);
            _logger.LogDebug("Forwarding GET {Path} to worker {WorkerId} at {Uri}", path, workerId, requestUri);

            return await _httpClient.GetAsync(requestUri);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to forward request to worker {WorkerId} at {RestUrl}{Path}",
                workerId, workerRestUrl, path);
            return null;
        }
    }

    /// <summary>
    /// Forwards a POST request to the worker that owns the specified connector.
    /// </summary>
    [SuppressMessage("Design", "CA1054:URI-like parameters should not be strings", Justification = "Path segment, not a full URI")]
    public async Task<HttpResponseMessage?> ForwardPostToWorkerAsync(
        string workerId, string path, HttpContent? content = null)
    {
        var workerRestUrl = GetWorkerRestUrl(workerId);
        if (workerRestUrl == null)
        {
            _logger.LogWarning("Cannot forward request to worker {WorkerId}: REST URL unknown", workerId);
            return null;
        }

        try
        {
            var requestUri = new Uri(new Uri(workerRestUrl), path);
            _logger.LogDebug("Forwarding POST {Path} to worker {WorkerId} at {Uri}", path, workerId, requestUri);

            return await _httpClient.PostAsync(requestUri, content);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to forward POST request to worker {WorkerId} at {RestUrl}{Path}",
                workerId, workerRestUrl, path);
            return null;
        }
    }

    /// <summary>
    /// Forwards a PUT request to the worker that owns the specified connector.
    /// </summary>
    [SuppressMessage("Design", "CA1054:URI-like parameters should not be strings", Justification = "Path segment, not a full URI")]
    public async Task<HttpResponseMessage?> ForwardPutToWorkerAsync(
        string workerId, string path, HttpContent? content = null)
    {
        var workerRestUrl = GetWorkerRestUrl(workerId);
        if (workerRestUrl == null)
        {
            _logger.LogWarning("Cannot forward request to worker {WorkerId}: REST URL unknown", workerId);
            return null;
        }

        try
        {
            var requestUri = new Uri(new Uri(workerRestUrl), path);
            _logger.LogDebug("Forwarding PUT {Path} to worker {WorkerId} at {Uri}", path, workerId, requestUri);

            return await _httpClient.PutAsync(requestUri, content);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to forward PUT request to worker {WorkerId} at {RestUrl}{Path}",
                workerId, workerRestUrl, path);
            return null;
        }
    }

    private string? GetWorkerRestUrl(string workerId)
    {
        var worker = _coordinator.Workers.FirstOrDefault(
            w => w.WorkerId.Equals(workerId, StringComparison.Ordinal));
        return worker?.RestUrl;
    }

    public void Dispose()
    {
        _httpClient.Dispose();
    }
}
