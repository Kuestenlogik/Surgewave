using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Pipelines.Publishing;

/// <summary>
/// Publishes DSL-built pipelines to a running broker over the admin REST API
/// (default <c>https://localhost:9093</c>, the broker's gRPC/admin port).
/// Building pipelines needs no broker — only publishing does.
/// </summary>
public sealed class PipelinePublisher : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly HttpClient _http;
    private readonly bool _ownsClient;

    /// <summary>
    /// Connects to the broker's admin endpoint. For loopback addresses the broker's
    /// development TLS certificate is accepted; remote brokers need a trusted certificate.
    /// </summary>
    public PipelinePublisher(Uri brokerUrl)
    {
        ArgumentNullException.ThrowIfNull(brokerUrl);

#pragma warning disable CA2000 // ownership moves to the HttpClient via disposeHandler: true
        var handler = new HttpClientHandler
        {
            CheckCertificateRevocationList = true,
        };
#pragma warning restore CA2000
        if (IsLoopback(brokerUrl.Host))
        {
            handler.ServerCertificateCustomValidationCallback =
                HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
        }

        _http = new HttpClient(handler, disposeHandler: true)
        {
            BaseAddress = brokerUrl,
            Timeout = TimeSpan.FromSeconds(30),
        };
        _ownsClient = true;
    }

    /// <summary>
    /// Uses a caller-provided <see cref="HttpClient"/> (its <see cref="HttpClient.BaseAddress"/>
    /// must point at the broker). The client is not disposed with the publisher.
    /// </summary>
    public PipelinePublisher(HttpClient httpClient)
    {
        ArgumentNullException.ThrowIfNull(httpClient);
        _http = httpClient;
        _ownsClient = false;
    }

    /// <summary>Publishes a DSL-built pipeline.</summary>
    public Task<PublishResult> PublishAsync(
        BuiltPipeline pipeline,
        PipelinePublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        options ??= new PipelinePublishOptions();

        var name = options.NameOverride ?? pipeline.RequireName();
        return PublishCoreAsync(pipeline, name, options, cancellationToken);
    }

    /// <summary>Publishes a pipeline from its export-file form (as saved by the DSL or the UI).</summary>
    public Task<PublishResult> PublishAsync(
        PipelineExportFormat export,
        PipelinePublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(export);
        options ??= new PipelinePublishOptions();

        var data = export.Pipeline;
        var name = options.NameOverride ?? data.Name;
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new PipelinePublishException("The pipeline export has no name and no name override was given.");
        }

        var pipeline = new BuiltPipeline
        {
            Name = name,
            Description = data.Description,
            Nodes = data.Nodes.Select(n => new PipelineNode
            {
                Id = n.NodeId,
                ConnectorType = n.ConnectorType,
                Config = new Dictionary<string, string>(n.Config),
                X = n.X,
                Y = n.Y,
                Label = n.Label,
                RetryPolicy = n.RetryPolicy,
            }).ToList(),
            Connections = data.Connections.Select((c, index) => new PipelineConnection
            {
                Id = $"c{index + 1}",
                SourceNodeId = c.SourceNodeId,
                TargetNodeId = c.TargetNodeId,
                Type = c.Type,
            }).ToList(),
            Parameters = data.Parameters,
            Schedule = data.Schedule,
        };

        return PublishCoreAsync(pipeline, name, options, cancellationToken);
    }

    /// <summary>Lists the pipelines on the broker.</summary>
    public async Task<IReadOnlyList<PipelineDefinition>> ListAsync(CancellationToken cancellationToken = default)
    {
        using var response = await SendAsync(() => _http.GetAsync(new Uri("/api/pipelines", UriKind.Relative), cancellationToken));
        await EnsureSuccessAsync(response, "list pipelines", cancellationToken);
        return await ReadAsync<List<PipelineDefinition>>(response, cancellationToken);
    }

    /// <summary>
    /// Finds a pipeline by id or, when no id matches, by exact name.
    /// Returns null when neither matches; throws when the name is ambiguous.
    /// </summary>
    public async Task<PipelineDefinition?> FindAsync(string idOrName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(idOrName);

        using var response = await SendAsync(() => _http.GetAsync(new Uri($"/api/pipelines/{Uri.EscapeDataString(idOrName)}", UriKind.Relative), cancellationToken));
        if (response.StatusCode != HttpStatusCode.NotFound)
        {
            await EnsureSuccessAsync(response, $"get pipeline '{idOrName}'", cancellationToken);
            return await ReadAsync<PipelineDefinition>(response, cancellationToken);
        }

        var all = await ListAsync(cancellationToken);
        var matches = all.Where(p => string.Equals(p.Name, idOrName, StringComparison.Ordinal)).ToList();
        return matches.Count switch
        {
            0 => null,
            1 => matches[0],
            _ => throw new PipelinePublishException(
                $"The name '{idOrName}' matches {matches.Count} pipelines ({string.Join(", ", matches.Select(m => m.Id))}). Use the id."),
        };
    }

    /// <summary>Fetches a pipeline's portable export.</summary>
    public async Task<PipelineExportFormat> ExportAsync(string pipelineId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineId);
        using var response = await SendAsync(() => _http.GetAsync(new Uri($"/api/pipelines/{Uri.EscapeDataString(pipelineId)}/export", UriKind.Relative), cancellationToken));
        await EnsureSuccessAsync(response, $"export pipeline '{pipelineId}'", cancellationToken);
        return await ReadAsync<PipelineExportFormat>(response, cancellationToken);
    }

    /// <summary>Starts a pipeline.</summary>
    public async Task StartAsync(string pipelineId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineId);
        using var response = await SendAsync(() => _http.PostAsync(new Uri($"/api/pipelines/{Uri.EscapeDataString(pipelineId)}/start", UriKind.Relative), content: null, cancellationToken));
        await EnsureSuccessAsync(response, $"start pipeline '{pipelineId}'", cancellationToken);
    }

    /// <summary>Stops a pipeline.</summary>
    public async Task StopAsync(string pipelineId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineId);
        using var response = await SendAsync(() => _http.PostAsync(new Uri($"/api/pipelines/{Uri.EscapeDataString(pipelineId)}/stop", UriKind.Relative), content: null, cancellationToken));
        await EnsureSuccessAsync(response, $"stop pipeline '{pipelineId}'", cancellationToken);
    }

    public void Dispose()
    {
        if (_ownsClient)
        {
            _http.Dispose();
        }
    }

    private async Task<PublishResult> PublishCoreAsync(
        BuiltPipeline pipeline,
        string name,
        PipelinePublishOptions options,
        CancellationToken cancellationToken)
    {
        PipelineDefinition? existing = null;
        if (options.Mode == PublishMode.ReplaceByName)
        {
            var all = await ListAsync(cancellationToken);
            var matches = all.Where(p => string.Equals(p.Name, name, StringComparison.Ordinal)).ToList();
            if (matches.Count > 1)
            {
                throw new PipelinePublishException(
                    $"Cannot replace by name: '{name}' matches {matches.Count} pipelines. Delete the duplicates first.");
            }

            existing = matches.SingleOrDefault();
        }

        string pipelineId;
        var wasRunning = false;

        if (existing is null)
        {
            var body = new CreatePipelineRequestBody
            {
                Name = name,
                Description = pipeline.Description,
                Nodes = [.. pipeline.Nodes],
                Connections = [.. pipeline.Connections],
                Parameters = pipeline.Parameters is null ? null : new Dictionary<string, string>(pipeline.Parameters),
            };

            using var response = await SendAsync(() => _http.PostAsJsonAsync(new Uri("/api/pipelines", UriKind.Relative), body, JsonOptions, cancellationToken));
            await EnsureSuccessAsync(response, $"create pipeline '{name}'", cancellationToken);
            var created = await ReadAsync<PipelineDefinition>(response, cancellationToken);
            pipelineId = created.Id;

            if (pipeline.Schedule is not null)
            {
                await PutScheduleAsync(pipelineId, name, pipeline.Schedule, cancellationToken);
            }
        }
        else
        {
            pipelineId = existing.Id;
            wasRunning = existing.Status == PipelineStatus.Running;
            if (wasRunning)
            {
                await StopAsync(pipelineId, cancellationToken);
            }

            try
            {
                var body = new UpdatePipelineRequestBody
                {
                    Name = name,
                    Description = pipeline.Description,
                    Nodes = [.. pipeline.Nodes],
                    Connections = [.. pipeline.Connections],
                    // Empty instead of null: the broker keeps the existing parameters when the
                    // request sends null, and a redeploy must be the source of truth.
                    Parameters = pipeline.Parameters is null ? [] : new Dictionary<string, string>(pipeline.Parameters),
                };

                using var response = await SendAsync(() => _http.PutAsJsonAsync(new Uri($"/api/pipelines/{Uri.EscapeDataString(pipelineId)}", UriKind.Relative), body, JsonOptions, cancellationToken));
                await EnsureSuccessAsync(response, $"update pipeline '{name}'", cancellationToken);

                // Replace the schedule unconditionally — dropping WithSchedule from the code
                // must disable the one on the broker.
                await PutScheduleAsync(pipelineId, name, pipeline.Schedule ?? new ScheduleConfig(), cancellationToken);
            }
            catch when (wasRunning && !cancellationToken.IsCancellationRequested)
            {
                // The pipeline was stopped for the update; don't leave it down on a failed
                // deploy — restore the previous state as well as we can.
                try
                {
                    await StartAsync(pipelineId, cancellationToken);
                }
                catch (PipelinePublishException)
                {
                    // the original failure is the one worth surfacing
                }

                throw;
            }
        }

        var start = options.Start || wasRunning;
        if (start)
        {
            await StartAsync(pipelineId, cancellationToken);
        }

        return new PublishResult
        {
            PipelineId = pipelineId,
            Name = name,
            Replaced = existing is not null,
            WasRunning = wasRunning,
            Started = start,
        };
    }

    private async Task PutScheduleAsync(string pipelineId, string name, ScheduleConfig schedule, CancellationToken cancellationToken)
    {
        using var response = await SendAsync(() => _http.PutAsJsonAsync(new Uri($"/api/pipelines/{Uri.EscapeDataString(pipelineId)}/schedule", UriKind.Relative), schedule, JsonOptions, cancellationToken));
        await EnsureSuccessAsync(response, $"set schedule of pipeline '{name}'", cancellationToken);
    }

    private static async Task<HttpResponseMessage> SendAsync(Func<Task<HttpResponseMessage>> send)
    {
        try
        {
            return await send();
        }
        catch (HttpRequestException ex)
        {
            throw new PipelinePublishException(
                $"The broker is unreachable: {ex.Message} " +
                "Is it running with Surgewave:Connect:Enabled=true, and is the URL the admin endpoint (default https://localhost:9093)?",
                ex);
        }
        catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException || !ex.CancellationToken.IsCancellationRequested)
        {
            throw new PipelinePublishException(
                "The broker did not respond within the request timeout. " +
                "Is the URL the admin endpoint (default https://localhost:9093)?",
                ex);
        }
    }

    private static async Task<T> ReadAsync<T>(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var payload = await response.Content.ReadFromJsonAsync<T>(JsonOptions, cancellationToken);
        return payload ?? throw new PipelinePublishException("The broker returned an empty response.");
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, string operation, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        throw new PipelinePublishException(
            $"Failed to {operation}: {(int)response.StatusCode} {response.StatusCode} — {ExtractErrorMessage(body)}",
            response.StatusCode);
    }

    private static string ExtractErrorMessage(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
        {
            return "no details provided";
        }

        try
        {
            using var document = JsonDocument.Parse(body);
            if (document.RootElement.ValueKind == JsonValueKind.Object)
            {
                foreach (var key in (string[])["detail", "title", "message", "error"])
                {
                    if (document.RootElement.TryGetProperty(key, out var value) && value.ValueKind == JsonValueKind.String)
                    {
                        return value.GetString()!;
                    }
                }

                if (document.RootElement.TryGetProperty("errors", out var errors))
                {
                    return errors.ToString();
                }
            }
        }
        catch (JsonException)
        {
            // fall through to the raw body
        }

        return body.Length > 500 ? body[..500] : body;
    }

    private static bool IsLoopback(string host)
    {
        return host is "localhost" or "127.0.0.1" or "::1" or "[::1]";
    }
}
