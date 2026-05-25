namespace Kuestenlogik.Surgewave.Connect.Nodes.Trigger;

using System.Collections.Concurrent;
using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect.Configuration;

/// <summary>
/// HTTP webhook trigger that exposes an endpoint and emits received payloads.
/// </summary>
[ConnectorMetadata(
    Name = "WebhookTrigger",
    Description = "HTTP endpoint as event source",
    Tags = "trigger,webhook,http,api")]
public sealed class WebhookTrigger : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(WebhookTriggerTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("topic", ConfigType.String, "", Importance.High,
            "Output topic for webhook events")
        .Define("port", ConfigType.Int, "8888", Importance.Medium,
            "HTTP port to listen on")
        .Define("path", ConfigType.String, "/webhook", Importance.Medium,
            "URL path prefix")
        .Define("require.auth.header", ConfigType.String, "", Importance.Low,
            "Required Authorization header value");

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}

internal sealed class WebhookTriggerTask : SourceTask, IDisposable
{
    public override string Version => "1.0.0";

    private HttpListener? _listener;
    private CancellationTokenSource? _cts;
    private string _topic = "";
    private string _path = "/webhook";
    private string? _requiredAuth;
    private readonly ConcurrentQueue<SourceRecord> _pendingRecords = new();
    private Task? _listenerTask;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config.TryGetValue("topic", out var t) ? t : "";
        var port = config.TryGetValue("port", out var p) && int.TryParse(p, out var pt) ? pt : 8888;
        _path = config.TryGetValue("path", out var ph) ? ph : "/webhook";
        _requiredAuth = config.TryGetValue("require.auth.header", out var a) ? a : null;

        if (string.IsNullOrEmpty(_topic))
            return;

        _cts = new CancellationTokenSource();
        _listener = new HttpListener();
        _listener.Prefixes.Add($"http://+:{port}{_path}/");

        try
        {
            _listener.Start();
            _listenerTask = Task.Run(() => ListenLoop(_cts.Token));
        }
        catch (HttpListenerException)
        {
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://localhost:{port}{_path}/");
            _listener.Start();
            _listenerTask = Task.Run(() => ListenLoop(_cts.Token));
        }
    }

    private async Task ListenLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested && _listener?.IsListening == true)
        {
            try
            {
                var context = await _listener.GetContextAsync();
                await HandleRequest(context);
            }
            catch (HttpListenerException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch { }
        }
    }

    private async Task HandleRequest(HttpListenerContext context)
    {
        var request = context.Request;
        var response = context.Response;

        try
        {
            if (!string.IsNullOrEmpty(_requiredAuth))
            {
                var auth = request.Headers["Authorization"];
                if (auth != _requiredAuth)
                {
                    response.StatusCode = 401;
                    response.Close();
                    return;
                }
            }

            if (request.HttpMethod != "POST" && request.HttpMethod != "PUT")
            {
                response.StatusCode = 405;
                response.Close();
                return;
            }

            using var reader = new StreamReader(request.InputStream, Encoding.UTF8);
            var body = await reader.ReadToEndAsync();

            var headers = new Dictionary<string, byte[]>
            {
                ["_webhook_method"] = Encoding.UTF8.GetBytes(request.HttpMethod),
                ["_webhook_path"] = Encoding.UTF8.GetBytes(request.Url?.PathAndQuery ?? ""),
                ["_webhook_content_type"] = Encoding.UTF8.GetBytes(request.ContentType ?? "")
            };

            var record = new SourceRecord
            {
                SourcePartition = new Dictionary<string, object> { ["webhook"] = _path },
                SourceOffset = new Dictionary<string, object> { ["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() },
                Topic = _topic,
                Key = request.Url?.PathAndQuery is { } path ? Encoding.UTF8.GetBytes(path) : null,
                Value = Encoding.UTF8.GetBytes(body),
                Headers = headers
            };

            _pendingRecords.Enqueue(record);

            response.StatusCode = 202;
            response.ContentType = "application/json";
            var responseBody = Encoding.UTF8.GetBytes("{\"status\":\"accepted\"}");
            await response.OutputStream.WriteAsync(responseBody);
        }
        finally
        {
            response.Close();
        }
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        while (_pendingRecords.TryDequeue(out var record))
        {
            records.Add(record);
            if (records.Count >= 100)
                break;
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    public override void Stop()
    {
        _cts?.Cancel();
        _listener?.Stop();
        _listener?.Close();
    }

    public new void Dispose()
    {
        _cts?.Dispose();
        (_listener as IDisposable)?.Dispose();
        base.Dispose();
    }
}
