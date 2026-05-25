using System.Net.Http.Headers;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Plugins;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Service for the connector marketplace UI.
/// Communicates with the broker via Surgewave Native Protocol.
/// </summary>
public sealed class ConnectorMarketplaceService : IConnectorMarketplaceService, IAsyncDisposable
{
    private readonly SurgewaveNativeClient _client;
    private readonly ILogger<ConnectorMarketplaceService> _logger;
    private readonly string _brokerBaseUrl;
    private bool _connected;

    public ConnectorMarketplaceService(
        IConfiguration configuration,
        ILogger<ConnectorMarketplaceService> logger)
    {
        _logger = logger;

        var host = configuration["Surgewave:Host"] ?? "localhost";
        var port = int.Parse(configuration["Surgewave:Port"] ?? "9092", System.Globalization.CultureInfo.InvariantCulture);
        var grpcPort = configuration["Surgewave:GrpcPort"] ?? "5000";

        _client = new SurgewaveNativeClient(host, port);
        _brokerBaseUrl = $"http://{host}:{grpcPort}";
        _logger.LogInformation("Marketplace service configured for broker at {Host}:{Port}", host, port);
    }

    private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (!_connected)
        {
            await _client.ConnectAsync(cancellationToken);
            _connected = true;
            _logger.LogInformation("Connected to Surgewave broker for marketplace operations");
        }
    }

    /// <inheritdoc />
    public async Task<PluginSearchResult> SearchAsync(
        string? query = null,
        int skip = 0,
        int take = 50,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureConnectedAsync(cancellationToken);
            return await _client.Plugins.SearchAsync(query, skip, take, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to search packages with query: {Query}", query);
            return new PluginSearchResult([], 0);
        }
    }

    /// <inheritdoc />
    public async Task<PluginInfo?> GetPackageAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureConnectedAsync(cancellationToken);
            return await _client.Plugins.GetPluginAsync(packageId, version, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get package: {PackageId}", packageId);
            return null;
        }
    }

    /// <inheritdoc />
    public async Task<DependencyTreeNode?> GetDependencyTreeAsync(
        string packageId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureConnectedAsync(cancellationToken);
            return await _client.Plugins.GetDependencyTreeAsync(packageId, cancellationToken: cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get dependency tree for: {PackageId}", packageId);
            return null;
        }
    }

    /// <inheritdoc />
    public async Task<PluginInstallResult> InstallAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureConnectedAsync(cancellationToken);
            _logger.LogInformation("Installing package: {PackageId} v{Version}", packageId, version ?? "latest");
            return await _client.Plugins.InstallAsync(packageId, version, includeDependencies: true, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to install package: {PackageId}", packageId);
            return new PluginInstallResult(false, false, [], [$"Installation failed: {ex.Message}"]);
        }
    }

    /// <inheritdoc />
    public async Task<PluginUninstallResult> UninstallAsync(
        string packageId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureConnectedAsync(cancellationToken);
            _logger.LogInformation("Uninstalling package: {PackageId}", packageId);
            return await _client.Plugins.UninstallAsync(packageId, cancellationToken: cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to uninstall package: {PackageId}", packageId);
            return new PluginUninstallResult(false, [], ex.Message);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<PluginInfo>> ListInstalledAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureConnectedAsync(cancellationToken);
            return await _client.Plugins.ListInstalledAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list installed plugins");
            return [];
        }
    }

    /// <inheritdoc />
    public async Task<PluginUploadResult> UploadPluginAsync(
        Stream packageStream,
        string fileName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Uploading plugin package: {FileName}", fileName);

            using var httpClient = new HttpClient();
            using var content = new MultipartFormDataContent();
            using var streamContent = new StreamContent(packageStream);
            streamContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            content.Add(streamContent, "file", fileName);

            var response = await httpClient.PostAsync(
                $"{_brokerBaseUrl}/api/plugins/upload", content, cancellationToken);

            if (response.IsSuccessStatusCode)
            {
                var json = await response.Content.ReadFromJsonAsync<UploadResponse>(cancellationToken: cancellationToken);
                _logger.LogInformation("Plugin uploaded successfully: {PluginId} v{Version}", json?.PluginId, json?.Version);
                return PluginUploadResult.Success(json?.PluginId ?? "", json?.Version ?? "", json?.WasUpgrade ?? false);
            }

            var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);
            _logger.LogError("Plugin upload failed: {StatusCode} {Error}", response.StatusCode, errorBody);
            return PluginUploadResult.Failed($"Upload failed: {response.StatusCode}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to upload plugin: {FileName}", fileName);
            return PluginUploadResult.Failed($"Upload failed: {ex.Message}");
        }
    }

    private sealed record UploadResponse(string? PluginId, string? Version, bool? WasUpgrade);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
    }
}
