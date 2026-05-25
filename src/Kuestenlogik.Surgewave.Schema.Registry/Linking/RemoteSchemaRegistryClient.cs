using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry.Linking;

/// <summary>
/// HTTP client for interacting with a remote Confluent-compatible Schema Registry REST API.
/// </summary>
public sealed class RemoteSchemaRegistryClient : IDisposable
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly HttpClient _httpClient;
    private readonly bool _ownsHttpClient;

    /// <summary>
    /// Creates a new client for the specified schema registry base URL.
    /// </summary>
    public RemoteSchemaRegistryClient(string baseUrl)
    {
        _httpClient = new HttpClient { BaseAddress = new Uri(baseUrl.TrimEnd('/') + "/") };
        _httpClient.DefaultRequestHeaders.Accept.Add(
            new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/vnd.schemaregistry.v1+json"));
        _ownsHttpClient = true;
    }

    /// <summary>
    /// Creates a new client using an existing <see cref="HttpClient"/>.
    /// </summary>
    public RemoteSchemaRegistryClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
        _ownsHttpClient = false;
    }

    /// <summary>
    /// Lists all subjects in the remote registry.
    /// </summary>
    public async Task<IReadOnlyList<string>> GetSubjectsAsync(CancellationToken ct = default)
    {
        var response = await _httpClient.GetAsync("subjects", ct);
        response.EnsureSuccessStatusCode();
        var subjects = await response.Content.ReadFromJsonAsync<List<string>>(s_jsonOptions, ct);
        return subjects ?? [];
    }

    /// <summary>
    /// Lists all versions for a subject in the remote registry.
    /// </summary>
    public async Task<IReadOnlyList<int>> GetVersionsAsync(string subject, CancellationToken ct = default)
    {
        var response = await _httpClient.GetAsync($"subjects/{Uri.EscapeDataString(subject)}/versions", ct);
        response.EnsureSuccessStatusCode();
        var versions = await response.Content.ReadFromJsonAsync<List<int>>(s_jsonOptions, ct);
        return versions ?? [];
    }

    /// <summary>
    /// Gets a specific schema version from the remote registry.
    /// </summary>
    public async Task<RemoteSchema> GetSchemaAsync(string subject, int version, CancellationToken ct = default)
    {
        var response = await _httpClient.GetAsync(
            $"subjects/{Uri.EscapeDataString(subject)}/versions/{version}", ct);
        response.EnsureSuccessStatusCode();
        var schema = await response.Content.ReadFromJsonAsync<RemoteSchema>(s_jsonOptions, ct)
            ?? throw new InvalidOperationException($"Failed to deserialize schema for {subject} v{version}");
        return schema;
    }

    /// <summary>
    /// Registers a new schema under a subject in the remote registry.
    /// </summary>
    /// <returns>The global schema ID assigned by the remote registry.</returns>
    public async Task<int> RegisterSchemaAsync(string subject, string schema, string schemaType, CancellationToken ct = default)
    {
        var request = new RemoteRegisterRequest { Schema = schema, SchemaType = schemaType };
        var response = await _httpClient.PostAsJsonAsync(
            $"subjects/{Uri.EscapeDataString(subject)}/versions", request, s_jsonOptions, ct);
        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<RemoteRegisterResponse>(s_jsonOptions, ct)
            ?? throw new InvalidOperationException($"Failed to register schema for {subject}");
        return result.Id;
    }

    /// <summary>
    /// Gets the compatibility level for a subject in the remote registry.
    /// </summary>
    public async Task<string> GetCompatibilityAsync(string subject, CancellationToken ct = default)
    {
        var response = await _httpClient.GetAsync($"config/{Uri.EscapeDataString(subject)}", ct);
        response.EnsureSuccessStatusCode();
        var config = await response.Content.ReadFromJsonAsync<RemoteConfigResponse>(s_jsonOptions, ct);
        return config?.CompatibilityLevel ?? "BACKWARD";
    }

    /// <summary>
    /// Sets the compatibility level for a subject in the remote registry.
    /// </summary>
    public async Task SetCompatibilityAsync(string subject, string compatibility, CancellationToken ct = default)
    {
        var request = new RemoteConfigRequest { Compatibility = compatibility };
        var response = await _httpClient.PutAsJsonAsync(
            $"config/{Uri.EscapeDataString(subject)}", request, s_jsonOptions, ct);
        response.EnsureSuccessStatusCode();
    }

    public void Dispose()
    {
        if (_ownsHttpClient)
        {
            _httpClient.Dispose();
        }
    }
}

/// <summary>
/// Schema data returned from a remote registry.
/// </summary>
public sealed record RemoteSchema
{
    /// <summary>Global schema ID.</summary>
    [JsonPropertyName("id")]
    public int Id { get; init; }

    /// <summary>The schema definition string.</summary>
    [JsonPropertyName("schema")]
    public string Schema { get; init; } = "";

    /// <summary>Schema type (AVRO, JSON, PROTOBUF).</summary>
    [JsonPropertyName("schemaType")]
    public string SchemaType { get; init; } = "AVRO";

    /// <summary>Version number within the subject.</summary>
    [JsonPropertyName("version")]
    public int Version { get; init; }

    /// <summary>The subject name.</summary>
    [JsonPropertyName("subject")]
    public string? Subject { get; init; }
}

/// <summary>
/// Request to register a schema on a remote registry.
/// </summary>
internal sealed class RemoteRegisterRequest
{
    [JsonPropertyName("schema")]
    public required string Schema { get; init; }

    [JsonPropertyName("schemaType")]
    public required string SchemaType { get; init; }
}

/// <summary>
/// Response from registering a schema on a remote registry.
/// </summary>
internal sealed class RemoteRegisterResponse
{
    [JsonPropertyName("id")]
    public int Id { get; init; }
}

/// <summary>
/// Compatibility config response from a remote registry.
/// </summary>
internal sealed class RemoteConfigResponse
{
    [JsonPropertyName("compatibilityLevel")]
    public string? CompatibilityLevel { get; init; }
}

/// <summary>
/// Request to set compatibility on a remote registry.
/// </summary>
internal sealed class RemoteConfigRequest
{
    [JsonPropertyName("compatibility")]
    public required string Compatibility { get; init; }
}
