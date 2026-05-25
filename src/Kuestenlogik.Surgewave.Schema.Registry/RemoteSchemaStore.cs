using System.Net.Http.Json;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// <see cref="ISchemaStore"/> implementation that delegates to an external
/// standalone Schema Registry via HTTP. Used by the broker when
/// <c>Surgewave:SchemaRegistry:ExternalUrl</c> is configured — inference,
/// evolution, and migration services talk to the remote registry
/// instead of a local <see cref="SchemaStore"/>.
/// </summary>
public sealed class RemoteSchemaStore : ISchemaStore
{
    private readonly HttpClient _client;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    public RemoteSchemaStore(HttpClient client)
    {
        _client = client;
    }

    public Schema RegisterSchema(string subject, string schemaString, SchemaType schemaType,
        IReadOnlyList<SchemaReference>? references = null)
    {
        var body = new { schema = schemaString, schemaType = schemaType.ToString().ToUpperInvariant() };
        var response = _client.PostAsJsonAsync($"subjects/{subject}/versions", body, JsonOpts).GetAwaiter().GetResult();
        response.EnsureSuccessStatusCode();
        var result = response.Content.ReadFromJsonAsync<RegisterResult>(JsonOpts).GetAwaiter().GetResult();
        return new Schema
        {
            Id = result?.Id ?? 0,
            Version = result?.Version ?? 1,
            Subject = subject,
            SchemaString = schemaString,
            SchemaType = schemaType
        };
    }

    public Schema? GetLatestSchema(string subject)
    {
        try
        {
            var result = _client.GetFromJsonAsync<SchemaVersionResult>($"subjects/{subject}/versions/latest", JsonOpts)
                .GetAwaiter().GetResult();
            if (result is null) return null;
            return new Schema
            {
                Id = result.Id,
                Version = result.Version,
                Subject = result.Subject ?? subject,
                SchemaString = result.Schema ?? "",
                SchemaType = Enum.TryParse<SchemaType>(result.SchemaType, true, out var st) ? st : SchemaType.Json
            };
        }
        catch (HttpRequestException) { return null; }
    }

    public Schema? GetSchema(string subject, int version)
    {
        try
        {
            var result = _client.GetFromJsonAsync<SchemaVersionResult>($"subjects/{subject}/versions/{version}", JsonOpts)
                .GetAwaiter().GetResult();
            if (result is null) return null;
            return new Schema
            {
                Id = result.Id,
                Version = result.Version,
                Subject = result.Subject ?? subject,
                SchemaString = result.Schema ?? "",
                SchemaType = Enum.TryParse<SchemaType>(result.SchemaType, true, out var st) ? st : SchemaType.Json
            };
        }
        catch (HttpRequestException) { return null; }
    }

    public Schema? GetSchemaById(int id)
    {
        try
        {
            var result = _client.GetFromJsonAsync<SchemaByIdResult>($"schemas/ids/{id}", JsonOpts)
                .GetAwaiter().GetResult();
            if (result is null) return null;
            return new Schema
            {
                Id = id,
                Version = 0,
                Subject = "",
                SchemaString = result.Schema ?? "",
                SchemaType = Enum.TryParse<SchemaType>(result.SchemaType, true, out var st) ? st : SchemaType.Json
            };
        }
        catch (HttpRequestException) { return null; }
    }

    public IReadOnlyList<string> GetSubjects(bool includeDeleted = false)
    {
        try
        {
            return _client.GetFromJsonAsync<List<string>>("subjects", JsonOpts).GetAwaiter().GetResult() ?? [];
        }
        catch (HttpRequestException) { return []; }
    }

    public IReadOnlyList<int> GetVersions(string subject, bool includeDeleted = false)
    {
        try
        {
            return _client.GetFromJsonAsync<List<int>>($"subjects/{subject}/versions", JsonOpts).GetAwaiter().GetResult() ?? [];
        }
        catch (HttpRequestException) { return []; }
    }

    public IReadOnlyList<int> DeleteSubject(string subject, bool permanent = false)
    {
        var response = _client.DeleteAsync($"subjects/{subject}").GetAwaiter().GetResult();
        if (!response.IsSuccessStatusCode) return [];
        return response.Content.ReadFromJsonAsync<List<int>>(JsonOpts).GetAwaiter().GetResult() ?? [];
    }

    public int? DeleteVersion(string subject, int version, bool permanent = false)
    {
        var response = _client.DeleteAsync($"subjects/{subject}/versions/{version}").GetAwaiter().GetResult();
        if (!response.IsSuccessStatusCode) return null;
        return response.Content.ReadFromJsonAsync<int>(JsonOpts).GetAwaiter().GetResult();
    }

    public CompatibilityMode GetCompatibility(string subject) => CompatibilityMode.Backward;

    public void SetCompatibility(string subject, CompatibilityMode compatibility) { }

    public IReadOnlyList<Schema> GetSchemasForCompatibilityCheck(string subject, CompatibilityMode mode) => [];

    private sealed record RegisterResult(int Id, int Version);
    private sealed record SchemaVersionResult(int Id, int Version, string? Subject, string? Schema, string? SchemaType);
    private sealed record SchemaByIdResult(string? Schema, string? SchemaType);
}
