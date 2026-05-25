using System.Text;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Extensions;

/// <summary>
/// Schema registry integration for send operations.
/// </summary>
public sealed class SchemaValidatedSendBuilder<TKey, TValue>
{
    private readonly TypedSendBuilder<TKey, TValue> _inner;
    private readonly SurgewaveNativeClient _client;
    private readonly string _topic;
    private string? _keySubject;
    private string? _valueSubject;
    private bool _autoRegister;
    private bool _validateSchema = true;

    internal SchemaValidatedSendBuilder(TypedSendBuilder<TKey, TValue> inner, SurgewaveNativeClient client, string topic)
    {
        _inner = inner;
        _client = client;
        _topic = topic;
        // Default subjects follow Kafka convention
        _keySubject = $"{topic}-key";
        _valueSubject = $"{topic}-value";
    }

    /// <summary>
    /// Set custom key schema subject.
    /// </summary>
    public SchemaValidatedSendBuilder<TKey, TValue> WithKeySchema(string subject)
    {
        _keySubject = subject;
        return this;
    }

    /// <summary>
    /// Set custom value schema subject.
    /// </summary>
    public SchemaValidatedSendBuilder<TKey, TValue> WithValueSchema(string subject)
    {
        _valueSubject = subject;
        return this;
    }

    /// <summary>
    /// Use a specific schema subject for validation.
    /// </summary>
    public SchemaValidatedSendBuilder<TKey, TValue> WithSchema(string subject)
    {
        _valueSubject = subject;
        return this;
    }

    /// <summary>
    /// Auto-register schema if not found.
    /// </summary>
    public SchemaValidatedSendBuilder<TKey, TValue> WithSchemaAutoRegister()
    {
        _autoRegister = true;
        return this;
    }

    /// <summary>
    /// Skip schema validation (send without validation).
    /// </summary>
    public SchemaValidatedSendBuilder<TKey, TValue> SkipValidation()
    {
        _validateSchema = false;
        return this;
    }

    /// <summary>
    /// Set target partition.
    /// </summary>
    public SchemaValidatedSendBuilder<TKey, TValue> ToPartition(int partition)
    {
        _inner.ToPartition(partition);
        return this;
    }

    /// <summary>
    /// Set the message key.
    /// </summary>
    public SchemaValidatedSendBuilder<TKey, TValue> WithKey(TKey key)
    {
        _inner.WithKey(key);
        return this;
    }

    /// <summary>
    /// Set the message value.
    /// </summary>
    public SchemaValidatedSendBuilder<TKey, TValue> WithValue(TValue value)
    {
        _inner.WithValue(value);
        return this;
    }

    /// <summary>
    /// Execute with schema validation.
    /// </summary>
    public async Task<long> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        if (_validateSchema && _valueSubject != null)
        {
            await EnsureSchemaAsync(_valueSubject, typeof(TValue), cancellationToken);
        }

        return await _inner.ExecuteAsync(cancellationToken);
    }

    private async Task EnsureSchemaAsync(string subject, Type type, CancellationToken ct)
    {
        try
        {
            // Try to get existing schema versions
            var versions = await _client.Schema.GetSubjectVersionsAsync(subject, ct);
            if (versions.Count > 0) return; // Schema exists
        }
        catch
        {
            // Subject doesn't exist
        }

        if (_autoRegister)
        {
            // Generate and register JSON schema
            var schema = GenerateJsonSchema(type);
            await _client.Schema.RegisterSchemaAsync(subject, schema, "JSON", ct);
        }
        else
        {
            throw new InvalidOperationException($"Schema subject '{subject}' not found. Use WithSchemaAutoRegister() to auto-register.");
        }
    }

    private static string GenerateJsonSchema(Type type)
    {
        // Simple JSON schema generation
        var props = type.GetProperties()
            .Select(p => $"\"{p.Name}\": {{\"type\": \"{GetJsonType(p.PropertyType)}\"}}")
            .ToList();

        return $$"""
        {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                {{string.Join(",\n                ", props)}}
            }
        }
        """;
    }

    private static string GetJsonType(Type type)
    {
        if (type == typeof(string)) return "string";
        if (type == typeof(int) || type == typeof(long) || type == typeof(short)) return "integer";
        if (type == typeof(float) || type == typeof(double) || type == typeof(decimal)) return "number";
        if (type == typeof(bool)) return "boolean";
        if (type.IsArray || typeof(System.Collections.IEnumerable).IsAssignableFrom(type)) return "array";
        return "object";
    }
}

/// <summary>
/// Extension methods for schema registry integration.
/// </summary>
public static class SchemaExtensions
{
    /// <summary>
    /// Add schema validation to a typed send builder.
    /// </summary>
    public static SchemaValidatedSendBuilder<TKey, TValue> WithSchema<TKey, TValue>(
        this TypedSendBuilder<TKey, TValue> builder,
        string subject)
    {
        // Get client via reflection (internal field)
        var field = typeof(TypedSendBuilder<TKey, TValue>).GetField("_client", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var client = (SurgewaveNativeClient)field!.GetValue(builder)!;
        var topicField = typeof(TypedSendBuilder<TKey, TValue>).GetField("_topic", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var topic = (string)topicField!.GetValue(builder)!;

        return new SchemaValidatedSendBuilder<TKey, TValue>(builder, client, topic).WithSchema(subject);
    }

    /// <summary>
    /// Add schema validation with auto-registration.
    /// </summary>
    public static SchemaValidatedSendBuilder<TKey, TValue> WithSchemaAutoRegister<TKey, TValue>(
        this TypedSendBuilder<TKey, TValue> builder)
    {
        var field = typeof(TypedSendBuilder<TKey, TValue>).GetField("_client", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var client = (SurgewaveNativeClient)field!.GetValue(builder)!;
        var topicField = typeof(TypedSendBuilder<TKey, TValue>).GetField("_topic", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var topic = (string)topicField!.GetValue(builder)!;

        return new SchemaValidatedSendBuilder<TKey, TValue>(builder, client, topic).WithSchemaAutoRegister();
    }
}
