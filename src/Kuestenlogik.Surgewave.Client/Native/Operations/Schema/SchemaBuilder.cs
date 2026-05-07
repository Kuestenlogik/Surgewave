using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

/// <summary>
/// Fluent builder for schema registration.
/// </summary>
public sealed class SchemaBuilder
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _subject;
    private string _schemaType = "AVRO";
    private string _schemaString = string.Empty;

    internal SchemaBuilder(SurgewaveNativeClient client, string subject)
    {
        _client = client;
        _subject = subject;
    }

    /// <summary>
    /// Set the schema type.
    /// </summary>
    public SchemaBuilder WithType(string schemaType)
    {
        _schemaType = schemaType;
        return this;
    }

    /// <summary>
    /// Set the schema string.
    /// </summary>
    public SchemaBuilder WithSchema(string schemaString)
    {
        _schemaString = schemaString;
        return this;
    }

    /// <summary>
    /// Use AVRO schema type.
    /// </summary>
    public SchemaBuilder AsAvro()
    {
        _schemaType = "AVRO";
        return this;
    }

    /// <summary>
    /// Use JSON schema type.
    /// </summary>
    public SchemaBuilder AsJson()
    {
        _schemaType = "JSON";
        return this;
    }

    /// <summary>
    /// Use Protobuf schema type.
    /// </summary>
    public SchemaBuilder AsProtobuf()
    {
        _schemaType = "PROTOBUF";
        return this;
    }

    /// <summary>
    /// Execute the schema registration.
    /// </summary>
    public Task<SchemaRegistrationResult> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(_schemaString))
        {
            throw new InvalidOperationException("Schema string is required");
        }

        return _client.Schema.RegisterSchemaAsync(_subject, _schemaString, _schemaType, cancellationToken);
    }
}
