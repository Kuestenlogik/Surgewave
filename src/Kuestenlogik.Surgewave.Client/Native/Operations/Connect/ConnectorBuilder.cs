using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Connect;

/// <summary>
/// Fluent builder for connector creation.
/// </summary>
public sealed class ConnectorBuilder
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _name;
    private readonly Dictionary<string, string> _config = new();

    internal ConnectorBuilder(SurgewaveNativeClient client, string name)
    {
        _client = client;
        _name = name;
    }

    /// <summary>
    /// Set the connector class.
    /// </summary>
    public ConnectorBuilder WithClass(string connectorClass)
    {
        _config["connector.class"] = connectorClass;
        return this;
    }

    /// <summary>
    /// Set a configuration option.
    /// </summary>
    public ConnectorBuilder WithConfig(string key, string value)
    {
        _config[key] = value;
        return this;
    }

    /// <summary>
    /// Set the number of tasks.
    /// </summary>
    public ConnectorBuilder WithTasks(int taskCount)
    {
        _config["tasks.max"] = taskCount.ToString();
        return this;
    }

    /// <summary>
    /// Set the topics for a source connector.
    /// </summary>
    public ConnectorBuilder WithTopics(params string[] topics)
    {
        _config["topics"] = string.Join(",", topics);
        return this;
    }

    /// <summary>
    /// Set the topics regex for a sink connector.
    /// </summary>
    public ConnectorBuilder WithTopicsRegex(string regex)
    {
        _config["topics.regex"] = regex;
        return this;
    }

    /// <summary>
    /// Set the key converter.
    /// </summary>
    public ConnectorBuilder WithKeyConverter(string converter)
    {
        _config["key.converter"] = converter;
        return this;
    }

    /// <summary>
    /// Set the value converter.
    /// </summary>
    public ConnectorBuilder WithValueConverter(string converter)
    {
        _config["value.converter"] = converter;
        return this;
    }

    /// <summary>
    /// Use JSON converters.
    /// </summary>
    public ConnectorBuilder WithJsonConverters()
    {
        _config["key.converter"] = "org.apache.kafka.connect.json.JsonConverter";
        _config["value.converter"] = "org.apache.kafka.connect.json.JsonConverter";
        return this;
    }

    /// <summary>
    /// Use Avro converters.
    /// </summary>
    public ConnectorBuilder WithAvroConverters()
    {
        _config["key.converter"] = "io.confluent.connect.avro.AvroConverter";
        _config["value.converter"] = "io.confluent.connect.avro.AvroConverter";
        return this;
    }

    /// <summary>
    /// Execute the connector creation.
    /// </summary>
    public Task<ConnectorCreateResult> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        if (!_config.ContainsKey("connector.class"))
        {
            throw new InvalidOperationException("Connector class is required");
        }

        return _client.Connect.CreateConnectorAsync(_name, _config, cancellationToken);
    }
}
