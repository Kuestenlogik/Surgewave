using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Configuration for admin clients.
/// </summary>
public class AdminClientConfig : ClientConfig
{
    /// <summary>
    /// Creates an empty admin client configuration.
    /// </summary>
    public AdminClientConfig() { }

    /// <summary>
    /// Creates an admin client configuration from existing properties.
    /// </summary>
    public AdminClientConfig(IEnumerable<KeyValuePair<string, string>> config) : base(config) { }
}

/// <summary>
/// Builder for creating Confluent.Kafka-compatible admin clients.
/// </summary>
public sealed class AdminClientBuilder
{
    private readonly AdminClientConfig _config;
    private Action<IAdminClient, Error>? _errorHandler;
    private Action<IAdminClient, LogMessage>? _logHandler;
    private Action<IAdminClient, string>? _statisticsHandler;

    /// <summary>
    /// Creates a new AdminClientBuilder with the specified configuration.
    /// </summary>
    /// <param name="config">The admin client configuration.</param>
    public AdminClientBuilder(IEnumerable<KeyValuePair<string, string>> config)
    {
        _config = config as AdminClientConfig ?? new AdminClientConfig(config);
    }

    /// <summary>
    /// Set the error handler callback.
    /// </summary>
    public AdminClientBuilder SetErrorHandler(Action<IAdminClient, Error> errorHandler)
    {
        _errorHandler = errorHandler;
        return this;
    }

    /// <summary>
    /// Set the log handler callback.
    /// </summary>
    public AdminClientBuilder SetLogHandler(Action<IAdminClient, LogMessage> logHandler)
    {
        _logHandler = logHandler;
        return this;
    }

    /// <summary>
    /// Set the statistics handler callback.
    /// </summary>
    public AdminClientBuilder SetStatisticsHandler(Action<IAdminClient, string> statisticsHandler)
    {
        _statisticsHandler = statisticsHandler;
        return this;
    }

    /// <summary>
    /// Build the admin client.
    /// </summary>
    /// <returns>A new admin client instance.</returns>
    public IAdminClient Build()
    {
        var bootstrapServers = _config.BootstrapServers
            ?? throw new ArgumentException("BootstrapServers must be set");

        // Determine protocol
        var protocol = _config["surgewave.protocol"]?.ToLowerInvariant() switch
        {
            "surgewave" => ProtocolType.SurgewaveNative,
            "kafka" => ProtocolType.Kafka,
            _ => ProtocolType.Auto
        };

        // Build Surgewave client
        var builder = SurgewaveClient.Create(bootstrapServers);

        if (_config.ClientId is not null)
            builder.WithClientId(_config.ClientId);

        builder = protocol switch
        {
            ProtocolType.SurgewaveNative => builder.UseSurgewaveProtocol(),
            ProtocolType.Kafka => builder.UseKafkaProtocol(),
            _ => builder.UseAutoDetect()
        };

        var client = builder.Build();
        return new AdminClient(client);
    }
}
