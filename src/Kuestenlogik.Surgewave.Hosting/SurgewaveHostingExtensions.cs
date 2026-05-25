using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Runtime;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Extension methods for adding Surgewave broker to an ASP.NET Core application.
/// </summary>
public static class SurgewaveHostingExtensions
{
    /// <summary>
    /// Adds a Surgewave broker to the host with default configuration.
    /// Configuration is read from "Surgewave" section in appsettings.json.
    /// </summary>
    /// <example>
    /// <code>
    /// var builder = WebApplication.CreateBuilder(args);
    /// builder.AddSurgewave();
    /// </code>
    /// </example>
    public static IHostApplicationBuilder AddSurgewave(this IHostApplicationBuilder builder)
    {
        return builder.AddSurgewave(SurgewaveOptions.SectionName);
    }

    /// <summary>
    /// Adds a Surgewave broker to the host with configuration from a named section.
    /// </summary>
    /// <param name="builder">The host application builder.</param>
    /// <param name="configurationSectionName">The configuration section name.</param>
    /// <example>
    /// <code>
    /// var builder = WebApplication.CreateBuilder(args);
    /// builder.AddSurgewave("MySurgewaveConfig");
    /// </code>
    /// </example>
    public static IHostApplicationBuilder AddSurgewave(
        this IHostApplicationBuilder builder,
        string configurationSectionName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(configurationSectionName);

        var section = builder.Configuration.GetSection(configurationSectionName);
        builder.Services.Configure<SurgewaveOptions>(section);
        builder.Services.AddSurgewaveCore();

        return builder;
    }

    /// <summary>
    /// Adds a Surgewave broker to the host with configuration action.
    /// </summary>
    /// <param name="builder">The host application builder.</param>
    /// <param name="configure">Action to configure Surgewave options.</param>
    /// <example>
    /// <code>
    /// var builder = WebApplication.CreateBuilder(args);
    /// builder.AddSurgewave(options =>
    /// {
    ///     options.Port = 9092;
    ///     options.Storage = "ZeroCopyWal";
    ///     options.DataDirectory = "/var/surgewave/data";
    /// });
    /// </code>
    /// </example>
    public static IHostApplicationBuilder AddSurgewave(
        this IHostApplicationBuilder builder,
        Action<SurgewaveOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.Configure(configure);
        builder.Services.AddSurgewaveCore();

        return builder;
    }

    /// <summary>
    /// Adds a Surgewave broker to the host with configuration from section and additional configuration action.
    /// </summary>
    /// <param name="builder">The host application builder.</param>
    /// <param name="configurationSectionName">The configuration section name.</param>
    /// <param name="configure">Additional configuration action (applied after section binding).</param>
    public static IHostApplicationBuilder AddSurgewave(
        this IHostApplicationBuilder builder,
        string configurationSectionName,
        Action<SurgewaveOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(configurationSectionName);
        ArgumentNullException.ThrowIfNull(configure);

        var section = builder.Configuration.GetSection(configurationSectionName);
        builder.Services.Configure<SurgewaveOptions>(section);
        builder.Services.PostConfigure(configure);
        builder.Services.AddSurgewaveCore();

        return builder;
    }

    /// <summary>
    /// Adds Surgewave core services to the service collection.
    /// </summary>
    private static IServiceCollection AddSurgewaveCore(this IServiceCollection services)
    {
        // Only add once
        if (services.Any(s => s.ServiceType == typeof(SurgewaveBrokerHostedService)))
        {
            return services;
        }

        // Register the hosted service that manages broker lifecycle
        services.AddSingleton<SurgewaveBrokerHostedService>();
        services.AddHostedService(sp => sp.GetRequiredService<SurgewaveBrokerHostedService>());

        // Register SurgewaveRuntime as a singleton (created by hosted service)
        services.AddSingleton(sp => sp.GetRequiredService<SurgewaveBrokerHostedService>().Surgewave
            ?? throw new InvalidOperationException("Surgewave broker has not been started yet."));

        // Register LogManager for direct access
        services.AddSingleton(sp => sp.GetRequiredService<SurgewaveRuntime>().LogManager);

        return services;
    }
}

/// <summary>
/// Hosted service that manages the Surgewave broker lifecycle.
/// </summary>
internal sealed class SurgewaveBrokerHostedService : IHostedService, IAsyncDisposable
{
    private readonly IOptions<SurgewaveOptions> _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<SurgewaveBrokerHostedService> _logger;
    private SurgewaveRuntime? _surgewave;

    public SurgewaveBrokerHostedService(
        IOptions<SurgewaveOptions> options,
        ILoggerFactory loggerFactory,
        ILogger<SurgewaveBrokerHostedService> logger)
    {
        _options = options;
        _loggerFactory = loggerFactory;
        _logger = logger;
    }

    /// <summary>
    /// The running Surgewave broker instance.
    /// </summary>
    public SurgewaveRuntime? Surgewave => _surgewave;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var options = _options.Value;

        _logger.LogInformation(
            "Starting Surgewave broker on {Host}:{Port} with {Storage} storage",
            options.Host, options.Port, options.Storage);

        var runtimeOptions = MapToRuntimeOptions(options);

        _surgewave = await runtimeOptions.StartAsync(cancellationToken);

        _logger.LogInformation(
            "Surgewave broker started successfully at {BootstrapServers}",
            _surgewave.BootstrapServers);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_surgewave != null)
        {
            _logger.LogInformation("Stopping Surgewave broker...");
            await _surgewave.DisposeAsync();
            _surgewave = null;
            _logger.LogInformation("Surgewave broker stopped.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_surgewave != null)
        {
            await _surgewave.DisposeAsync();
            _surgewave = null;
        }
    }

    private SurgewaveRuntimeOptions MapToRuntimeOptions(SurgewaveOptions options)
    {
        return SurgewaveRuntime.CreateBuilder()
            .WithHost(options.Host)
            .WithPort(options.Port)
            .WithBrokerId(options.BrokerId)
            .WithDataDirectory(options.DataDirectory ?? "")
            .WithStorageEngine(options.GetStorageEngine())
            .WithAutoCreateTopics(options.AutoCreateTopics)
            .WithPartitions(options.Topics.DefaultPartitions)
            .WithReplicationFactor(options.Topics.DefaultReplicationFactor)
            .WithRetentionHours(options.Retention.Hours)
            .WithRetentionBytes(options.Retention.Bytes)
            .WithCleanup(false) // Production broker should not delete data
            .WithLogging(_loggerFactory)
            .Build();
    }
}
