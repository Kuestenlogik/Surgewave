using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Extension methods for adding Surgewave clients to DI.
/// </summary>
public static class SurgewaveClientExtensions
{
    /// <summary>
    /// Adds a typed Surgewave producer to the service collection.
    /// </summary>
    /// <example>
    /// <code>
    /// builder.Services.AddSurgewaveProducer&lt;string, Order&gt;(options =>
    /// {
    ///     options.BootstrapServers = "localhost:9092";
    ///     options.ValueSerializer = Serializers.Json&lt;Order&gt;();
    /// });
    ///
    /// // Then inject:
    /// public class OrderService(SurgewaveProducer&lt;string, Order&gt; producer) { }
    /// </code>
    /// </example>
    public static IServiceCollection AddSurgewaveProducer<TKey, TValue>(
        this IServiceCollection services,
        Action<SurgewaveProducerOptions<TKey, TValue>> configure)
    {
        services.AddSingleton(sp =>
        {
            var options = new SurgewaveProducerOptions<TKey, TValue>();
            configure(options);
            return new SurgewaveProducer<TKey, TValue>(options);
        });

        return services;
    }

    /// <summary>
    /// Adds a typed Surgewave producer that connects to the embedded Surgewave broker.
    /// </summary>
    /// <example>
    /// <code>
    /// builder.AddSurgewave();
    /// builder.Services.AddSurgewaveProducerForEmbeddedBroker&lt;string, Order&gt;();
    /// </code>
    /// </example>
    public static IServiceCollection AddSurgewaveProducerForEmbeddedBroker<TKey, TValue>(
        this IServiceCollection services,
        Action<SurgewaveProducerOptions<TKey, TValue>>? configure = null)
    {
        services.AddSingleton(sp =>
        {
            var surgewave = sp.GetRequiredService<Kuestenlogik.Surgewave.Runtime.SurgewaveRuntime>();
            var options = new SurgewaveProducerOptions<TKey, TValue>
            {
                BootstrapServers = surgewave.BootstrapServers
            };
            configure?.Invoke(options);
            return new SurgewaveProducer<TKey, TValue>(options);
        });

        return services;
    }

    /// <summary>
    /// Adds a typed Surgewave consumer to the service collection.
    /// </summary>
    /// <example>
    /// <code>
    /// builder.Services.AddSurgewaveConsumer&lt;string, Order&gt;(options =>
    /// {
    ///     options.BootstrapServers = "localhost:9092";
    ///     options.GroupId = "order-processor";
    ///     options.ValueDeserializer = Serializers.JsonDeserializer&lt;Order&gt;();
    /// });
    /// </code>
    /// </example>
    public static IServiceCollection AddSurgewaveConsumer<TKey, TValue>(
        this IServiceCollection services,
        Action<SurgewaveConsumerOptions<TKey, TValue>> configure)
    {
        services.AddSingleton(sp =>
        {
            var options = new SurgewaveConsumerOptions<TKey, TValue>();
            configure(options);
            return new SurgewaveConsumer<TKey, TValue>(options);
        });

        return services;
    }

    /// <summary>
    /// Adds a typed Surgewave consumer that connects to the embedded Surgewave broker.
    /// </summary>
    public static IServiceCollection AddSurgewaveConsumerForEmbeddedBroker<TKey, TValue>(
        this IServiceCollection services,
        Action<SurgewaveConsumerOptions<TKey, TValue>>? configure = null)
    {
        services.AddSingleton(sp =>
        {
            var surgewave = sp.GetRequiredService<Kuestenlogik.Surgewave.Runtime.SurgewaveRuntime>();
            var options = new SurgewaveConsumerOptions<TKey, TValue>
            {
                BootstrapServers = surgewave.BootstrapServers
            };
            configure?.Invoke(options);
            return new SurgewaveConsumer<TKey, TValue>(options);
        });

        return services;
    }

    /// <summary>
    /// Adds a keyed Surgewave producer factory for creating producers dynamically.
    /// </summary>
    /// <example>
    /// <code>
    /// builder.Services.AddSurgewaveProducerFactory&lt;string, Order&gt;(options =>
    /// {
    ///     options.BootstrapServers = "localhost:9092";
    /// });
    ///
    /// // Then inject:
    /// public class OrderService(ISurgewaveProducerFactory&lt;string, Order&gt; factory)
    /// {
    ///     var producer = factory.CreateProducer();
    /// }
    /// </code>
    /// </example>
    public static IServiceCollection AddSurgewaveProducerFactory<TKey, TValue>(
        this IServiceCollection services,
        Action<SurgewaveProducerOptions<TKey, TValue>> configure)
    {
        services.AddSingleton<ISurgewaveProducerFactory<TKey, TValue>>(sp =>
            new SurgewaveProducerFactory<TKey, TValue>(configure));

        return services;
    }
}

/// <summary>
/// Factory for creating Surgewave producers.
/// </summary>
public interface ISurgewaveProducerFactory<TKey, TValue>
{
    /// <summary>
    /// Creates a new producer instance.
    /// </summary>
    SurgewaveProducer<TKey, TValue> CreateProducer();
}

internal sealed class SurgewaveProducerFactory<TKey, TValue>(
    Action<SurgewaveProducerOptions<TKey, TValue>> configure) : ISurgewaveProducerFactory<TKey, TValue>
{
    public SurgewaveProducer<TKey, TValue> CreateProducer()
    {
        var options = new SurgewaveProducerOptions<TKey, TValue>();
        configure(options);
        return new SurgewaveProducer<TKey, TValue>(options);
    }
}
