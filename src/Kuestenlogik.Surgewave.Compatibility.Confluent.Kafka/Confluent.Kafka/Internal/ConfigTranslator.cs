using Kuestenlogik.Surgewave.Client.Abstractions;
using SurgewaveAutoOffsetReset = Kuestenlogik.Surgewave.Client.Consumer.AutoOffsetReset;
using SurgewaveIsolationLevel = Kuestenlogik.Surgewave.Client.Consumer.IsolationLevel;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Translates Confluent.Kafka configuration to Surgewave.Client configuration.
/// </summary>
internal static class ConfigTranslator
{
    /// <summary>
    /// Apply producer config to Surgewave producer options.
    /// </summary>
    public static void ApplyProducerConfig<TKey, TValue>(
        ProducerConfig config,
        ProducerOptions<TKey, TValue> options)
    {
        if (config.LingerMs.HasValue)
            options.LingerMs = (int)config.LingerMs.Value;

        if (config.BatchNumMessages.HasValue)
            options.BatchSize = config.BatchNumMessages.Value;

        if (config.RequestTimeoutMs.HasValue)
            options.RequestTimeoutMs = config.RequestTimeoutMs.Value;

        if (config.Acks.HasValue)
        {
            options.RequiredAcks = config.Acks.Value switch
            {
                Acks.None => 0,
                Acks.Leader => 1,
                Acks.All => -1,
                _ => 1
            };
        }
    }

    /// <summary>
    /// Apply consumer config to Surgewave consumer options.
    /// </summary>
    public static void ApplyConsumerConfig<TKey, TValue>(
        ConsumerConfig config,
        ConsumerOptions<TKey, TValue> options)
    {
        if (config.GroupId is not null)
            options.GroupId = config.GroupId;

        if (config.AutoOffsetReset.HasValue)
        {
            options.AutoOffsetReset = config.AutoOffsetReset.Value switch
            {
                AutoOffsetReset.Earliest => SurgewaveAutoOffsetReset.Earliest,
                AutoOffsetReset.Latest => SurgewaveAutoOffsetReset.Latest,
                _ => SurgewaveAutoOffsetReset.Latest
            };
        }

        if (config.EnableAutoCommit.HasValue)
            options.EnableAutoCommit = config.EnableAutoCommit.Value;

        if (config.AutoCommitIntervalMs.HasValue)
            options.AutoCommitIntervalMs = config.AutoCommitIntervalMs.Value;

        if (config.MaxPollIntervalMs.HasValue)
            options.MaxPollIntervalMs = config.MaxPollIntervalMs.Value;

        if (config.SessionTimeoutMs.HasValue)
            options.SessionTimeoutMs = config.SessionTimeoutMs.Value;

        if (config.IsolationLevel.HasValue)
        {
            options.IsolationLevel = config.IsolationLevel.Value switch
            {
                IsolationLevel.ReadCommitted => SurgewaveIsolationLevel.ReadCommitted,
                IsolationLevel.ReadUncommitted => SurgewaveIsolationLevel.ReadUncommitted,
                _ => SurgewaveIsolationLevel.ReadUncommitted
            };
        }
    }

    /// <summary>
    /// Get bootstrap servers from config.
    /// </summary>
    public static string GetBootstrapServers(ClientConfig config) =>
        config.BootstrapServers ?? throw new ArgumentException("BootstrapServers must be set");

    /// <summary>
    /// Get Surgewave protocol setting from config.
    /// </summary>
    public static string? GetSurgewaveProtocol(ClientConfig config) =>
        config["surgewave.protocol"];
}
