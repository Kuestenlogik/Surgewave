using Kuestenlogik.Surgewave.Broker.Native.Handlers;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;
using Kuestenlogik.Surgewave.Storage.Disaggregated;

namespace Kuestenlogik.Surgewave.Broker.Native.Operations.Topics;

/// <summary>
/// Operation to create a topic.
/// </summary>
public sealed class CreateTopicOperation : IVoidOperationHandler<CreateTopicRequestPayload>
{
    private readonly LogManager _logManager;

    public CreateTopicOperation(LogManager logManager) => _logManager = logManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CreateTopic;

    public CreateTopicRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => CreateTopicRequestPayload.Read(ref reader);

    public void ValidateRequest(in CreateTopicRequestPayload request)
    {
        if (string.IsNullOrEmpty(request.Name))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "Topic name required");

        // ADR-014: validate storage.mode + the replication-factor invariant.
        // We resolve the mode here (string-level) before LogManager touches it
        // so the error reaches the client with a useful diagnostic instead of
        // a generic InvalidOperationException.
        if (request.Configs is { Length: > 0 })
        {
            foreach (var c in request.Configs)
            {
                if (c.Key != StorageModeKeys.ConfigKey) continue;
                StorageMode mode;
                try
                {
                    mode = StorageModeKeys.Parse(c.Value);
                }
                catch (ArgumentException ex)
                {
                    throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, ex.Message);
                }
                try
                {
                    // objectStoreConfigured + isEmbeddedRuntime stay conservatively
                    // true/false here in P1 (no cluster-level introspection yet).
                    // P2/P3 wire these to the actual broker config so the rejection
                    // can also catch "disaggregated requested but no bucket configured".
                    StorageModeValidator.Validate(
                        mode,
                        request.ReplicationFactor,
                        objectStoreConfigured: true,
                        isEmbeddedRuntime: false);
                }
                catch (StorageModeValidationException ex)
                {
                    throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, ex.Message);
                }
            }
        }
    }

    public Task ExecuteAsync(CreateTopicRequestPayload request, CancellationToken cancellationToken)
    {
        Dictionary<string, string>? config = null;
        if (request.Configs is { Length: > 0 })
        {
            config = new Dictionary<string, string>(request.Configs.Length);
            foreach (var c in request.Configs)
            {
                if (!string.IsNullOrEmpty(c.Key) && c.Value != null)
                    config[c.Key] = c.Value;
            }
        }

        return _logManager.CreateTopicAsync(request.Name, request.Partitions, request.ReplicationFactor, config, cancellationToken);
    }
}

/// <summary>
/// Operation to delete a topic.
/// </summary>
public sealed class DeleteTopicOperation : IVoidOperationHandler<DeleteTopicRequestPayload>
{
    private readonly LogManager _logManager;

    public DeleteTopicOperation(LogManager logManager) => _logManager = logManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DeleteTopic;

    public DeleteTopicRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => DeleteTopicRequestPayload.Read(ref reader);

    public void ValidateRequest(in DeleteTopicRequestPayload request)
    {
        if (string.IsNullOrEmpty(request.Name))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "Topic name required");
    }

    public Task ExecuteAsync(DeleteTopicRequestPayload request, CancellationToken cancellationToken)
        => _logManager.DeleteTopicAsync(request.Name, cancellationToken);
}

/// <summary>
/// Response payload for listing topics (wrapper for the operation handler).
/// </summary>
public readonly record struct ListTopicsResult
{
    public required TopicInfoPayload[] Topics { get; init; }
}

/// <summary>
/// Empty request for list operations.
/// </summary>
public readonly record struct EmptyRequest;

/// <summary>
/// Operation to list all topics.
/// </summary>
public sealed class ListTopicsOperation : INoRequestOperationHandler<ListTopicsResult>
{
    private readonly LogManager _logManager;

    public ListTopicsOperation(LogManager logManager) => _logManager = logManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListTopics;

    public Task<ListTopicsResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        var topics = _logManager.ListTopics()
            .Select(t => new TopicInfoPayload
            {
                Name = t.Name,
                PartitionCount = t.PartitionCount,
                Strategy = MapStorageModeToStrategy(t.StorageModeRaw),
            })
            .ToArray();

        return Task.FromResult(new ListTopicsResult { Topics = topics });
    }

    public void WriteResponse(IPayloadWriter writer, in ListTopicsResult response)
    {
        var responsePayload = new ListTopicsResponsePayload { Topics = response.Topics };
        responsePayload.WriteTo(writer);
    }

    /// <summary>
    /// Translate the topic's <c>storage.mode</c> config into the wire-level
    /// <see cref="ProduceStrategy"/> hint per ADR-014. Unknown / future
    /// values fall back to <see cref="ProduceStrategy.Replicated"/> so an
    /// old broker reporting a value a new client added later does not
    /// accidentally route producers through a non-existent path.
    /// </summary>
    private static ProduceStrategy MapStorageModeToStrategy(string? storageModeRaw) => storageModeRaw switch
    {
        "disaggregated-wal" => ProduceStrategy.WalViaBroker,
        "disaggregated-stateless" => ProduceStrategy.StatelessViaBroker,
        _ => ProduceStrategy.Replicated,
    };
}

/// <summary>
/// Operation to alter topic configuration.
/// </summary>
public sealed class AlterConfigOperation : IVoidOperationHandler<AlterConfigRequestPayload>
{
    private readonly LogManager _logManager;

    public AlterConfigOperation(LogManager logManager) => _logManager = logManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.AlterConfig;

    public AlterConfigRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => AlterConfigRequestPayload.Read(ref reader);

    public void ValidateRequest(in AlterConfigRequestPayload request)
    {
        if (string.IsNullOrEmpty(request.TopicName))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "Topic name required");
    }

    public Task ExecuteAsync(AlterConfigRequestPayload request, CancellationToken cancellationToken)
    {
        var configUpdates = new Dictionary<string, string>();
        if (request.Configs != null)
        {
            foreach (var config in request.Configs)
            {
                if (!string.IsNullOrEmpty(config.Key) && config.Value != null)
                    configUpdates[config.Key] = config.Value;
            }
        }

        var success = _logManager.UpdateTopicConfig(request.TopicName, configUpdates);
        if (!success)
            throw new SurgewaveOperationException(SurgewaveErrorCode.TopicNotFound, $"Topic '{request.TopicName}' not found");

        return Task.CompletedTask;
    }
}

/// <summary>
/// Response for describe config operation.
/// </summary>
public readonly record struct DescribeConfigResult
{
    public required string TopicName { get; init; }
    public required TopicConfigPayload[] Configs { get; init; }
}

/// <summary>
/// Operation to describe topic configuration.
/// </summary>
public sealed class DescribeConfigOperation : IOperationHandler<DescribeConfigRequestPayload, DescribeConfigResult>
{
    private readonly LogManager _logManager;

    public DescribeConfigOperation(LogManager logManager) => _logManager = logManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DescribeConfig;

    public DescribeConfigRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => DescribeConfigRequestPayload.Read(ref reader);

    public void ValidateRequest(in DescribeConfigRequestPayload request)
    {
        if (string.IsNullOrEmpty(request.TopicName))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "Topic name required");
    }

    public Task<DescribeConfigResult> ExecuteAsync(DescribeConfigRequestPayload request, CancellationToken cancellationToken)
    {
        var config = _logManager.GetTopicConfig(request.TopicName);
        if (config == null)
            throw new SurgewaveOperationException(SurgewaveErrorCode.TopicNotFound, $"Topic '{request.TopicName}' not found");

        var configs = config.Select(kv => new TopicConfigPayload { Key = kv.Key, Value = kv.Value }).ToArray();
        return Task.FromResult(new DescribeConfigResult
        {
            TopicName = request.TopicName,
            Configs = configs
        });
    }

    public void WriteResponse(IPayloadWriter writer, in DescribeConfigResult response)
    {
        var responsePayload = new DescribeConfigResponsePayload
        {
            TopicName = response.TopicName,
            Configs = response.Configs
        };
        responsePayload.WriteTo(writer);
    }
}

/// <summary>
/// Result for describe topic operation.
/// </summary>
public readonly record struct DescribeTopicResult
{
    public required DescribeTopicResponsePayload Payload { get; init; }
}

/// <summary>
/// Operation to describe a topic with full partition metadata.
/// </summary>
public sealed class DescribeTopicOperation : IOperationHandler<DescribeTopicRequestPayload, DescribeTopicResult>
{
    private readonly LogManager _logManager;
    private readonly NativeRequestContext _context;

    public DescribeTopicOperation(LogManager logManager, NativeRequestContext context)
    {
        _logManager = logManager;
        _context = context;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DescribeTopic;

    public DescribeTopicRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => DescribeTopicRequestPayload.Read(ref reader);

    public void ValidateRequest(in DescribeTopicRequestPayload request)
    {
        if (string.IsNullOrEmpty(request.TopicName))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "Topic name required");
    }

    public Task<DescribeTopicResult> ExecuteAsync(DescribeTopicRequestPayload request, CancellationToken cancellationToken)
    {
        var metadata = _logManager.GetTopicMetadata(request.TopicName);
        if (metadata == null)
            throw new SurgewaveOperationException(SurgewaveErrorCode.TopicNotFound, $"Topic '{request.TopicName}' not found");

        var partitions = new PartitionMetadataPayload[metadata.PartitionCount];
        var brokerId = _context.Config.BrokerId;

        for (int i = 0; i < metadata.PartitionCount; i++)
        {
            var tp = new TopicPartition { Topic = metadata.Name, Partition = i };
            var log = _logManager.GetLog(tp);

            // In standalone mode, current broker is leader with full replica set
            partitions[i] = new PartitionMetadataPayload
            {
                PartitionId = i,
                Leader = brokerId,
                LeaderEpoch = 0,
                Replicas = [brokerId],
                Isr = [brokerId],
                HighWatermark = log?.HighWatermark ?? 0,
                LogStartOffset = log?.LogStartOffset ?? 0
            };
        }

        var payload = new DescribeTopicResponsePayload
        {
            TopicName = metadata.Name,
            PartitionCount = metadata.PartitionCount,
            ReplicationFactor = metadata.ReplicationFactor,
            IsInternal = metadata.Name.StartsWith("__"),
            Partitions = partitions
        };

        return Task.FromResult(new DescribeTopicResult { Payload = payload });
    }

    public void WriteResponse(IPayloadWriter writer, in DescribeTopicResult response)
        => response.Payload.WriteTo(writer);
}

/// <summary>
/// Operation to add partitions to an existing topic.
/// </summary>
public sealed class CreatePartitionsOperation : IVoidOperationHandler<CreatePartitionsRequestPayload>
{
    private readonly LogManager _logManager;

    public CreatePartitionsOperation(LogManager logManager) => _logManager = logManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CreatePartitions;

    public CreatePartitionsRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => CreatePartitionsRequestPayload.Read(ref reader);

    public void ValidateRequest(in CreatePartitionsRequestPayload request)
    {
        if (string.IsNullOrEmpty(request.TopicName))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "Topic name required");
        if (request.TotalPartitions <= 0)
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "Total partitions must be positive");
    }

    public Task ExecuteAsync(CreatePartitionsRequestPayload request, CancellationToken cancellationToken)
    {
        var metadata = _logManager.GetTopicMetadata(request.TopicName);
        if (metadata == null)
            throw new SurgewaveOperationException(SurgewaveErrorCode.TopicNotFound, $"Topic '{request.TopicName}' not found");

        if (request.TotalPartitions <= metadata.PartitionCount)
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest,
                $"Topic currently has {metadata.PartitionCount} partitions. New count must be greater.");

        var success = _logManager.AddPartitions(request.TopicName, request.TotalPartitions);
        if (!success)
            throw new SurgewaveOperationException(SurgewaveErrorCode.UnknownError, "Failed to add partitions");

        return Task.CompletedTask;
    }
}

/// <summary>
/// Result for delete records operation.
/// </summary>
public readonly record struct DeleteRecordsResult
{
    public required DeleteRecordsResponsePayload Payload { get; init; }
}

/// <summary>
/// Operation to delete records up to a specified offset.
/// </summary>
public sealed class DeleteRecordsOperation : IOperationHandler<DeleteRecordsRequestPayload, DeleteRecordsResult>
{
    private readonly LogManager _logManager;

    public DeleteRecordsOperation(LogManager logManager) => _logManager = logManager;

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DeleteRecords;

    public DeleteRecordsRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => DeleteRecordsRequestPayload.Read(ref reader);

    public void ValidateRequest(in DeleteRecordsRequestPayload request)
    {
        if (string.IsNullOrEmpty(request.TopicName))
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "Topic name required");
        if (request.Partition < 0)
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, "Partition must be non-negative");
    }

    public Task<DeleteRecordsResult> ExecuteAsync(DeleteRecordsRequestPayload request, CancellationToken cancellationToken)
    {
        var tp = new TopicPartition { Topic = request.TopicName, Partition = request.Partition };
        var log = _logManager.GetLog(tp);
        if (log == null)
            throw new SurgewaveOperationException(SurgewaveErrorCode.TopicNotFound, $"Topic partition '{request.TopicName}-{request.Partition}' not found");

        if (log is not PartitionLog persistentLog)
            throw new SurgewaveOperationException(SurgewaveErrorCode.InvalidRequest, $"DeleteRecords not supported on ephemeral topic '{request.TopicName}'");

        var newLogStartOffset = persistentLog.DeleteRecordsToOffset(request.BeforeOffset);

        var payload = new DeleteRecordsResponsePayload
        {
            TopicName = request.TopicName,
            Partition = request.Partition,
            LowWatermark = newLogStartOffset
        };

        return Task.FromResult(new DeleteRecordsResult { Payload = payload });
    }

    public void WriteResponse(IPayloadWriter writer, in DeleteRecordsResult response)
        => response.Payload.WriteTo(writer);
}
