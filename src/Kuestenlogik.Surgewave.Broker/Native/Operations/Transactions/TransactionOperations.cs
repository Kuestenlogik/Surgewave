using Kuestenlogik.Surgewave.Broker.Native.Handlers;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;
using TxnDescription = Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions.TransactionDescription;

namespace Kuestenlogik.Surgewave.Broker.Native.Operations.Transactions;

/// <summary>
/// Maps protocol-neutral transaction statuses to Surgewave native error codes (#59). The native
/// transaction ops now speak the neutral coordinator contract directly instead of building Kafka DTOs.
/// </summary>
internal static class TxnStatusMapper
{
    public static SurgewaveErrorCode ToSurgewave(TxnErrorStatus status)
    {
        return status switch
        {
            TxnErrorStatus.None => SurgewaveErrorCode.None,
            TxnErrorStatus.InvalidProducerEpoch => SurgewaveErrorCode.InvalidProducerEpoch,
            TxnErrorStatus.UnknownProducerId => SurgewaveErrorCode.UnknownProducerId,
            TxnErrorStatus.InvalidTxnState => SurgewaveErrorCode.InvalidTxnState,
            _ => SurgewaveErrorCode.UnknownError
        };
    }
}

/// <summary>
/// Result for init producer id operation.
/// </summary>
public readonly record struct InitProducerIdResult : IOperationResult
{
    public required InitProducerIdResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to initialize a producer id.
/// </summary>
public sealed class InitProducerIdOperation : IOperationHandler<InitProducerIdRequestPayload, InitProducerIdResult>
{
    private readonly TransactionCoordinator _coordinator;
    private readonly uint _requestId;

    public InitProducerIdOperation(TransactionCoordinator coordinator, uint requestId)
    {
        _coordinator = coordinator;
        _requestId = requestId;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.InitProducerId;

    public InitProducerIdRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => InitProducerIdRequestPayload.Read(ref reader);

    public void ValidateRequest(in InitProducerIdRequestPayload request) { }

    public async Task<InitProducerIdResult> ExecuteAsync(InitProducerIdRequestPayload request, CancellationToken cancellationToken)
    {
        var command = new InitProducerIdCommand
        {
            TransactionalId = request.TransactionalId,
            TransactionTimeoutMs = request.TransactionTimeoutMs,
            ProducerId = request.ProducerId,
            ProducerEpoch = request.ProducerEpoch
        };

        var response = await _coordinator.InitProducerIdAsync(command, cancellationToken);
        var errorCode = TxnStatusMapper.ToSurgewave(response.Status);

        var responsePayload = new InitProducerIdResponsePayload
        {
            ErrorCode = (ushort)errorCode,
            ProducerId = response.ProducerId,
            ProducerEpoch = response.ProducerEpoch
        };

        return new InitProducerIdResult { Response = responsePayload, ErrorCode = errorCode };
    }

    public void WriteResponse(IPayloadWriter writer, in InitProducerIdResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for add partitions to txn operation.
/// </summary>
public readonly record struct AddPartitionsToTxnResult
{
    public required AddPartitionsToTxnResponsePayload Response { get; init; }
}

/// <summary>
/// Operation to add partitions to a transaction.
/// </summary>
public sealed class AddPartitionsToTxnOperation : IOperationHandler<AddPartitionsToTxnRequestPayload, AddPartitionsToTxnResult>
{
    private readonly TransactionCoordinator _coordinator;
    private readonly uint _requestId;

    public AddPartitionsToTxnOperation(TransactionCoordinator coordinator, uint requestId)
    {
        _coordinator = coordinator;
        _requestId = requestId;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.AddPartitionsToTxn;

    public AddPartitionsToTxnRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => AddPartitionsToTxnRequestPayload.Read(ref reader);

    public void ValidateRequest(in AddPartitionsToTxnRequestPayload request) { }

    public Task<AddPartitionsToTxnResult> ExecuteAsync(AddPartitionsToTxnRequestPayload request, CancellationToken cancellationToken)
    {
        var commandTopics = new List<AddPartitionsTopic>();
        foreach (var (topic, partitions) in request.Topics)
        {
            commandTopics.Add(new AddPartitionsTopic(topic, partitions));
        }

        var command = new AddPartitionsToTxnCommand
        {
            TransactionalId = request.TransactionalId,
            ProducerId = request.ProducerId,
            ProducerEpoch = request.ProducerEpoch,
            Topics = commandTopics
        };

        var result = _coordinator.AddPartitionsToTxn(command);

        var results = new Dictionary<string, List<PartitionResult>>();
        foreach (var topic in result.Topics)
        {
            var payloadResults = new List<PartitionResult>();
            foreach (var partitionResult in topic.Partitions)
            {
                payloadResults.Add(new PartitionResult
                {
                    Partition = partitionResult.Partition,
                    ErrorCode = (ushort)TxnStatusMapper.ToSurgewave(partitionResult.Status)
                });
            }
            results[topic.Topic] = payloadResults;
        }

        var responsePayload = new AddPartitionsToTxnResponsePayload { Results = results };
        return Task.FromResult(new AddPartitionsToTxnResult { Response = responsePayload });
    }

    public void WriteResponse(IPayloadWriter writer, in AddPartitionsToTxnResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Request payload for add offsets to txn operation.
/// </summary>
public readonly record struct AddOffsetsToTxnRequest
{
    public required string TransactionalId { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
    public required string GroupId { get; init; }

    public static AddOffsetsToTxnRequest Read(ref SurgewavePayloadReader reader)
        => new()
        {
            TransactionalId = reader.ReadString() ?? string.Empty,
            ProducerId = reader.ReadInt64(),
            ProducerEpoch = reader.ReadInt16(),
            GroupId = reader.ReadString() ?? string.Empty
        };
}

/// <summary>
/// Result for add offsets to txn operation.
/// </summary>
public readonly record struct AddOffsetsToTxnResult : IOperationResult
{
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to add offsets to a transaction.
/// </summary>
public sealed class AddOffsetsToTxnOperation : IOperationHandler<AddOffsetsToTxnRequest, AddOffsetsToTxnResult>
{
    private readonly TransactionCoordinator _coordinator;
    private readonly uint _requestId;

    public AddOffsetsToTxnOperation(TransactionCoordinator coordinator, uint requestId)
    {
        _coordinator = coordinator;
        _requestId = requestId;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.AddOffsetsToTxn;

    public AddOffsetsToTxnRequest ParseRequest(ref SurgewavePayloadReader reader)
        => AddOffsetsToTxnRequest.Read(ref reader);

    public void ValidateRequest(in AddOffsetsToTxnRequest request) { }

    public Task<AddOffsetsToTxnResult> ExecuteAsync(AddOffsetsToTxnRequest request, CancellationToken cancellationToken)
    {
        var command = new AddOffsetsToTxnCommand
        {
            TransactionalId = request.TransactionalId,
            ProducerId = request.ProducerId,
            ProducerEpoch = request.ProducerEpoch,
            GroupId = request.GroupId
        };

        var response = _coordinator.AddOffsetsToTxn(command);
        var errorCode = TxnStatusMapper.ToSurgewave(response.Status);

        return Task.FromResult(new AddOffsetsToTxnResult { ErrorCode = errorCode });
    }

    public void WriteResponse(IPayloadWriter writer, in AddOffsetsToTxnResult response)
        => writer.WriteUInt16((ushort)response.ErrorCode);
}

/// <summary>
/// Result for txn offset commit operation.
/// </summary>
public readonly record struct TxnOffsetCommitResult
{
    public required TxnOffsetCommitResponsePayload Response { get; init; }
}

/// <summary>
/// Operation to commit offsets in a transaction.
/// </summary>
public sealed class TxnOffsetCommitOperation : IOperationHandler<TxnOffsetCommitRequestPayload, TxnOffsetCommitResult>
{
    private readonly TransactionCoordinator _coordinator;
    private readonly uint _requestId;

    public TxnOffsetCommitOperation(TransactionCoordinator coordinator, uint requestId)
    {
        _coordinator = coordinator;
        _requestId = requestId;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.TxnOffsetCommit;

    public TxnOffsetCommitRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => TxnOffsetCommitRequestPayload.Read(ref reader);

    public void ValidateRequest(in TxnOffsetCommitRequestPayload request) { }

    public Task<TxnOffsetCommitResult> ExecuteAsync(TxnOffsetCommitRequestPayload request, CancellationToken cancellationToken)
    {
        // The Surgewave native protocol's TxnOffsetCommit payload still
        // identifies topics by name. We build the neutral command with Name set
        // and TopicId left empty — the v6 (TopicId) path only kicks in for
        // Kafka-protocol clients negotiated up to v6+, not the native one.
        var commandTopics = new List<TxnOffsetCommitTopic>(request.Topics.Count);
        foreach (var (topic, partitions) in request.Topics)
        {
            var commandPartitions = new List<global::Kuestenlogik.Surgewave.Coordination.Transactions.TxnOffsetCommitPartition>();
            foreach (var partition in partitions)
            {
                commandPartitions.Add(new global::Kuestenlogik.Surgewave.Coordination.Transactions.TxnOffsetCommitPartition(partition.Partition, partition.CommittedOffset, partition.Metadata));
            }
            commandTopics.Add(new TxnOffsetCommitTopic { Name = topic, TopicId = Guid.Empty, Partitions = commandPartitions });
        }

        var command = new TxnOffsetCommitCommand
        {
            TransactionalId = request.TransactionalId,
            GroupId = request.GroupId,
            ProducerId = request.ProducerId,
            ProducerEpoch = request.ProducerEpoch,
            Topics = commandTopics
        };

        var result = _coordinator.TxnOffsetCommit(command);

        var results = new Dictionary<string, List<PartitionResult>>();
        foreach (var topic in result.Topics)
        {
            var payloadResults = new List<PartitionResult>();
            foreach (var partitionResult in topic.Partitions)
            {
                payloadResults.Add(new PartitionResult
                {
                    Partition = partitionResult.Partition,
                    ErrorCode = (ushort)TxnStatusMapper.ToSurgewave(partitionResult.Status)
                });
            }
            // Topic.Name is the working identifier server-side; even after a
            // v6 wire read the coordinator resolves it before storing.
            results[topic.Name ?? string.Empty] = payloadResults;
        }

        var responsePayload = new TxnOffsetCommitResponsePayload { Topics = results };
        return Task.FromResult(new TxnOffsetCommitResult { Response = responsePayload });
    }

    public void WriteResponse(IPayloadWriter writer, in TxnOffsetCommitResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for end txn operation.
/// </summary>
public readonly record struct EndTxnResult : IOperationResult
{
    public required EndTxnResponsePayload Response { get; init; }
    public required SurgewaveErrorCode ErrorCode { get; init; }
}

/// <summary>
/// Operation to end a transaction.
/// </summary>
public sealed class EndTxnOperation : IOperationHandler<EndTxnRequestPayload, EndTxnResult>
{
    private readonly TransactionCoordinator _coordinator;
    private readonly uint _requestId;

    public EndTxnOperation(TransactionCoordinator coordinator, uint requestId)
    {
        _coordinator = coordinator;
        _requestId = requestId;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.EndTxn;

    public EndTxnRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => EndTxnRequestPayload.Read(ref reader);

    public void ValidateRequest(in EndTxnRequestPayload request) { }

    public async Task<EndTxnResult> ExecuteAsync(EndTxnRequestPayload request, CancellationToken cancellationToken)
    {
        var command = new EndTxnCommand
        {
            TransactionalId = request.TransactionalId,
            ProducerId = request.ProducerId,
            ProducerEpoch = request.ProducerEpoch,
            Committed = request.Committed
        };

        var response = await _coordinator.EndTxnAsync(command, cancellationToken);
        var errorCode = TxnStatusMapper.ToSurgewave(response.Status);

        var responsePayload = new EndTxnResponsePayload { ErrorCode = (ushort)errorCode };
        return new EndTxnResult { Response = responsePayload, ErrorCode = errorCode };
    }

    public void WriteResponse(IPayloadWriter writer, in EndTxnResult response)
        => response.Response.WriteTo(writer);
}

/// <summary>
/// Result for list transactions operation.
/// </summary>
public readonly record struct ListTransactionsResult
{
    public required SurgewaveErrorCode ErrorCode { get; init; }
    public required int TransactionCount { get; init; }
}

/// <summary>
/// Operation to list transactions.
/// </summary>
public sealed class ListTransactionsOperation : INoRequestOperationHandler<ListTransactionsResult>
{
    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListTransactions;

    public Task<ListTransactionsResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        return Task.FromResult(new ListTransactionsResult
        {
            ErrorCode = SurgewaveErrorCode.None,
            TransactionCount = 0
        });
    }

    public void WriteResponse(IPayloadWriter writer, in ListTransactionsResult response)
    {
        writer.WriteUInt16((ushort)response.ErrorCode);
        writer.WriteInt32(response.TransactionCount);
    }
}

/// <summary>
/// Result for describe transactions operation.
/// </summary>
public readonly record struct DescribeTransactionsResult
{
    public required DescribeTransactionsResponsePayload Response { get; init; }
}

/// <summary>
/// Operation to describe transactions.
/// </summary>
public sealed class DescribeTransactionsOperation : IOperationHandler<DescribeTransactionsRequestPayload, DescribeTransactionsResult>
{
    public SurgewaveOpCode OpCode => SurgewaveOpCode.DescribeTransactions;

    public DescribeTransactionsRequestPayload ParseRequest(ref SurgewavePayloadReader reader)
        => DescribeTransactionsRequestPayload.Read(ref reader);

    public void ValidateRequest(in DescribeTransactionsRequestPayload request) { }

    public Task<DescribeTransactionsResult> ExecuteAsync(DescribeTransactionsRequestPayload request, CancellationToken cancellationToken)
    {
        var transactions = new List<TxnDescription>();
        foreach (var txnId in request.TransactionalIds)
        {
            transactions.Add(new TxnDescription
            {
                TransactionalId = txnId,
                ErrorCode = (ushort)SurgewaveErrorCode.None,
                State = "Unknown",
                ProducerId = 0L,
                ProducerEpoch = 0,
                Partitions = new List<TransactionPartition>()
            });
        }

        var responsePayload = new DescribeTransactionsResponsePayload
        {
            ErrorCode = (ushort)SurgewaveErrorCode.None,
            Transactions = transactions
        };

        return Task.FromResult(new DescribeTransactionsResult { Response = responsePayload });
    }

    public void WriteResponse(IPayloadWriter writer, in DescribeTransactionsResult response)
        => response.Response.WriteTo(writer);
}
