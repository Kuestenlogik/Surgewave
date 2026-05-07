using Google.Protobuf;
using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Result from InitProducerId operation.
/// </summary>
public record InitProducerIdResultDto(int ErrorCode, long ProducerId, int ProducerEpoch);

/// <summary>
/// Result from AddPartitionsToTxn operation.
/// </summary>
public record AddPartitionsToTxnResultDto(List<(string Topic, int Partition, int ErrorCode)> Results);

/// <summary>
/// Result from AddOffsetsToTxn operation.
/// </summary>
public record AddOffsetsToTxnResultDto(int ErrorCode);

/// <summary>
/// Result from TxnOffsetCommit operation.
/// </summary>
public record TxnOffsetCommitResultDto(List<(string Topic, int Partition, int ErrorCode)> Results);

/// <summary>
/// Result from EndTxn operation.
/// </summary>
public record EndTxnResultDto(int ErrorCode);

/// <summary>
/// Transaction listing for ListTransactions.
/// </summary>
public record TransactionListingDto(string TransactionalId, long ProducerId, string State);

/// <summary>
/// Transaction description for DescribeTransactions.
/// </summary>
public record TransactionDescriptionDto(
    string TransactionalId,
    string State,
    long ProducerId,
    int ProducerEpoch,
    long TransactionTimeoutMs,
    long TransactionStartTimeMs,
    List<(string Topic, int Partition)> Partitions,
    int ErrorCode);

/// <summary>
/// Delegate for InitProducerId operation.
/// </summary>
public delegate InitProducerIdResultDto InitProducerIdDelegate(
    string transactionalId,
    int transactionTimeoutMs,
    long producerId,
    int producerEpoch);

/// <summary>
/// Delegate for AddPartitionsToTxn operation.
/// </summary>
public delegate AddPartitionsToTxnResultDto AddPartitionsToTxnDelegate(
    string transactionalId,
    long producerId,
    int producerEpoch,
    List<(string Topic, int Partition)> partitions);

/// <summary>
/// Delegate for AddOffsetsToTxn operation.
/// </summary>
public delegate AddOffsetsToTxnResultDto AddOffsetsToTxnDelegate(
    string transactionalId,
    long producerId,
    int producerEpoch,
    string groupId);

/// <summary>
/// Delegate for TxnOffsetCommit operation.
/// </summary>
public delegate TxnOffsetCommitResultDto TxnOffsetCommitDelegate(
    string transactionalId,
    string groupId,
    long producerId,
    int producerEpoch,
    int generationId,
    string memberId,
    List<(string Topic, int Partition, long Offset, string? Metadata)> offsets);

/// <summary>
/// Delegate for EndTxn operation.
/// </summary>
public delegate EndTxnResultDto EndTxnDelegate(
    string transactionalId,
    long producerId,
    int producerEpoch,
    bool commit);

/// <summary>
/// Delegate for ListTransactions operation.
/// </summary>
public delegate List<TransactionListingDto> ListTransactionsDelegate(
    List<string>? statesFilter,
    List<long>? producerIdFilter);

/// <summary>
/// Delegate for DescribeTransactions operation.
/// </summary>
public delegate List<TransactionDescriptionDto> DescribeTransactionsDelegate(List<string> transactionalIds);

/// <summary>
/// gRPC TransactionService implementation.
/// </summary>
public class TransactionServiceImpl : TransactionService.TransactionServiceBase
{
    private readonly InitProducerIdDelegate _initProducerId;
    private readonly AddPartitionsToTxnDelegate _addPartitionsToTxn;
    private readonly AddOffsetsToTxnDelegate _addOffsetsToTxn;
    private readonly TxnOffsetCommitDelegate _txnOffsetCommit;
    private readonly EndTxnDelegate _endTxn;
    private readonly ListTransactionsDelegate _listTransactions;
    private readonly DescribeTransactionsDelegate _describeTransactions;

    public TransactionServiceImpl(
        InitProducerIdDelegate initProducerId,
        AddPartitionsToTxnDelegate addPartitionsToTxn,
        AddOffsetsToTxnDelegate addOffsetsToTxn,
        TxnOffsetCommitDelegate txnOffsetCommit,
        EndTxnDelegate endTxn,
        ListTransactionsDelegate listTransactions,
        DescribeTransactionsDelegate describeTransactions)
    {
        _initProducerId = initProducerId;
        _addPartitionsToTxn = addPartitionsToTxn;
        _addOffsetsToTxn = addOffsetsToTxn;
        _txnOffsetCommit = txnOffsetCommit;
        _endTxn = endTxn;
        _listTransactions = listTransactions;
        _describeTransactions = describeTransactions;
    }

    public override Task<InitProducerIdResponse> InitProducerId(InitProducerIdRequest request, ServerCallContext context)
    {
        var result = _initProducerId(
            request.TransactionalId,
            request.TransactionTimeoutMs,
            request.ProducerId,
            request.ProducerEpoch);

        return Task.FromResult(new InitProducerIdResponse
        {
            ProducerId = result.ProducerId,
            ProducerEpoch = result.ProducerEpoch,
            Status = new ResponseStatus { ErrorCode = MapErrorCode(result.ErrorCode) }
        });
    }

    public override Task<AddPartitionsToTxnResponse> AddPartitionsToTxn(AddPartitionsToTxnRequest request, ServerCallContext context)
    {
        var partitions = request.Partitions
            .Select(p => (p.Topic, p.Partition))
            .ToList();

        var result = _addPartitionsToTxn(
            request.TransactionalId,
            request.ProducerId,
            request.ProducerEpoch,
            partitions);

        var response = new AddPartitionsToTxnResponse();
        foreach (var (topic, partition, errorCode) in result.Results)
        {
            response.Results.Add(new TopicPartitionResult
            {
                Topic = topic,
                Partition = partition,
                Status = new ResponseStatus { ErrorCode = MapErrorCode(errorCode) }
            });
        }

        return Task.FromResult(response);
    }

    public override Task<AddOffsetsToTxnResponse> AddOffsetsToTxn(AddOffsetsToTxnRequest request, ServerCallContext context)
    {
        var result = _addOffsetsToTxn(
            request.TransactionalId,
            request.ProducerId,
            request.ProducerEpoch,
            request.GroupId);

        return Task.FromResult(new AddOffsetsToTxnResponse
        {
            Status = new ResponseStatus { ErrorCode = MapErrorCode(result.ErrorCode) }
        });
    }

    public override Task<TxnOffsetCommitResponse> TxnOffsetCommit(TxnOffsetCommitRequest request, ServerCallContext context)
    {
        var offsets = request.Offsets
            .Select(o => (o.Topic, o.Partition, o.Offset, string.IsNullOrEmpty(o.Metadata) ? (string?)null : o.Metadata))
            .ToList();

        var result = _txnOffsetCommit(
            request.TransactionalId,
            request.GroupId,
            request.ProducerId,
            request.ProducerEpoch,
            request.GenerationId,
            request.MemberId,
            offsets);

        var response = new TxnOffsetCommitResponse();
        foreach (var (topic, partition, errorCode) in result.Results)
        {
            response.Results.Add(new TxnOffsetCommitResult
            {
                Topic = topic,
                Partition = partition,
                Status = new ResponseStatus { ErrorCode = MapErrorCode(errorCode) }
            });
        }

        return Task.FromResult(response);
    }

    public override Task<EndTxnResponse> EndTxn(EndTxnRequest request, ServerCallContext context)
    {
        var result = _endTxn(
            request.TransactionalId,
            request.ProducerId,
            request.ProducerEpoch,
            request.Commit);

        return Task.FromResult(new EndTxnResponse
        {
            Status = new ResponseStatus { ErrorCode = MapErrorCode(result.ErrorCode) }
        });
    }

    public override Task<ListTransactionsResponse> ListTransactions(ListTransactionsRequest request, ServerCallContext context)
    {
        var statesFilter = request.StatesFilter.Count > 0 ? request.StatesFilter.ToList() : null;
        var producerIdFilter = request.ProducerIdFilter.Count > 0 ? request.ProducerIdFilter.ToList() : null;

        var transactions = _listTransactions(statesFilter, producerIdFilter);

        var response = new ListTransactionsResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        foreach (var txn in transactions)
        {
            response.Transactions.Add(new TransactionListing
            {
                TransactionalId = txn.TransactionalId,
                ProducerId = txn.ProducerId,
                State = txn.State
            });
        }

        return Task.FromResult(response);
    }

    public override Task<DescribeTransactionsResponse> DescribeTransactions(DescribeTransactionsRequest request, ServerCallContext context)
    {
        var descriptions = _describeTransactions(request.TransactionalIds.ToList());

        var response = new DescribeTransactionsResponse();

        foreach (var desc in descriptions)
        {
            var txnDesc = new TransactionDescription
            {
                TransactionalId = desc.TransactionalId,
                State = desc.State,
                ProducerId = desc.ProducerId,
                ProducerEpoch = desc.ProducerEpoch,
                TransactionTimeoutMs = desc.TransactionTimeoutMs,
                TransactionStartTimeMs = desc.TransactionStartTimeMs,
                Status = new ResponseStatus { ErrorCode = MapErrorCode(desc.ErrorCode) }
            };

            foreach (var (topic, partition) in desc.Partitions)
            {
                txnDesc.Partitions.Add(new TopicPartition
                {
                    Topic = topic,
                    Partition = partition
                });
            }

            response.Transactions.Add(txnDesc);
        }

        return Task.FromResult(response);
    }

    private static ErrorCode MapErrorCode(int errorCode) => errorCode switch
    {
        0 => ErrorCode.None,
        15 => ErrorCode.CoordinatorNotAvailable,
        16 => ErrorCode.NotCoordinator,
        47 => ErrorCode.InvalidTxnState,
        51 => ErrorCode.ConcurrentTransactions,
        53 => ErrorCode.TransactionalIdAuthorizationFailed,
        57 => ErrorCode.InvalidProducerEpoch,
        59 => ErrorCode.UnknownProducerId,
        _ => ErrorCode.Unknown
    };
}
