using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Gateway.Services;

/// <summary>
/// gRPC service implementation for transaction operations.
/// Uses Surgewave native client to communicate with the broker.
/// </summary>
public sealed class TransactionGatewayService : TransactionService.TransactionServiceBase
{
    private readonly ClusterRegistry _registry;
    private readonly ILogger<TransactionGatewayService> _logger;

    public TransactionGatewayService(ClusterRegistry registry, ILogger<TransactionGatewayService> logger)
    {
        _registry = registry;
        _logger = logger;
    }

    public override async Task<InitProducerIdResponse> InitProducerId(InitProducerIdRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var result = await client.Transactions.InitProducerIdAsync(
                string.IsNullOrEmpty(request.TransactionalId) ? null : request.TransactionalId,
                request.TransactionTimeoutMs > 0 ? request.TransactionTimeoutMs : 60000,
                context.CancellationToken);

            return new InitProducerIdResponse
            {
                ProducerId = result.ProducerId,
                ProducerEpoch = result.ProducerEpoch,
                Status = new ResponseStatus
                {
                    ErrorCode = (ErrorCode)result.ErrorCode
                }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize producer ID for {TransactionalId}", request.TransactionalId);
            return new InitProducerIdResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<AddPartitionsToTxnResponse> AddPartitionsToTxn(AddPartitionsToTxnRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);

            // Convert partitions to dictionary format expected by native client
            var topics = new Dictionary<string, List<int>>();
            foreach (var partition in request.Partitions)
            {
                if (!topics.TryGetValue(partition.Topic, out var partitions))
                {
                    partitions = [];
                    topics[partition.Topic] = partitions;
                }
                partitions.Add(partition.Partition);
            }

            var results = await client.Transactions.AddPartitionsToTxnAsync(
                request.TransactionalId,
                request.ProducerId,
                (short)request.ProducerEpoch,
                topics,
                context.CancellationToken);

            var response = new AddPartitionsToTxnResponse();

            foreach (var (topic, partitionResults) in results)
            {
                foreach (var partitionResult in partitionResults)
                {
                    response.Results.Add(new TopicPartitionResult
                    {
                        Topic = topic,
                        Partition = partitionResult.Partition,
                        Status = new ResponseStatus
                        {
                            ErrorCode = (ErrorCode)partitionResult.ErrorCode
                        }
                    });
                }
            }

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to add partitions to transaction {TransactionalId}", request.TransactionalId);

            // Return error for all requested partitions
            var response = new AddPartitionsToTxnResponse();
            foreach (var partition in request.Partitions)
            {
                response.Results.Add(new TopicPartitionResult
                {
                    Topic = partition.Topic,
                    Partition = partition.Partition,
                    Status = new ResponseStatus
                    {
                        ErrorCode = ErrorCode.Unknown,
                        ErrorMessage = ex.Message
                    }
                });
            }
            return response;
        }
    }

    public override Task<AddOffsetsToTxnResponse> AddOffsetsToTxn(AddOffsetsToTxnRequest request, ServerCallContext context)
    {
        // AddOffsetsToTxn is not yet implemented in native client
        _logger.LogWarning("AddOffsetsToTxn is not yet supported via gateway");

        return Task.FromResult(new AddOffsetsToTxnResponse
        {
            Status = new ResponseStatus
            {
                ErrorCode = ErrorCode.Unknown,
                ErrorMessage = "AddOffsetsToTxn is not yet implemented"
            }
        });
    }

    public override Task<TxnOffsetCommitResponse> TxnOffsetCommit(TxnOffsetCommitRequest request, ServerCallContext context)
    {
        // TxnOffsetCommit is not yet implemented in native client
        _logger.LogWarning("TxnOffsetCommit is not yet supported via gateway");

        var response = new TxnOffsetCommitResponse();
        foreach (var offset in request.Offsets)
        {
            response.Results.Add(new TxnOffsetCommitResult
            {
                Topic = offset.Topic,
                Partition = offset.Partition,
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = "TxnOffsetCommit is not yet implemented"
                }
            });
        }
        return Task.FromResult(response);
    }

    public override async Task<EndTxnResponse> EndTxn(EndTxnRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var errorCode = await client.Transactions.EndTxnAsync(
                request.TransactionalId,
                request.ProducerId,
                (short)request.ProducerEpoch,
                request.Commit,
                context.CancellationToken);

            return new EndTxnResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = (ErrorCode)errorCode
                }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to end transaction {TransactionalId}", request.TransactionalId);
            return new EndTxnResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<ListTransactionsResponse> ListTransactions(ListTransactionsRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var transactions = await client.Transactions.ListAsync(context.CancellationToken);

            var response = new ListTransactionsResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };

            foreach (var txn in transactions)
            {
                // Apply filters if specified
                if (request.StatesFilter.Count > 0 &&
                    !request.StatesFilter.Contains(txn.State, StringComparer.OrdinalIgnoreCase))
                    continue;

                if (request.ProducerIdFilter.Count > 0 &&
                    !request.ProducerIdFilter.Contains(txn.ProducerId))
                    continue;

                response.Transactions.Add(new TransactionListing
                {
                    TransactionalId = txn.TransactionalId,
                    ProducerId = txn.ProducerId,
                    State = txn.State
                });
            }

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list transactions");
            return new ListTransactionsResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<DescribeTransactionsResponse> DescribeTransactions(DescribeTransactionsRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var descriptions = await client.Transactions.DescribeAsync(
                request.TransactionalIds.ToList(),
                context.CancellationToken);

            var response = new DescribeTransactionsResponse();

            foreach (var desc in descriptions)
            {
                var txnDesc = new Api.Grpc.TransactionDescription
                {
                    TransactionalId = desc.TransactionalId,
                    State = desc.State,
                    ProducerId = desc.ProducerId,
                    ProducerEpoch = desc.ProducerEpoch,
                    Status = new ResponseStatus
                    {
                        ErrorCode = (ErrorCode)desc.ErrorCode
                    }
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

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to describe transactions");
            return new DescribeTransactionsResponse();
        }
    }
}
