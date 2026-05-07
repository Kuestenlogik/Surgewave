using Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Transactions;

/// <summary>
/// Command to initialize a producer ID for idempotent or transactional producing.
/// </summary>
public sealed class InitProducerIdCommand : ISurgewaveCommand<InitProducerIdResult>
{
    private readonly InitProducerIdRequestPayload _request;

    public InitProducerIdCommand(string? transactionalId, int transactionTimeoutMs)
    {
        _request = new InitProducerIdRequestPayload
        {
            TransactionalId = transactionalId,
            TransactionTimeoutMs = transactionTimeoutMs,
            ProducerId = -1,
            ProducerEpoch = -1
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.InitProducerId;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public InitProducerIdResult ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var responsePayload = InitProducerIdResponsePayload.Read(ref reader);
        return new InitProducerIdResult(
            (SurgewaveErrorCode)responsePayload.ErrorCode,
            responsePayload.ProducerId,
            responsePayload.ProducerEpoch);
    }
}

/// <summary>
/// Command to add partitions to an ongoing transaction.
/// </summary>
public sealed class AddPartitionsToTxnCommand : ISurgewaveCommand<Dictionary<string, List<PartitionTxnResult>>>
{
    private readonly AddPartitionsToTxnRequestPayload _request;

    public AddPartitionsToTxnCommand(
        string transactionalId,
        long producerId,
        short producerEpoch,
        Dictionary<string, List<int>> topics)
    {
        _request = new AddPartitionsToTxnRequestPayload
        {
            TransactionalId = transactionalId,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            Topics = topics
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.AddPartitionsToTxn;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public Dictionary<string, List<PartitionTxnResult>> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var responsePayload = AddPartitionsToTxnResponsePayload.Read(ref reader);

        var result = new Dictionary<string, List<PartitionTxnResult>>();
        foreach (var (topic, partitionResults) in responsePayload.Results)
        {
            var clientResults = new List<PartitionTxnResult>();
            foreach (var partitionResult in partitionResults)
            {
                clientResults.Add(new PartitionTxnResult(
                    partitionResult.Partition,
                    (SurgewaveErrorCode)partitionResult.ErrorCode));
            }
            result[topic] = clientResults;
        }

        return result;
    }
}

/// <summary>
/// Command to commit or abort a transaction.
/// </summary>
public sealed class EndTxnCommand : ISurgewaveCommand<SurgewaveErrorCode>
{
    private readonly EndTxnRequestPayload _request;

    public EndTxnCommand(string transactionalId, long producerId, short producerEpoch, bool commit)
    {
        _request = new EndTxnRequestPayload
        {
            TransactionalId = transactionalId,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            Committed = commit
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.EndTxn;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public SurgewaveErrorCode ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var responsePayload = EndTxnResponsePayload.Read(ref reader);
        return (SurgewaveErrorCode)responsePayload.ErrorCode;
    }
}

/// <summary>
/// Command to list all active transactions.
/// </summary>
public sealed class ListTransactionsCommand : ISurgewaveCommand<List<TransactionInfo>>
{
    public SurgewaveOpCode OpCode => SurgewaveOpCode.ListTransactions;
    public void WriteRequest(ref SurgewavePayloadWriter writer) { }
    public int EstimateRequestSize() => 0;

    public List<TransactionInfo> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var errorCode = (SurgewaveErrorCode)reader.ReadUInt16();
        var count = reader.ReadInt32();
        var result = new List<TransactionInfo>(count);

        for (int i = 0; i < count; i++)
        {
            var txnId = reader.ReadString() ?? string.Empty;
            var state = reader.ReadString() ?? string.Empty;
            var prodId = reader.ReadInt64();
            var epoch = reader.ReadInt16();
            result.Add(new TransactionInfo(txnId, state, prodId, epoch));
        }

        return result;
    }
}

/// <summary>
/// Command to add consumer group offsets to a transaction.
/// This must be called before TxnOffsetCommit.
/// </summary>
public sealed class AddOffsetsToTxnCommand : ISurgewaveCommand<SurgewaveErrorCode>
{
    private readonly AddOffsetsToTxnRequestPayload _request;

    public AddOffsetsToTxnCommand(
        string transactionalId,
        long producerId,
        short producerEpoch,
        string groupId)
    {
        _request = new AddOffsetsToTxnRequestPayload
        {
            TransactionalId = transactionalId,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            GroupId = groupId
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.AddOffsetsToTxn;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public SurgewaveErrorCode ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var responsePayload = AddOffsetsToTxnResponsePayload.Read(ref reader);
        return (SurgewaveErrorCode)responsePayload.ErrorCode;
    }
}

/// <summary>
/// Command to commit consumer group offsets within a transaction.
/// AddOffsetsToTxn must be called before this command.
/// </summary>
public sealed class TxnOffsetCommitCommand : ISurgewaveCommand<Dictionary<string, List<PartitionTxnResult>>>
{
    private readonly TxnOffsetCommitRequestPayload _request;

    public TxnOffsetCommitCommand(
        string transactionalId,
        string groupId,
        long producerId,
        short producerEpoch,
        Dictionary<string, List<TxnOffsetCommitPartition>> topics)
    {
        _request = new TxnOffsetCommitRequestPayload
        {
            TransactionalId = transactionalId,
            GroupId = groupId,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            Topics = topics
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.TxnOffsetCommit;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public Dictionary<string, List<PartitionTxnResult>> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var responsePayload = TxnOffsetCommitResponsePayload.Read(ref reader);

        var result = new Dictionary<string, List<PartitionTxnResult>>();
        foreach (var (topic, partitionResults) in responsePayload.Topics)
        {
            var clientResults = new List<PartitionTxnResult>();
            foreach (var partitionResult in partitionResults)
            {
                clientResults.Add(new PartitionTxnResult(
                    partitionResult.Partition,
                    (SurgewaveErrorCode)partitionResult.ErrorCode));
            }
            result[topic] = clientResults;
        }

        return result;
    }
}

/// <summary>
/// Command to describe specific transactions.
/// </summary>
public sealed class DescribeTransactionsCommand : ISurgewaveCommand<List<Operations.Transactions.TransactionDescription>>
{
    private readonly DescribeTransactionsRequestPayload _request;

    public DescribeTransactionsCommand(List<string> transactionalIds)
    {
        _request = new DescribeTransactionsRequestPayload
        {
            TransactionalIds = transactionalIds
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DescribeTransactions;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public List<Operations.Transactions.TransactionDescription> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var responsePayload = DescribeTransactionsResponsePayload.Read(ref reader);

        var result = new List<Operations.Transactions.TransactionDescription>();
        foreach (var txn in responsePayload.Transactions)
        {
            var partitions = new List<(string, int)>();
            foreach (var partition in txn.Partitions)
            {
                partitions.Add((partition.Topic, partition.Partition));
            }

            result.Add(new Operations.Transactions.TransactionDescription(
                txn.TransactionalId,
                (SurgewaveErrorCode)txn.ErrorCode,
                txn.State,
                txn.ProducerId,
                txn.ProducerEpoch,
                partitions));
        }

        return result;
    }
}
