using Kuestenlogik.Surgewave.Broker.Native.Operations.Topics;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol topic operations: CreateTopic, DeleteTopic, ListTopics, AlterConfig, DescribeConfig,
/// CreatePartitions, DeleteRecords.
/// </summary>
public sealed class NativeTopicHandler : NativeHandlerBase
{
    public NativeTopicHandler(LogManager logManager)
    {
        RegisterVoid<CreateTopicRequestPayload>(SurgewaveOpCode.CreateTopic, _ => new CreateTopicOperation(logManager));
        RegisterVoid<DeleteTopicRequestPayload>(SurgewaveOpCode.DeleteTopic, _ => new DeleteTopicOperation(logManager));
        RegisterNoRequest<ListTopicsResult>(SurgewaveOpCode.ListTopics, _ => new ListTopicsOperation(logManager));
        RegisterVoid<AlterConfigRequestPayload>(SurgewaveOpCode.AlterConfig, _ => new AlterConfigOperation(logManager));
        Register<DescribeConfigRequestPayload, DescribeConfigResult>(SurgewaveOpCode.DescribeConfig, _ => new DescribeConfigOperation(logManager));
        Register<DescribeTopicRequestPayload, DescribeTopicResult>(SurgewaveOpCode.DescribeTopic, ctx => new DescribeTopicOperation(logManager, ctx));
        RegisterVoid<CreatePartitionsRequestPayload>(SurgewaveOpCode.CreatePartitions, _ => new CreatePartitionsOperation(logManager));
        Register<DeleteRecordsRequestPayload, DeleteRecordsResult>(SurgewaveOpCode.DeleteRecords, _ => new DeleteRecordsOperation(logManager));
    }
}
