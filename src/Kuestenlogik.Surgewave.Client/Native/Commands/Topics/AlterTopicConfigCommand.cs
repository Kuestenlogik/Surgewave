using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Topics;

/// <summary>
/// Command to alter topic configuration.
/// </summary>
public sealed class AlterTopicConfigCommand : ISurgewaveVoidCommand
{
    private readonly AlterConfigRequestPayload _request;

    public AlterTopicConfigCommand(string topicName, Dictionary<string, string> config)
    {
        var configs = config
            .Select(kv => new TopicConfigPayload { Key = kv.Key, Value = kv.Value })
            .ToArray();

        _request = new AlterConfigRequestPayload
        {
            TopicName = topicName,
            Configs = configs
        };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.AlterConfig;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public Unit ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        ValidateResponse(ref reader, header);
        return Unit.Value;
    }

    public void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header) { }
}
