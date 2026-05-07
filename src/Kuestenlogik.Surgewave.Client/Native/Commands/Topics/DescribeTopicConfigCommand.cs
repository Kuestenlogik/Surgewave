using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Topics;

/// <summary>
/// Command to describe topic configuration.
/// </summary>
public sealed class DescribeTopicConfigCommand : ISurgewaveCommand<Dictionary<string, string>>
{
    private readonly DescribeConfigRequestPayload _request;

    public DescribeTopicConfigCommand(string topicName)
    {
        _request = new DescribeConfigRequestPayload { TopicName = topicName };
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.DescribeConfig;

    public void WriteRequest(ref SurgewavePayloadWriter writer) => _request.Write(ref writer);

    public int EstimateRequestSize() => _request.EstimateSize();

    public Dictionary<string, string> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = DescribeConfigResponsePayload.Read(ref reader);
        return response.Configs.ToDictionary(c => c.Key, c => c.Value);
    }
}
