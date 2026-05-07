namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for DescribeTopic request.
/// </summary>
public readonly record struct DescribeTopicRequestPayload
{
    public string TopicName { get; init; }

    public static DescribeTopicRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new DescribeTopicRequestPayload
        {
            TopicName = reader.ReadString() ?? string.Empty
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TopicName);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TopicName);
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(TopicName ?? "");
}
