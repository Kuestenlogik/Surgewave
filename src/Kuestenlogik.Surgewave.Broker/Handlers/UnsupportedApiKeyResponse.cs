using Kuestenlogik.Surgewave.Protocol.Kafka;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Minimal Kafka response for API keys that have no registered handler. Returns just
/// the correlation ID — the client can parse the frame and recognise the API key
/// is not supported without the connection being torn down.
///
/// <para>
/// This is the broker-side counterpart to what a real Kafka broker returns for
/// unsupported API versions: a response header with the correlation ID, allowing
/// the client to match the inflight request and move on. Confluent.Kafka (librdkafka)
/// interprets a response with only the correlation ID as an implicit unsupported-API
/// signal when no body follows.
/// </para>
/// </summary>
internal sealed class UnsupportedApiKeyResponse : KafkaResponse
{
    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // Minimal response: just the correlation ID. The 4-byte size prefix is added
        // by the response writer (WriteResponseAsync) that wraps this frame.
        writer.WriteInt32(CorrelationId);
    }
}
