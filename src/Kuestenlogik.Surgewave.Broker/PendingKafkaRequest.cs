using Kuestenlogik.Surgewave.Protocol.Kafka;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Represents a pending Kafka protocol request in the Channel pipeline.
/// Carries ownership of the rented payload buffer (must be returned to ArrayPool after processing).
/// </summary>
/// <param name="Request">The parsed Kafka request.</param>
/// <param name="Size">The original request size in bytes.</param>
/// <param name="RentedBuffer">Buffer rented from ArrayPool. Must be returned after processing.</param>
internal readonly record struct PendingKafkaRequest(
    KafkaRequest Request,
    int Size,
    byte[] RentedBuffer);
