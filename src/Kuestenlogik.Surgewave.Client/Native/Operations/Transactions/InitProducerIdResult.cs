using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;

/// <summary>
/// Result of initializing a producer ID.
/// </summary>
public record InitProducerIdResult(SurgewaveErrorCode ErrorCode, long ProducerId, short ProducerEpoch);
