using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Topics;

/// <summary>
/// Topic information returned by ListTopics. <see cref="Strategy"/>
/// is the broker's per-topic produce-path hint (ADR-014). Clients that
/// do not care about the hint can ignore it — the default
/// <see cref="ProduceStrategy.Replicated"/> behaves exactly like the
/// pre-G21 native client.
/// </summary>
public record TopicInfo(string Name, int PartitionCount, ProduceStrategy Strategy = ProduceStrategy.Replicated);
