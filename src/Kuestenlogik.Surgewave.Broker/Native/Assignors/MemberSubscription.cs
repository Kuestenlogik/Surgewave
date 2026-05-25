namespace Kuestenlogik.Surgewave.Broker.Native.Assignors;

/// <summary>
/// Member subscription metadata
/// </summary>
public record MemberSubscription(string MemberId, List<string> Topics, byte[] UserData);
