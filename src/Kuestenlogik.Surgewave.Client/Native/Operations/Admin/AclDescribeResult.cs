using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// Result of describing ACLs.
/// </summary>
public record AclDescribeResult(SurgewaveErrorCode ErrorCode, List<AclEntry> Acls);
