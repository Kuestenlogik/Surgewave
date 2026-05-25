using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// Result of deleting ACLs.
/// </summary>
public record AclDeleteResult(SurgewaveErrorCode ErrorCode, List<AclEntry> DeletedAcls);
