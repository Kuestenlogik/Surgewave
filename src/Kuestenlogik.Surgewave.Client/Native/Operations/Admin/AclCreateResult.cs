using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// Result of creating an ACL.
/// </summary>
public record AclCreateResult(SurgewaveErrorCode ErrorCode, string? ErrorMessage);
