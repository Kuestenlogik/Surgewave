namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Error codes exchanged through the ACL delegates (<see cref="CreateAclsDelegate"/>,
/// <see cref="DeleteAclsDelegate"/>). The values are Kafka-wire-compatible so
/// Kafka tooling sees familiar semantics, but this type deliberately lives in
/// the gRPC server layer: the broker host must not need a Protocol.Kafka
/// reference to report them.
/// </summary>
public static class AclErrorCodes
{
    /// <summary>No error.</summary>
    public const int None = 0;

    /// <summary>ACL authorization is not enabled on this broker (Kafka wire code 54, SECURITY_DISABLED).</summary>
    public const int SecurityDisabled = 54;

    /// <summary>Unspecified server-side error (Kafka wire code -1, UNKNOWN_SERVER_ERROR).</summary>
    public const int Unknown = -1;
}
