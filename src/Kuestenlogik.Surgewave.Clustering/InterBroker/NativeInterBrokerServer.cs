using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc4/Inc5 — the native SRWV inter-broker receive server. Decodes a native frame
/// (<see cref="InterBrokerFrameCodec"/>), dispatches by opcode to the neutral
/// <see cref="INativeInterBrokerService"/>, and writes the response frame. It shares the broker's
/// ReplicationPort with the Family-B replication/Raft traffic: the <see cref="ReplicationServer"/>
/// hands off any frame whose opcode is in the native band (see <see cref="IsNativeOpcode"/>), so the
/// hot fetch/Raft path is unchanged and old peers — which never emit native opcodes — are unaffected.
/// <para>
/// The controller-plane ops (LeaderAndIsr / UpdateMetadata / StopReplica / AlterPartition) are wired
/// to real logic; the remaining native opcodes (registration/heartbeat, txn markers) are answered
/// with an <see cref="SurgewaveOpCode.Error"/> frame carrying
/// <see cref="ClusterRpcStatus.UnsupportedVersion"/> until their handlers land (Inc6/Inc7).
/// </para>
/// </summary>
public sealed partial class NativeInterBrokerServer
{
    /// <summary>
    /// First opcode of the native inter-broker/Raft SRWV band (0x15xx/0x16xx). Family-B replication
    /// api keys (Fetch=1, Heartbeat=100, Raft=101/102/104) are all below this, so a single threshold
    /// separates native frames from the legacy replication frames on the shared port.
    /// </summary>
    public const ushort OpcodeBandStart = 0x1500;

    private readonly ILogger<NativeInterBrokerServer> _logger;
    private readonly INativeInterBrokerService? _service;

    public NativeInterBrokerServer(ILogger<NativeInterBrokerServer> logger, INativeInterBrokerService? service = null)
    {
        _logger = logger;
        _service = service;
    }

    /// <summary>True if <paramref name="rawOpcode"/> falls in the native inter-broker/Raft band.</summary>
    public static bool IsNativeOpcode(ushort rawOpcode) => rawOpcode >= OpcodeBandStart;

    /// <summary>
    /// Decode, dispatch and encode a single native inter-broker request. Returns the full response
    /// frame (<c>[size][opcode][payload]</c>). Pure over the injected service — no I/O — so it is
    /// directly unit-testable.
    /// </summary>
    public async ValueTask<byte[]> ProcessAsync(SurgewaveOpCode opcode, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        switch (opcode)
        {
            case SurgewaveOpCode.InterBrokerLeaderAndIsr:
                return await HandleAsync<PartitionStatesPayload>(
                    opcode, payload, static (s, p, ct) => s.ApplyLeaderAndIsrAsync(p, ct), ct).ConfigureAwait(false);

            case SurgewaveOpCode.InterBrokerUpdateMetadata:
                return await HandleAsync<PartitionStatesPayload>(
                    opcode, payload, static (s, p, ct) => s.ApplyUpdateMetadataAsync(p, ct), ct).ConfigureAwait(false);

            case SurgewaveOpCode.InterBrokerStopReplica:
                return await HandleAsync<StopReplicaPayload>(
                    opcode, payload, static (s, p, ct) => s.ApplyStopReplicaAsync(p, ct), ct).ConfigureAwait(false);

            case SurgewaveOpCode.InterBrokerAlterPartition:
                return await HandleAsync<AlterPartitionPayload>(
                    opcode, payload, static (s, p, ct) => s.ApplyIsrChangeAsync(p, ct), ct).ConfigureAwait(false);

            case SurgewaveOpCode.InterBrokerRegistration:
                return await HandleRegistrationAsync(payload, ct).ConfigureAwait(false);

            case SurgewaveOpCode.InterBrokerHeartbeat:
                return await HandleHeartbeatAsync(payload, ct).ConfigureAwait(false);

            default:
                LogUnsupportedOpcode(opcode);
                return ErrorFrame(ClusterRpcStatus.UnsupportedVersion);
        }
    }

    // Registration/heartbeat (#60 Inc6b) return richer response payloads than the bare status frame,
    // so they can't ride the generic status-returning HandleAsync path.
    private async ValueTask<byte[]> HandleRegistrationAsync(ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        if (_service is null)
            return InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.InterBrokerRegistration,
                new BrokerRegistrationResponsePayload(new BrokerRegistrationOutcome(ClusterRpcStatus.NotController, -1)));

        BrokerRegistrationRequestPayload request;
        try
        {
            var reader = new SurgewavePayloadReader(payload.Span);
            request = BrokerRegistrationRequestPayload.Read(ref reader);
        }
        catch (Exception ex)
        {
            LogDecodeError(SurgewaveOpCode.InterBrokerRegistration, ex);
            return ErrorFrame(ClusterRpcStatus.Unknown);
        }

        var outcome = await _service.RegisterBrokerAsync(request.Input, ct).ConfigureAwait(false);
        return InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.InterBrokerRegistration, new BrokerRegistrationResponsePayload(outcome));
    }

    private async ValueTask<byte[]> HandleHeartbeatAsync(ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        if (_service is null)
            return InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.InterBrokerHeartbeat,
                new BrokerHeartbeatResponsePayload(new BrokerHeartbeatOutcome(ClusterRpcStatus.NotController, true, false, false)));

        BrokerHeartbeatRequestPayload request;
        try
        {
            var reader = new SurgewavePayloadReader(payload.Span);
            request = BrokerHeartbeatRequestPayload.Read(ref reader);
        }
        catch (Exception ex)
        {
            LogDecodeError(SurgewaveOpCode.InterBrokerHeartbeat, ex);
            return ErrorFrame(ClusterRpcStatus.Unknown);
        }

        var outcome = await _service.HeartbeatAsync(request.Input, ct).ConfigureAwait(false);
        return InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.InterBrokerHeartbeat, new BrokerHeartbeatResponsePayload(outcome));
    }

    /// <summary>
    /// Handle one already-framed body (<c>[opcode][payload]</c>, size prefix already consumed by the
    /// caller) and write the response frame to <paramref name="stream"/>. Used by the
    /// <see cref="ReplicationServer"/> multiplex, which has read the length prefix itself.
    /// </summary>
    public async ValueTask HandleBodyAsync(Stream stream, ReadOnlyMemory<byte> body, CancellationToken ct)
    {
        var opcode = InterBrokerFrameCodec.PeekOpcode(body.Span);
        var response = await ProcessAsync(opcode, body[2..], ct).ConfigureAwait(false);
        await WriteFrameAsync(stream, response, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Read one native frame from <paramref name="stream"/>, process it, and write the response.
    /// Returns <c>false</c> on end-of-stream so a caller loop stops on a closed connection. Used by the
    /// standalone/loopback path; the shared-port path uses <see cref="HandleBodyAsync"/>.
    /// </summary>
    public async ValueTask<bool> HandleSingleAsync(Stream stream, CancellationToken ct)
    {
        var frame = await InterBrokerFrameCodec.ReadFrameAsync(stream, ct).ConfigureAwait(false);
        if (frame is null)
            return false;

        var response = await ProcessAsync(frame.Value.Opcode, frame.Value.Payload, ct).ConfigureAwait(false);
        await WriteFrameAsync(stream, response, ct).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// The shared decode → apply → encode shape of every wired op: deserialize
    /// <typeparamref name="TPayload"/>, route it to the service, echo the request opcode with the
    /// resulting status — degrading to an <see cref="SurgewaveOpCode.Error"/> frame when no service is
    /// wired (this broker is not an applier) or the payload fails to decode.
    /// </summary>
    private async ValueTask<byte[]> HandleAsync<TPayload>(
        SurgewaveOpCode opcode,
        ReadOnlyMemory<byte> payload,
        Func<INativeInterBrokerService, TPayload, CancellationToken, ValueTask<ClusterRpcStatus>> apply,
        CancellationToken ct)
        where TPayload : ISerializablePayload<TPayload>
    {
        if (_service is null)
            return ErrorFrame(ClusterRpcStatus.NotController);

        TPayload request;
        try
        {
            var reader = new SurgewavePayloadReader(payload.Span);
            request = TPayload.Read(ref reader);
        }
        catch (Exception ex)
        {
            LogDecodeError(opcode, ex);
            return ErrorFrame(ClusterRpcStatus.Unknown);
        }

        var status = await apply(_service, request, ct).ConfigureAwait(false);
        return InterBrokerFrameCodec.EncodeFrame(opcode, new InterBrokerStatusPayload(status));
    }

    private static byte[] ErrorFrame(ClusterRpcStatus status)
        => InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.Error, new InterBrokerStatusPayload(status));

    private static async ValueTask WriteFrameAsync(Stream stream, byte[] frame, CancellationToken ct)
    {
        await stream.WriteAsync(frame, ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Native inter-broker opcode {Opcode} not supported yet, replying with UnsupportedVersion")]
    private partial void LogUnsupportedOpcode(SurgewaveOpCode opcode);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to decode native inter-broker payload for {Opcode}")]
    private partial void LogDecodeError(SurgewaveOpCode opcode, Exception ex);
}
