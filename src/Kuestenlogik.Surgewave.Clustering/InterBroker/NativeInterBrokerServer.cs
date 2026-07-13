using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Protocol.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc4 — the native SRWV inter-broker receive server. Decodes a native frame
/// (<see cref="InterBrokerFrameCodec"/>), dispatches by opcode to the neutral
/// <see cref="INativeInterBrokerService"/>, and writes the response frame. It shares the broker's
/// ReplicationPort with the Family-B replication/Raft traffic: the <see cref="ReplicationServer"/>
/// hands off any frame whose opcode is in the native band (see <see cref="IsNativeOpcode"/>), so the
/// hot fetch/Raft path is unchanged and old peers — which never emit native opcodes — are unaffected.
/// <para>
/// Only <see cref="SurgewaveOpCode.InterBrokerUpdateMetadata"/> is wired to real logic in this
/// increment; every other native opcode is answered with an <see cref="SurgewaveOpCode.Error"/> frame
/// carrying <see cref="ClusterRpcStatus.UnsupportedVersion"/> until its handler lands (Inc5–7).
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
            case SurgewaveOpCode.InterBrokerUpdateMetadata:
                return await HandleUpdateMetadataAsync(payload, ct).ConfigureAwait(false);

            default:
                LogUnsupportedOpcode(opcode);
                return ErrorFrame(ClusterRpcStatus.UnsupportedVersion);
        }
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

    private async ValueTask<byte[]> HandleUpdateMetadataAsync(ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        if (_service is null)
        {
            // No service wired (e.g. this broker is not acting as the applier) — signal not-controller.
            return ErrorFrame(ClusterRpcStatus.NotController);
        }

        PartitionStatesPayload request;
        try
        {
            var reader = new SurgewavePayloadReader(payload.Span);
            request = PartitionStatesPayload.Read(ref reader);
        }
        catch (Exception ex)
        {
            LogDecodeError(SurgewaveOpCode.InterBrokerUpdateMetadata, ex);
            return ErrorFrame(ClusterRpcStatus.Unknown);
        }

        var status = await _service.ApplyUpdateMetadataAsync(request, ct).ConfigureAwait(false);
        return InterBrokerFrameCodec.EncodeFrame(
            SurgewaveOpCode.InterBrokerUpdateMetadata, new InterBrokerStatusPayload(status));
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
