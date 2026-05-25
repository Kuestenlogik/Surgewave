using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol quota operations.
/// </summary>
public sealed class NativeQuotaHandler : INativeRequestHandler
{
    private readonly QuotaManager? _quotaManager;

    public IEnumerable<SurgewaveOpCode> SupportedOpCodes =>
    [
        SurgewaveOpCode.GetQuotaConfig,
        SurgewaveOpCode.SetQuotaConfig,
        SurgewaveOpCode.DescribeClientQuotas,
        SurgewaveOpCode.ListClientQuotas
    ];

    public NativeQuotaHandler(QuotaManager? quotaManager)
    {
        _quotaManager = quotaManager;
    }

    public Task HandleAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        return context.Header.OpCode switch
        {
            SurgewaveOpCode.GetQuotaConfig => HandleGetQuotaConfigAsync(context, cancellationToken),
            SurgewaveOpCode.SetQuotaConfig => HandleSetQuotaConfigAsync(context, payload, cancellationToken),
            SurgewaveOpCode.DescribeClientQuotas => HandleDescribeClientQuotasAsync(context, payload, cancellationToken),
            SurgewaveOpCode.ListClientQuotas => HandleListClientQuotasAsync(context, cancellationToken),
            _ => Task.CompletedTask
        };
    }

    private async Task HandleGetQuotaConfigAsync(NativeRequestContext context, CancellationToken cancellationToken)
    {
        using var writer = new BigEndianWriter();

        var quotaConfig = context.Config.Quotas;
        writer.Write(quotaConfig.Enabled ? (byte)1 : (byte)0);
        writer.Write(quotaConfig.ProducerBytesPerSecond);
        writer.Write(quotaConfig.ProducerBurstBytes);
        writer.Write(quotaConfig.ConsumerBytesPerSecond);
        writer.Write(quotaConfig.ConsumerBurstBytes);
        writer.Write(quotaConfig.MaxThrottleTimeMs);
        writer.Write(quotaConfig.ClientInactivityTimeoutMinutes);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.GetQuotaConfig,
            SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
    }

    private async Task HandleSetQuotaConfigAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (_quotaManager == null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.SetQuotaConfig,
                SurgewaveErrorCode.UnknownError, "Quota manager not available", cancellationToken);
            return;
        }

        var reader = new SurgewavePayloadReader(payload.Span);

        var enabled = reader.ReadUInt8() != 0;
        var producerBytesPerSecond = reader.ReadInt64();
        var producerBurstBytes = reader.ReadInt64();
        var consumerBytesPerSecond = reader.ReadInt64();
        var consumerBurstBytes = reader.ReadInt64();
        var maxThrottleTimeMs = reader.ReadInt32();
        var clientInactivityTimeoutMinutes = reader.ReadInt32();

        _quotaManager.UpdateConfig(
            enabled: enabled,
            producerBytesPerSecond: producerBytesPerSecond,
            producerBurstBytes: producerBurstBytes,
            consumerBytesPerSecond: consumerBytesPerSecond,
            consumerBurstBytes: consumerBurstBytes,
            maxThrottleTimeMs: maxThrottleTimeMs,
            clientInactivityTimeoutMinutes: clientInactivityTimeoutMinutes);

        using var responseWriter = new BigEndianWriter(4);
        responseWriter.Write((ushort)SurgewaveErrorCode.None);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.SetQuotaConfig,
            SurgewaveErrorCode.None, responseWriter.AsMemory(), cancellationToken);
    }

    private async Task HandleDescribeClientQuotasAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (_quotaManager == null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.DescribeClientQuotas,
                SurgewaveErrorCode.UnknownError, "Quota manager not available", cancellationToken);
            return;
        }

        var reader = new SurgewavePayloadReader(payload.Span);
        var clientId = reader.ReadString() ?? string.Empty;

        var stats = _quotaManager.GetClientStats(clientId);

        using var writer = new BigEndianWriter();

        if (stats != null)
        {
            writer.Write((ushort)SurgewaveErrorCode.None);
            writer.WriteString(clientId);
            writer.Write(stats.TotalProducedBytes);
            writer.Write(stats.TotalFetchedBytes);
            writer.Write(stats.ProduceThrottleCount);
            writer.Write(stats.FetchThrottleCount);
            writer.Write(stats.AvailableProduceTokens);
            writer.Write(stats.AvailableFetchTokens);
            writer.Write(stats.LastActivity.Ticks);
        }
        else
        {
            writer.Write((ushort)SurgewaveErrorCode.None);
            writer.WriteString(clientId);
            writer.Write(0L);
            writer.Write(0L);
            writer.Write(0);
            writer.Write(0);
            writer.Write(0L);
            writer.Write(0L);
            writer.Write(DateTime.MinValue.Ticks);
        }

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.DescribeClientQuotas,
            SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
    }

    private async Task HandleListClientQuotasAsync(NativeRequestContext context, CancellationToken cancellationToken)
    {
        if (_quotaManager == null)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.ListClientQuotas,
                SurgewaveErrorCode.UnknownError, "Quota manager not available", cancellationToken);
            return;
        }

        var allStats = _quotaManager.GetAllClientStats().ToList();

        using var writer = new BigEndianWriter();
        writer.Write((ushort)SurgewaveErrorCode.None);
        writer.Write(allStats.Count);

        foreach (var (clientId, stats) in allStats)
        {
            writer.WriteString(clientId);
            writer.Write(stats.TotalProducedBytes);
            writer.Write(stats.TotalFetchedBytes);
            writer.Write(stats.ProduceThrottleCount);
            writer.Write(stats.FetchThrottleCount);
            writer.Write(stats.AvailableProduceTokens);
            writer.Write(stats.AvailableFetchTokens);
            writer.Write(stats.LastActivity.Ticks);
        }

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.ListClientQuotas,
            SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
    }
}
