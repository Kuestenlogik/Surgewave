using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Quota configuration result DTO.
/// </summary>
public record QuotaConfigDto(
    bool Enabled,
    long ProducerBytesPerSecond,
    long ProducerBurstBytes,
    long ConsumerBytesPerSecond,
    long ConsumerBurstBytes,
    int MaxThrottleTimeMs,
    int ClientInactivityTimeoutMinutes);

/// <summary>
/// Client quota stats result DTO.
/// </summary>
public record ClientQuotaStatsDto(
    string ClientId,
    long TotalProducedBytes,
    long TotalFetchedBytes,
    int ProduceThrottleCount,
    int FetchThrottleCount,
    long AvailableProduceTokens,
    long AvailableFetchTokens,
    long LastActivityTimestamp);

/// <summary>
/// Delegate to get quota configuration.
/// </summary>
public delegate QuotaConfigDto GetQuotaConfigDelegate();

/// <summary>
/// Delegate to set quota configuration.
/// </summary>
public delegate void SetQuotaConfigDelegate(
    bool? enabled,
    long? producerBytesPerSecond,
    long? producerBurstBytes,
    long? consumerBytesPerSecond,
    long? consumerBurstBytes,
    int? maxThrottleTimeMs,
    int? clientInactivityTimeoutMinutes);

/// <summary>
/// Delegate to describe client quotas.
/// </summary>
public delegate ClientQuotaStatsDto? DescribeClientQuotasDelegate(string clientId);

/// <summary>
/// Delegate to list all client quotas.
/// </summary>
public delegate List<ClientQuotaStatsDto> ListClientQuotasDelegate();

/// <summary>
/// gRPC QuotaService implementation.
/// </summary>
public class QuotaServiceImpl : QuotaService.QuotaServiceBase
{
    private readonly GetQuotaConfigDelegate _getQuotaConfig;
    private readonly SetQuotaConfigDelegate _setQuotaConfig;
    private readonly DescribeClientQuotasDelegate _describeClientQuotas;
    private readonly ListClientQuotasDelegate _listClientQuotas;

    public QuotaServiceImpl(
        GetQuotaConfigDelegate getQuotaConfig,
        SetQuotaConfigDelegate setQuotaConfig,
        DescribeClientQuotasDelegate describeClientQuotas,
        ListClientQuotasDelegate listClientQuotas)
    {
        _getQuotaConfig = getQuotaConfig;
        _setQuotaConfig = setQuotaConfig;
        _describeClientQuotas = describeClientQuotas;
        _listClientQuotas = listClientQuotas;
    }

    public override Task<GetQuotaConfigResponse> GetQuotaConfig(GetQuotaConfigRequest request, ServerCallContext context)
    {
        var config = _getQuotaConfig();

        return Task.FromResult(new GetQuotaConfigResponse
        {
            Enabled = config.Enabled,
            ProducerBytesPerSecond = config.ProducerBytesPerSecond,
            ProducerBurstBytes = config.ProducerBurstBytes,
            ConsumerBytesPerSecond = config.ConsumerBytesPerSecond,
            ConsumerBurstBytes = config.ConsumerBurstBytes,
            MaxThrottleTimeMs = config.MaxThrottleTimeMs,
            ClientInactivityTimeoutMinutes = config.ClientInactivityTimeoutMinutes,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        });
    }

    public override Task<SetQuotaConfigResponse> SetQuotaConfig(SetQuotaConfigRequest request, ServerCallContext context)
    {
        _setQuotaConfig(
            request.Enabled,
            request.ProducerBytesPerSecond > 0 ? request.ProducerBytesPerSecond : null,
            request.ProducerBurstBytes > 0 ? request.ProducerBurstBytes : null,
            request.ConsumerBytesPerSecond > 0 ? request.ConsumerBytesPerSecond : null,
            request.ConsumerBurstBytes > 0 ? request.ConsumerBurstBytes : null,
            request.MaxThrottleTimeMs > 0 ? request.MaxThrottleTimeMs : null,
            request.ClientInactivityTimeoutMinutes > 0 ? request.ClientInactivityTimeoutMinutes : null);

        return Task.FromResult(new SetQuotaConfigResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        });
    }

    public override Task<DescribeClientQuotasResponse> DescribeClientQuotas(DescribeClientQuotasRequest request, ServerCallContext context)
    {
        var stats = _describeClientQuotas(request.ClientId);

        if (stats == null)
        {
            return Task.FromResult(new DescribeClientQuotasResponse
            {
                ClientId = request.ClientId,
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = $"Client '{request.ClientId}' not found"
                }
            });
        }

        return Task.FromResult(new DescribeClientQuotasResponse
        {
            ClientId = stats.ClientId,
            TotalProducedBytes = stats.TotalProducedBytes,
            TotalFetchedBytes = stats.TotalFetchedBytes,
            ProduceThrottleCount = stats.ProduceThrottleCount,
            FetchThrottleCount = stats.FetchThrottleCount,
            AvailableProduceTokens = stats.AvailableProduceTokens,
            AvailableFetchTokens = stats.AvailableFetchTokens,
            LastActivity = stats.LastActivityTimestamp,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        });
    }

    public override Task<ListClientQuotasResponse> ListClientQuotas(ListClientQuotasRequest request, ServerCallContext context)
    {
        var allStats = _listClientQuotas();

        var response = new ListClientQuotasResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        foreach (var stats in allStats)
        {
            response.Clients.Add(new ClientQuotaStats
            {
                ClientId = stats.ClientId,
                TotalProducedBytes = stats.TotalProducedBytes,
                TotalFetchedBytes = stats.TotalFetchedBytes,
                ProduceThrottleCount = stats.ProduceThrottleCount,
                FetchThrottleCount = stats.FetchThrottleCount,
                AvailableProduceTokens = stats.AvailableProduceTokens,
                AvailableFetchTokens = stats.AvailableFetchTokens,
                LastActivity = stats.LastActivityTimestamp
            });
        }

        return Task.FromResult(response);
    }
}
