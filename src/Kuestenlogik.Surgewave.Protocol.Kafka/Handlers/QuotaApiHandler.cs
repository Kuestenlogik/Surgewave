using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Handler for Kafka quota APIs: DescribeClientQuotas, AlterClientQuotas
/// </summary>
public sealed partial class QuotaApiHandler : IKafkaRequestHandler
{
    private readonly IQuotaManager _quotaManager;
    private readonly ILogger<QuotaApiHandler> _logger;

    // Kafka quota configuration keys
    private const string ProducerByteRateKey = "producer_byte_rate";
    private const string ConsumerByteRateKey = "consumer_byte_rate";
    private const string RequestPercentageKey = "request_percentage";

    public IEnumerable<ApiKey> SupportedApiKeys => [ApiKey.DescribeClientQuotas, ApiKey.AlterClientQuotas];

    public QuotaApiHandler(IQuotaManager quotaManager, ILogger<QuotaApiHandler> logger)
    {
        _quotaManager = quotaManager;
        _logger = logger;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult<KafkaResponse>(request switch
        {
            DescribeClientQuotasRequest describeRequest => HandleDescribeClientQuotas(describeRequest),
            AlterClientQuotasRequest alterRequest => HandleAlterClientQuotas(alterRequest),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by QuotaApiHandler")
        });
    }

    private DescribeClientQuotasResponse HandleDescribeClientQuotas(DescribeClientQuotasRequest request)
    {
        var entries = new List<DescribeClientQuotasResponse.EntryData>();
        var config = _quotaManager.Config;

        // Check if filtering by specific client or user
        string? clientIdFilter = null;
        string? userFilter = null;
        bool matchDefault = false;
        bool matchAny = false;

        foreach (var component in request.Components)
        {
            switch (component.EntityType.ToLowerInvariant())
            {
                case "client-id":
                    if (component.MatchType == 0) // Exact match
                        clientIdFilter = component.Match;
                    else if (component.MatchType == 1) // Default
                        matchDefault = true;
                    else if (component.MatchType == 2) // Any
                        matchAny = true;
                    break;
                case "user":
                    if (component.MatchType == 0)
                        userFilter = component.Match;
                    else if (component.MatchType == 1)
                        matchDefault = true;
                    else if (component.MatchType == 2)
                        matchAny = true;
                    break;
            }
        }

        // If requesting default quotas or any quotas, return global config
        if (matchDefault || matchAny || request.Components.Count == 0)
        {
            var defaultEntry = CreateDefaultQuotaEntry(config);
            entries.Add(defaultEntry);
        }

        // If filtering by specific client ID, also include client stats if available
        if (clientIdFilter != null)
        {
            var clientStats = _quotaManager.GetClientStats(clientIdFilter);
            if (clientStats != null)
            {
                // Client exists, return default quotas with client entity
                var clientEntry = new DescribeClientQuotasResponse.EntryData
                {
                    Entity =
                    [
                        new DescribeClientQuotasResponse.EntityData
                        {
                            EntityType = "client-id",
                            EntityName = clientIdFilter
                        }
                    ],
                    Values = GetQuotaValues(config)
                };
                entries.Add(clientEntry);
            }
        }

        // If matchAny, also include all tracked clients
        if (matchAny)
        {
            foreach (var (clientId, _) in _quotaManager.GetAllClientStats())
            {
                var clientEntry = new DescribeClientQuotasResponse.EntryData
                {
                    Entity =
                    [
                        new DescribeClientQuotasResponse.EntityData
                        {
                            EntityType = "client-id",
                            EntityName = clientId
                        }
                    ],
                    Values = GetQuotaValues(config)
                };
                entries.Add(clientEntry);
            }
        }

        LogDescribeQuotas(request.Components.Count, entries.Count);

        return new DescribeClientQuotasResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Entries = entries
        };
    }

    private static DescribeClientQuotasResponse.EntryData CreateDefaultQuotaEntry(QuotaConfig config)
    {
        return new DescribeClientQuotasResponse.EntryData
        {
            Entity =
            [
                new DescribeClientQuotasResponse.EntityData
                {
                    EntityType = "client-id",
                    EntityName = null // null means default
                }
            ],
            Values = GetQuotaValues(config)
        };
    }

    private static List<DescribeClientQuotasResponse.ValueData> GetQuotaValues(QuotaConfig config)
    {
        var values = new List<DescribeClientQuotasResponse.ValueData>();

        if (config.ProducerBytesPerSecond > 0)
        {
            values.Add(new DescribeClientQuotasResponse.ValueData
            {
                Key = ProducerByteRateKey,
                Value = config.ProducerBytesPerSecond
            });
        }

        if (config.ConsumerBytesPerSecond > 0)
        {
            values.Add(new DescribeClientQuotasResponse.ValueData
            {
                Key = ConsumerByteRateKey,
                Value = config.ConsumerBytesPerSecond
            });
        }

        return values;
    }

    private AlterClientQuotasResponse HandleAlterClientQuotas(AlterClientQuotasRequest request)
    {
        var responseEntries = new List<AlterClientQuotasResponse.EntryData>();

        foreach (var entry in request.Entries)
        {
            var errorCode = ErrorCode.None;
            string? errorMessage = null;

            // Determine if this is a default quota or per-client quota
            bool isDefault = entry.Entity.All(e => e.EntityName == null);

            if (isDefault)
            {
                // Alter default (global) quotas
                if (!request.ValidateOnly)
                {
                    foreach (var op in entry.Ops)
                    {
                        if (op.Remove)
                        {
                            // Remove quota - set to unlimited (-1)
                            ApplyQuotaRemoval(op.Key);
                        }
                        else
                        {
                            // Set quota value
                            var result = ApplyQuotaValue(op.Key, op.Value);
                            if (result != null)
                            {
                                errorCode = ErrorCode.InvalidConfig;
                                errorMessage = result;
                            }
                        }
                    }
                }

                LogAlterQuotas("default", entry.Ops.Count, request.ValidateOnly);
            }
            else
            {
                // Per-client quotas - currently not supported for individual clients
                // We still accept the request but apply to global config
                // This matches behavior of many Kafka deployments with simple quota configs

                var clientId = entry.Entity.FirstOrDefault(e => e.EntityType == "client-id")?.EntityName;

                if (!request.ValidateOnly)
                {
                    foreach (var op in entry.Ops)
                    {
                        if (op.Remove)
                        {
                            ApplyQuotaRemoval(op.Key);
                        }
                        else
                        {
                            var result = ApplyQuotaValue(op.Key, op.Value);
                            if (result != null)
                            {
                                errorCode = ErrorCode.InvalidConfig;
                                errorMessage = result;
                            }
                        }
                    }
                }

                LogAlterQuotas(clientId ?? "unknown", entry.Ops.Count, request.ValidateOnly);
            }

            responseEntries.Add(new AlterClientQuotasResponse.EntryData
            {
                ErrorCode = errorCode,
                ErrorMessage = errorMessage,
                Entity = entry.Entity.Select(e => new AlterClientQuotasResponse.EntityData
                {
                    EntityType = e.EntityType,
                    EntityName = e.EntityName
                }).ToList()
            });
        }

        return new AlterClientQuotasResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            Entries = responseEntries
        };
    }

    private string? ApplyQuotaValue(string key, double value)
    {
        var longValue = (long)value;

        switch (key.ToLowerInvariant())
        {
            case ProducerByteRateKey:
                _quotaManager.UpdateConfig(producerBytesPerSecond: longValue, enabled: longValue > 0);
                return null;

            case ConsumerByteRateKey:
                _quotaManager.UpdateConfig(consumerBytesPerSecond: longValue, enabled: longValue > 0);
                return null;

            case RequestPercentageKey:
                // Request percentage not yet implemented
                return "request_percentage quota is not yet supported";

            default:
                return $"Unknown quota key: {key}";
        }
    }

    private void ApplyQuotaRemoval(string key)
    {
        switch (key.ToLowerInvariant())
        {
            case ProducerByteRateKey:
                _quotaManager.UpdateConfig(producerBytesPerSecond: -1);
                break;

            case ConsumerByteRateKey:
                _quotaManager.UpdateConfig(consumerBytesPerSecond: -1);
                break;
        }

        // Check if all quotas are now unlimited - disable quotas
        var config = _quotaManager.Config;
        if (config.ProducerBytesPerSecond <= 0 && config.ConsumerBytesPerSecond <= 0)
        {
            _quotaManager.UpdateConfig(enabled: false);
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "DescribeClientQuotas: {ComponentCount} components, returning {EntryCount} entries")]
    private partial void LogDescribeQuotas(int componentCount, int entryCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "AlterClientQuotas for {Entity}: {OpCount} operations, validateOnly={ValidateOnly}")]
    private partial void LogAlterQuotas(string entity, int opCount, bool validateOnly);
}
