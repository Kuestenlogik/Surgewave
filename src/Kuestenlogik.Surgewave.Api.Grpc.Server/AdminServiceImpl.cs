using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Partition info result from LogManager.
/// </summary>
public record PartitionInfoDto(
    int PartitionId,
    int Leader,
    List<int> Replicas,
    List<int> Isr,
    long HighWatermark,
    long LogStartOffset);

/// <summary>
/// Delegate to get partition info.
/// </summary>
public delegate PartitionInfoDto? GetPartitionInfoDelegate(string topic, int partition);

/// <summary>
/// Delegate to alter broker config.
/// </summary>
public delegate bool AlterBrokerConfigDelegate(Dictionary<string, string> configs);

/// <summary>
/// One broker config entry for DescribeBrokerConfig.
/// </summary>
public record BrokerConfigEntryDto(
    string Key,
    string Value,
    bool IsDefault,
    bool IsReadOnly,
    bool IsSensitive);

/// <summary>
/// Delegate to enumerate the broker's effective config (static + dynamic overrides).
/// </summary>
public delegate List<BrokerConfigEntryDto> DescribeBrokerConfigDelegate();

/// <summary>
/// Delegate to set a single dynamic broker config entry.
/// Returns null on success, otherwise a human-readable error.
/// </summary>
public delegate string? SetBrokerConfigDelegate(string key, string? value);

/// <summary>
/// Delegate to trigger a leader election for one partition.
/// Returns true when a new leader was elected.
/// </summary>
public delegate Task<bool> ElectLeaderDelegate(string topic, int partition);

/// <summary>
/// gRPC AdminService implementation
/// </summary>
public class AdminServiceImpl : AdminService.AdminServiceBase
{
    private readonly int _brokerId;
    private readonly string _host;
    private readonly int _kafkaPort;
    private readonly int _grpcPort;
    private readonly DateTime _startTime;
    private readonly GetPartitionInfoDelegate? _getPartitionInfo;
    private readonly AlterBrokerConfigDelegate? _alterBrokerConfig;
    private readonly DescribeBrokerConfigDelegate? _describeBrokerConfig;
    private readonly SetBrokerConfigDelegate? _setBrokerConfig;
    private readonly ElectLeaderDelegate? _electLeader;

    public AdminServiceImpl(
        int brokerId,
        string host,
        int kafkaPort,
        int grpcPort,
        GetPartitionInfoDelegate? getPartitionInfo = null,
        AlterBrokerConfigDelegate? alterBrokerConfig = null,
        DescribeBrokerConfigDelegate? describeBrokerConfig = null,
        SetBrokerConfigDelegate? setBrokerConfig = null,
        ElectLeaderDelegate? electLeader = null)
    {
        _brokerId = brokerId;
        _host = host;
        _kafkaPort = kafkaPort;
        _grpcPort = grpcPort;
        _startTime = DateTime.UtcNow;
        _getPartitionInfo = getPartitionInfo;
        _alterBrokerConfig = alterBrokerConfig;
        _describeBrokerConfig = describeBrokerConfig;
        _setBrokerConfig = setBrokerConfig;
        _electLeader = electLeader;
    }

    private static readonly string s_brokerVersion =
        typeof(AdminServiceImpl).Assembly
            .GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
            is [System.Reflection.AssemblyInformationalVersionAttribute attr, ..]
            ? attr.InformationalVersion.Split('+')[0]
            : typeof(AdminServiceImpl).Assembly.GetName().Version?.ToString(3) ?? "unknown";

    public override Task<GetBrokerInfoResponse> GetBrokerInfo(GetBrokerInfoRequest request, ServerCallContext context)
    {
        return Task.FromResult(new GetBrokerInfoResponse
        {
            BrokerId = _brokerId,
            Host = _host,
            KafkaPort = _kafkaPort,
            GrpcPort = _grpcPort,
            Version = s_brokerVersion,
            StartTime = new DateTimeOffset(_startTime).ToUnixTimeSeconds()
        });
    }

    public override Task<GetPartitionInfoResponse> GetPartitionInfo(GetPartitionInfoRequest request, ServerCallContext context)
    {
        var info = _getPartitionInfo?.Invoke(request.Topic, request.Partition);

        if (info == null)
        {
            return Task.FromResult(new GetPartitionInfoResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.UnknownTopicOrPartition,
                    ErrorMessage = $"Topic '{request.Topic}' partition {request.Partition} not found"
                }
            });
        }

        var partitionInfo = new PartitionInfo
        {
            PartitionId = info.PartitionId,
            Leader = info.Leader,
            HighWatermark = info.HighWatermark,
            LogStartOffset = info.LogStartOffset
        };
        partitionInfo.Replicas.AddRange(info.Replicas);
        partitionInfo.Isr.AddRange(info.Isr);

        return Task.FromResult(new GetPartitionInfoResponse
        {
            PartitionInfo = partitionInfo,
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        });
    }

    public override async Task<ElectLeaderResponse> ElectLeader(ElectLeaderRequest request, ServerCallContext context)
    {
        var response = new ElectLeaderResponse();

        foreach (var partition in request.Partitions)
        {
            ResponseStatus status;
            if (_electLeader is null)
            {
                status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = "Leader election is not available on this broker (no cluster controller wired)"
                };
            }
            else
            {
                try
                {
                    var elected = await _electLeader(partition.Topic, partition.Partition);
                    status = elected
                        ? new ResponseStatus { ErrorCode = ErrorCode.None }
                        : new ResponseStatus
                        {
                            ErrorCode = ErrorCode.Unknown,
                            ErrorMessage = "Election failed (no eligible replica or not controller)"
                        };
                }
                catch (Exception ex)
                {
                    status = new ResponseStatus { ErrorCode = ErrorCode.Unknown, ErrorMessage = ex.Message };
                }
            }

            response.Results.Add(new ElectionResult
            {
                Topic = partition.Topic,
                Partition = partition.Partition,
                Status = status
            });
        }

        return response;
    }

    public override Task<DescribeBrokerConfigResponse> DescribeBrokerConfig(DescribeBrokerConfigRequest request, ServerCallContext context)
    {
        var response = new DescribeBrokerConfigResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        // Identity entries that only this layer knows.
        response.Configs.Add(new ConfigEntryDetail
        {
            Key = "broker.id",
            Value = _brokerId.ToString(),
            IsDefault = false,
            IsReadOnly = true,
            IsSensitive = false
        });

        response.Configs.Add(new ConfigEntryDetail
        {
            Key = "listeners",
            Value = $"PLAINTEXT://{_host}:{_kafkaPort}",
            IsDefault = false,
            IsReadOnly = true,
            IsSensitive = false
        });

        response.Configs.Add(new ConfigEntryDetail
        {
            Key = "grpc.port",
            Value = _grpcPort.ToString(),
            IsDefault = false,
            IsReadOnly = true,
            IsSensitive = false
        });

        // Effective broker config (static values + dynamic overrides) from
        // DynamicBrokerConfig — previously this returned an in-memory fake dict.
        if (_describeBrokerConfig is not null)
        {
            var requestedKeys = request.ConfigKeys.Count > 0
                ? new HashSet<string>(request.ConfigKeys, StringComparer.OrdinalIgnoreCase)
                : null;

            foreach (var entry in _describeBrokerConfig())
            {
                if (requestedKeys is not null && !requestedKeys.Contains(entry.Key))
                    continue;

                response.Configs.Add(new ConfigEntryDetail
                {
                    Key = entry.Key,
                    Value = entry.Value,
                    IsDefault = entry.IsDefault,
                    IsReadOnly = entry.IsReadOnly,
                    IsSensitive = entry.IsSensitive
                });
            }
        }

        return Task.FromResult(response);
    }

    public override Task<AlterBrokerConfigResponse> AlterBrokerConfig(AlterBrokerConfigRequest request, ServerCallContext context)
    {
        if (_setBrokerConfig is null)
        {
            return Task.FromResult(new AlterBrokerConfigResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = "Dynamic broker config is not available on this broker"
                }
            });
        }

        var errors = new List<string>();
        foreach (var config in request.Configs)
        {
            var error = _setBrokerConfig(config.Key, string.IsNullOrEmpty(config.Value) ? null : config.Value);
            if (error is not null)
            {
                errors.Add($"{config.Key}: {error}");
            }
        }

        // Notify external handler if provided
        if (_alterBrokerConfig != null && request.Configs.Count > 0)
        {
            var configDict = request.Configs.ToDictionary(c => c.Key, c => c.Value);
            _alterBrokerConfig(configDict);
        }

        return Task.FromResult(new AlterBrokerConfigResponse
        {
            Status = errors.Count == 0
                ? new ResponseStatus { ErrorCode = ErrorCode.None }
                : new ResponseStatus { ErrorCode = ErrorCode.Unknown, ErrorMessage = string.Join("; ", errors) }
        });
    }
}
