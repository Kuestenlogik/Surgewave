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
    private readonly Dictionary<string, string> _dynamicConfig = new();

    public AdminServiceImpl(
        int brokerId,
        string host,
        int kafkaPort,
        int grpcPort,
        GetPartitionInfoDelegate? getPartitionInfo = null,
        AlterBrokerConfigDelegate? alterBrokerConfig = null)
    {
        _brokerId = brokerId;
        _host = host;
        _kafkaPort = kafkaPort;
        _grpcPort = grpcPort;
        _startTime = DateTime.UtcNow;
        _getPartitionInfo = getPartitionInfo;
        _alterBrokerConfig = alterBrokerConfig;
    }

    public override Task<GetBrokerInfoResponse> GetBrokerInfo(GetBrokerInfoRequest request, ServerCallContext context)
    {
        return Task.FromResult(new GetBrokerInfoResponse
        {
            BrokerId = _brokerId,
            Host = _host,
            KafkaPort = _kafkaPort,
            GrpcPort = _grpcPort,
            Version = "1.0.0",
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

    public override Task<ElectLeaderResponse> ElectLeader(ElectLeaderRequest request, ServerCallContext context)
    {
        var response = new ElectLeaderResponse();

        foreach (var partition in request.Partitions)
        {
            response.Results.Add(new ElectionResult
            {
                Topic = partition.Topic,
                Partition = partition.Partition,
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            });
        }

        return Task.FromResult(response);
    }

    public override Task<DescribeBrokerConfigResponse> DescribeBrokerConfig(DescribeBrokerConfigRequest request, ServerCallContext context)
    {
        var response = new DescribeBrokerConfigResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        // Static broker config
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

        // Dynamic config (can be changed at runtime)
        foreach (var (key, value) in _dynamicConfig)
        {
            response.Configs.Add(new ConfigEntryDetail
            {
                Key = key,
                Value = value,
                IsDefault = false,
                IsReadOnly = false,
                IsSensitive = false
            });
        }

        return Task.FromResult(response);
    }

    public override Task<AlterBrokerConfigResponse> AlterBrokerConfig(AlterBrokerConfigRequest request, ServerCallContext context)
    {
        // Update dynamic config entries
        foreach (var config in request.Configs)
        {
            if (string.IsNullOrEmpty(config.Value))
            {
                _dynamicConfig.Remove(config.Key);
            }
            else
            {
                _dynamicConfig[config.Key] = config.Value;
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
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        });
    }
}
