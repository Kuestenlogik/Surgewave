using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;

/// <summary>
/// Handler for Raft cluster membership APIs: BrokerRegistration, BrokerHeartbeat.
/// This handler runs on the controller and manages broker registrations.
/// </summary>
public sealed class ClusterMembershipHandler : IKafkaRequestHandler
{
    private readonly ClusterIdManager _clusterIdManager;
    private readonly ClusterState _clusterState;
    private readonly ILogger<ClusterMembershipHandler> _logger;

    /// <summary>
    /// Tracks registered brokers with their metadata.
    /// Key: BrokerId, Value: Registration info
    /// </summary>
    private readonly ConcurrentDictionary<int, BrokerRegistrationInfo> _registrations = new();

    /// <summary>
    /// Next broker epoch to assign.
    /// </summary>
    private long _nextBrokerEpoch = 1;
    private readonly Lock _epochLock = new();

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.BrokerRegistration,
        ApiKey.BrokerHeartbeat
    ];

    public ClusterMembershipHandler(
        ClusterIdManager clusterIdManager,
        ClusterState clusterState,
        ILogger<ClusterMembershipHandler> logger)
    {
        _clusterIdManager = clusterIdManager;
        _clusterState = clusterState;
        _logger = logger;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return request switch
        {
            BrokerRegistrationRequest registrationRequest => Task.FromResult<KafkaResponse>(HandleBrokerRegistration(registrationRequest)),
            BrokerHeartbeatRequest heartbeatRequest => Task.FromResult<KafkaResponse>(HandleBrokerHeartbeat(heartbeatRequest)),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by ClusterMembershipHandler")
        };
    }

    private BrokerRegistrationResponse HandleBrokerRegistration(BrokerRegistrationRequest request)
    {
        _logger.LogInformation(
            "Broker registration request from BrokerId={BrokerId} ClusterId={ClusterId} IncarnationId={IncarnationId}",
            request.BrokerId, request.ClusterId, request.IncarnationId);

        // Validate cluster ID matches
        if (!_clusterIdManager.ValidateClusterId(request.ClusterId))
        {
            _logger.LogWarning(
                "Rejecting broker registration: cluster ID mismatch. Expected={Expected}, Got={Got}",
                _clusterIdManager.GetClusterId(), request.ClusterId);

            return new BrokerRegistrationResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.ClusterAuthorizationFailed,
                BrokerEpoch = -1
            };
        }

        // Check if this is a new registration or re-registration
        long brokerEpoch;
        if (_registrations.TryGetValue(request.BrokerId, out var existingReg))
        {
            // Same incarnation ID = broker reconnecting without restart
            if (existingReg.IncarnationId == request.IncarnationId)
            {
                brokerEpoch = existingReg.BrokerEpoch;
                _logger.LogDebug(
                    "Broker {BrokerId} reconnecting with same incarnation, keeping epoch {Epoch}",
                    request.BrokerId, brokerEpoch);
            }
            else
            {
                // New incarnation = broker restarted, assign new epoch
                brokerEpoch = AssignNewEpoch();
                _logger.LogInformation(
                    "Broker {BrokerId} restarted (new incarnation), assigning new epoch {Epoch}",
                    request.BrokerId, brokerEpoch);
            }
        }
        else
        {
            // First registration for this broker ID
            brokerEpoch = AssignNewEpoch();
            _logger.LogInformation(
                "New broker {BrokerId} registration, assigning epoch {Epoch}",
                request.BrokerId, brokerEpoch);
        }

        // Extract primary listener for ClusterState
        var primaryListener = request.Listeners.FirstOrDefault();
        var host = primaryListener?.Host ?? "localhost";
        var port = primaryListener?.Port ?? 9092;
        var rack = request.Rack;

        // Create registration info
        var registration = new BrokerRegistrationInfo
        {
            BrokerId = request.BrokerId,
            ClusterId = request.ClusterId,
            IncarnationId = request.IncarnationId,
            BrokerEpoch = brokerEpoch,
            Listeners = request.Listeners.Select(l => new ListenerInfo
            {
                Name = l.Name,
                Host = l.Host,
                Port = l.Port,
                SecurityProtocol = l.SecurityProtocol
            }).ToList(),
            Features = request.Features.Select(f => new FeatureInfo
            {
                Name = f.Name,
                MinVersion = f.MinSupportedVersion,
                MaxVersion = f.MaxSupportedVersion
            }).ToList(),
            Rack = rack,
            LogDirs = request.LogDirs,
            IsFenced = true, // Start fenced until caught up
            RegisteredAt = DateTimeOffset.UtcNow,
            LastHeartbeat = DateTimeOffset.UtcNow,
            CurrentMetadataOffset = -1
        };

        _registrations[request.BrokerId] = registration;

        // Update cluster state
        var brokerNode = new BrokerNode
        {
            BrokerId = request.BrokerId,
            Host = host,
            Port = (int)port,
            Rack = rack
        };
        _clusterState.AddBroker(brokerNode);

        _logger.LogInformation(
            "Broker {BrokerId} registered successfully at {Host}:{Port} (fenced=true, epoch={Epoch})",
            request.BrokerId, host, port, brokerEpoch);

        return new BrokerRegistrationResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            BrokerEpoch = brokerEpoch
        };
    }

    private BrokerHeartbeatResponse HandleBrokerHeartbeat(BrokerHeartbeatRequest request)
    {
        _logger.LogDebug(
            "Heartbeat from BrokerId={BrokerId} Epoch={BrokerEpoch} MetadataOffset={MetadataOffset} WantFence={WantFence} WantShutdown={WantShutdown}",
            request.BrokerId, request.BrokerEpoch, request.CurrentMetadataOffset, request.WantFence, request.WantShutDown);

        // Find broker registration
        if (!_registrations.TryGetValue(request.BrokerId, out var registration))
        {
            _logger.LogWarning("Heartbeat from unregistered broker {BrokerId}", request.BrokerId);
            return new BrokerHeartbeatResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.BrokerNotAvailable,
                IsFenced = true,
                IsCaughtUp = false,
                ShouldShutDown = false
            };
        }

        // Validate epoch
        if (request.BrokerEpoch != registration.BrokerEpoch)
        {
            _logger.LogWarning(
                "Heartbeat from broker {BrokerId} with stale epoch {RequestEpoch} (expected {ExpectedEpoch})",
                request.BrokerId, request.BrokerEpoch, registration.BrokerEpoch);
            return new BrokerHeartbeatResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.StaleBrokerEpoch,
                IsFenced = true,
                IsCaughtUp = false,
                ShouldShutDown = false
            };
        }

        // Update heartbeat tracking
        registration.LastHeartbeat = DateTimeOffset.UtcNow;
        registration.CurrentMetadataOffset = request.CurrentMetadataOffset;

        // Handle offline log dirs
        if (request.OfflineLogDirs.Count > 0)
        {
            _logger.LogWarning(
                "Broker {BrokerId} reported {Count} offline log directories",
                request.BrokerId, request.OfflineLogDirs.Count);
            registration.OfflineLogDirs = request.OfflineLogDirs;
        }

        // Determine broker state
        var isCaughtUp = IsBrokerCaughtUp(registration);
        var isFenced = registration.IsFenced;
        var shouldShutDown = false;

        // Handle fence/unfence requests
        if (request.WantFence && !isFenced)
        {
            _logger.LogInformation("Broker {BrokerId} requested to be fenced", request.BrokerId);
            registration.IsFenced = true;
            isFenced = true;
        }
        else if (!request.WantFence && isFenced && isCaughtUp)
        {
            // Unfence broker if it's caught up
            _logger.LogInformation(
                "Unfencing broker {BrokerId} (caught up at metadata offset {Offset})",
                request.BrokerId, request.CurrentMetadataOffset);
            registration.IsFenced = false;
            isFenced = false;
        }

        // Handle shutdown request
        if (request.WantShutDown)
        {
            _logger.LogInformation("Broker {BrokerId} requested controlled shutdown", request.BrokerId);
            registration.InControlledShutdown = true;

            // For now, allow immediate shutdown (in a full implementation,
            // we would first move partitions away from this broker)
            shouldShutDown = true;
        }

        return new BrokerHeartbeatResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            IsCaughtUp = isCaughtUp,
            IsFenced = isFenced,
            ShouldShutDown = shouldShutDown
        };
    }

    private long AssignNewEpoch()
    {
        lock (_epochLock)
        {
            return _nextBrokerEpoch++;
        }
    }

    private bool IsBrokerCaughtUp(BrokerRegistrationInfo registration)
    {
        // In a full implementation, we'd compare against the controller's metadata log offset
        // For now, consider caught up after first heartbeat with non-negative offset
        return registration.CurrentMetadataOffset >= 0;
    }

    /// <summary>
    /// Gets all registered brokers.
    /// </summary>
    public IEnumerable<BrokerRegistrationInfo> RegisteredBrokers => _registrations.Values;

    /// <summary>
    /// Get registration info for a specific broker.
    /// </summary>
    public BrokerRegistrationInfo? GetBrokerRegistration(int brokerId)
    {
        return _registrations.TryGetValue(brokerId, out var reg) ? reg : null;
    }

    /// <summary>
    /// Check if a broker is currently fenced.
    /// </summary>
    public bool IsBrokerFenced(int brokerId)
    {
        return !_registrations.TryGetValue(brokerId, out var reg) || reg.IsFenced;
    }
}

/// <summary>
/// Tracks registration information for a broker.
/// </summary>
public sealed class BrokerRegistrationInfo
{
    public required int BrokerId { get; init; }
    public required string ClusterId { get; init; }
    public required Guid IncarnationId { get; init; }
    public required long BrokerEpoch { get; init; }
    public required List<ListenerInfo> Listeners { get; init; }
    public required List<FeatureInfo> Features { get; init; }
    public string? Rack { get; init; }
    public List<Guid> LogDirs { get; init; } = [];
    public List<Guid> OfflineLogDirs { get; set; } = [];
    public bool IsFenced { get; set; } = true;
    public bool InControlledShutdown { get; set; }
    public required DateTimeOffset RegisteredAt { get; init; }
    public DateTimeOffset LastHeartbeat { get; set; }
    public long CurrentMetadataOffset { get; set; } = -1;
}

public sealed class ListenerInfo
{
    public required string Name { get; init; }
    public required string Host { get; init; }
    public required ushort Port { get; init; }
    public required short SecurityProtocol { get; init; }
}

public sealed class FeatureInfo
{
    public required string Name { get; init; }
    public required short MinVersion { get; init; }
    public required short MaxVersion { get; init; }
}

/// <summary>
/// Error code for stale broker epoch (not in base Kafka protocol but needed for KRaft).
/// </summary>
file static class KRaftErrorCodes
{
    public const short StaleBrokerEpoch = 77;
}

// Extension to add the error code
file static class ErrorCodeExtensions
{
    public static ErrorCode StaleBrokerEpoch => (ErrorCode)KRaftErrorCodes.StaleBrokerEpoch;
}
