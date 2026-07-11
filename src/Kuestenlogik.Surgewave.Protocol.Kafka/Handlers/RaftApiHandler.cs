using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Broker;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;

/// <summary>
/// Handler for Raft consensus APIs: Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum, FetchSnapshot.
/// These APIs implement the Raft protocol for ZooKeeper-free cluster management.
/// </summary>
public sealed partial class RaftApiHandler : IKafkaRequestHandler
{
    private readonly IBrokerConfigView _config;
    private readonly RaftNode? _raftNode;
    private readonly RaftPersistence? _raftPersistence;
    private readonly ClusterState _clusterState;
    private readonly ILogger<RaftApiHandler> _logger;

    // The Raft metadata topic - always partition 0
    private const string MetadataTopicName = "__cluster_metadata";
    private const int MetadataPartition = 0;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.Vote,
        ApiKey.BeginQuorumEpoch,
        ApiKey.EndQuorumEpoch,
        ApiKey.DescribeQuorum,
        ApiKey.FetchSnapshot,
        // KIP-853 voter-change RPCs. Surgewave advertises the API and registers
        // a handler so admin tools see a precise error code rather than the
        // generic UNSUPPORTED_VERSION; the underlying online-reconfiguration
        // protocol is not implemented yet, so the handler returns
        // UnsupportedVersion with a stable message documenting the
        // limitation. Static voter sets (the broker's startup config) work
        // unchanged through Vote / BeginQuorumEpoch / DescribeQuorum.
        ApiKey.AddRaftVoter,
        ApiKey.RemoveRaftVoter,
        ApiKey.UpdateRaftVoter,
    ];

    public RaftApiHandler(
        IBrokerConfigView config,
        RaftNode? raftNode,
        RaftPersistence? raftPersistence,
        ClusterState clusterState,
        ILogger<RaftApiHandler> logger)
    {
        _config = config;
        _raftNode = raftNode;
        _raftPersistence = raftPersistence;
        _clusterState = clusterState;
        _logger = logger;
    }

    public async Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        // KIP-853 voter-change RPCs always return the not-supported response,
        // regardless of whether the broker has a RaftNode. The cluster's
        // voter set is configured at startup and not changeable online — the
        // RaftNode being absent means consensus is disabled, so voter changes
        // are even less applicable. Either way the operator must reconfigure
        // and restart, which is the message the handler returns.
        switch (request)
        {
            case AddRaftVoterRequest addRaftVoterRequest: return HandleAddRaftVoter(addRaftVoterRequest);
            case RemoveRaftVoterRequest removeRaftVoterRequest: return HandleRemoveRaftVoter(removeRaftVoterRequest);
            case UpdateRaftVoterRequest updateRaftVoterRequest: return HandleUpdateRaftVoter(updateRaftVoterRequest);
        }

        // Raft consensus APIs require consensus to be enabled
        if (_raftNode == null)
        {
            return CreateNotControllerResponse(request);
        }

        return request switch
        {
            VoteRequest voteRequest => await HandleVoteAsync(voteRequest, cancellationToken),
            BeginQuorumEpochRequest beginQuorumEpochRequest => HandleBeginQuorumEpoch(beginQuorumEpochRequest),
            EndQuorumEpochRequest endQuorumEpochRequest => await HandleEndQuorumEpochAsync(endQuorumEpochRequest, cancellationToken),
            DescribeQuorumRequest describeQuorumRequest => HandleDescribeQuorum(describeQuorumRequest),
            FetchSnapshotRequest fetchSnapshotRequest => await HandleFetchSnapshotAsync(fetchSnapshotRequest, cancellationToken),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by RaftApiHandler")
        };
    }

    // KIP-853 dynamic Raft voter changes. Surgewave's RaftNode does not yet
    // implement the online-reconfiguration state machine (joint-consensus or
    // single-server-changes), so these handlers respond with
    // UnsupportedVersion plus a stable machine-readable reason. The wire
    // surface is intentionally bound (rather than left to the dispatcher's
    // generic "no handler" response) so admin tools that probe the API see
    // a precise error code instead of `UNSUPPORTED_VERSION (35)` from the
    // generic dispatcher fallback, which they otherwise interpret as a
    // protocol-version mismatch and retry against. The static voter set
    // (broker startup config) continues to work through the existing
    // Vote / BeginQuorumEpoch / DescribeQuorum / FetchSnapshot handlers.

    private const string KIP853NotSupportedMessage =
        "Online Raft voter reconfiguration (KIP-853) is not implemented in this Surgewave release. " +
        "Update the broker's voter configuration and restart instead.";

    private AddRaftVoterResponse HandleAddRaftVoter(AddRaftVoterRequest request)
    {
        // Shape validation runs before the not-supported reply so that an
        // operator who supplies a malformed request gets the precise
        // protocol error rather than a misleading "feature off" message.
        // The same pre-check will keep applying once the underlying
        // implementation lands.
        if (request.VoterId < 0)
        {
            return new AddRaftVoterResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.InvalidRequest,
                ErrorMessage = "VoterId must be non-negative.",
            };
        }
        if (request.Listeners.Count == 0)
        {
            return new AddRaftVoterResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.InvalidRequest,
                ErrorMessage = "AddRaftVoter requires at least one listener.",
            };
        }

        LogKip853Rejected(nameof(ApiKey.AddRaftVoter), request.VoterId);
        return new AddRaftVoterResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.UnsupportedVersion,
            ErrorMessage = KIP853NotSupportedMessage,
        };
    }

    private RemoveRaftVoterResponse HandleRemoveRaftVoter(RemoveRaftVoterRequest request)
    {
        if (request.VoterId < 0)
        {
            return new RemoveRaftVoterResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.InvalidRequest,
                ErrorMessage = "VoterId must be non-negative.",
            };
        }

        LogKip853Rejected(nameof(ApiKey.RemoveRaftVoter), request.VoterId);
        return new RemoveRaftVoterResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.UnsupportedVersion,
            ErrorMessage = KIP853NotSupportedMessage,
        };
    }

    private UpdateRaftVoterResponse HandleUpdateRaftVoter(UpdateRaftVoterRequest request)
    {
        if (request.VoterId < 0 || request.Listeners.Count == 0)
        {
            return new UpdateRaftVoterResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.InvalidRequest,
            };
        }

        LogKip853Rejected(nameof(ApiKey.UpdateRaftVoter), request.VoterId);
        return new UpdateRaftVoterResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.UnsupportedVersion,
        };
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "KIP-853 {Api} rejected for voterId={VoterId}: online voter changes not implemented")]
    private partial void LogKip853Rejected(string api, int voterId);

    /// <summary>
    /// Handle Vote request - candidate requests votes for leader election.
    /// Maps to internal Raft RequestVote RPC.
    /// </summary>
    private async Task<VoteResponse> HandleVoteAsync(VoteRequest request, CancellationToken ct)
    {
        LogVoteRequestReceived(request.ClusterId);

        // Validate cluster ID if provided
        if (!string.IsNullOrEmpty(request.ClusterId) &&
            !string.IsNullOrEmpty(_config.ClusterId) &&
            request.ClusterId != _config.ClusterId)
        {
            LogClusterIdMismatch(request.ClusterId, _config.ClusterId);
            return CreateVoteErrorResponse(request, ErrorCode.InconsistentClusterId);
        }

        var topics = new List<VoteResponse.TopicData>();

        foreach (var topic in request.Topics)
        {
            var partitions = new List<VoteResponse.PartitionData>();

            foreach (var partition in topic.Partitions)
            {
                // Only handle the metadata partition
                if (topic.TopicName != MetadataTopicName || partition.PartitionIndex != MetadataPartition)
                {
                    partitions.Add(new VoteResponse.PartitionData
                    {
                        PartitionIndex = partition.PartitionIndex,
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        LeaderId = -1,
                        LeaderEpoch = -1,
                        VoteGranted = false
                    });
                    continue;
                }

                // Map Kafka Vote API to internal Raft RequestVote
                var raftRequest = new RequestVoteRequest(
                    partition.CandidateEpoch,
                    partition.CandidateId,
                    partition.LastOffset,
                    partition.LastOffsetEpoch
                );

                var raftResponse = await _raftNode!.HandleRequestVoteAsync(raftRequest, ct);

                partitions.Add(new VoteResponse.PartitionData
                {
                    PartitionIndex = partition.PartitionIndex,
                    ErrorCode = ErrorCode.None,
                    LeaderId = _raftNode.LeaderId ?? -1,
                    LeaderEpoch = _raftNode.CurrentTerm,
                    VoteGranted = raftResponse.VoteGranted
                });

                LogVoteResult(partition.CandidateId, partition.CandidateEpoch, raftResponse.VoteGranted);
            }

            topics.Add(new VoteResponse.TopicData
            {
                TopicName = topic.TopicName,
                Partitions = partitions
            });
        }

        return new VoteResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            Topics = topics
        };
    }

    /// <summary>
    /// Handle BeginQuorumEpoch request - new leader notifies followers of the new epoch.
    /// </summary>
    private BeginQuorumEpochResponse HandleBeginQuorumEpoch(BeginQuorumEpochRequest request)
    {
        LogBeginQuorumEpochReceived(request.ClusterId);

        // Validate cluster ID if provided
        if (!string.IsNullOrEmpty(request.ClusterId) &&
            !string.IsNullOrEmpty(_config.ClusterId) &&
            request.ClusterId != _config.ClusterId)
        {
            LogClusterIdMismatch(request.ClusterId, _config.ClusterId);
            return CreateBeginQuorumEpochErrorResponse(request, ErrorCode.InconsistentClusterId);
        }

        var topics = new List<BeginQuorumEpochResponse.TopicData>();

        foreach (var topic in request.Topics)
        {
            var partitions = new List<BeginQuorumEpochResponse.PartitionData>();

            foreach (var partition in topic.Partitions)
            {
                // Only handle the metadata partition
                if (topic.TopicName != MetadataTopicName || partition.PartitionIndex != MetadataPartition)
                {
                    partitions.Add(new BeginQuorumEpochResponse.PartitionData
                    {
                        PartitionIndex = partition.PartitionIndex,
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        LeaderId = -1,
                        LeaderEpoch = -1
                    });
                    continue;
                }

                // Accept the new epoch notification - this is sent by the leader after winning election
                // The internal Raft node will recognize the leader through AppendEntries heartbeats
                LogNewEpochAcknowledged(partition.LeaderId, partition.LeaderEpoch);

                partitions.Add(new BeginQuorumEpochResponse.PartitionData
                {
                    PartitionIndex = partition.PartitionIndex,
                    ErrorCode = ErrorCode.None,
                    LeaderId = _raftNode!.LeaderId ?? partition.LeaderId,
                    LeaderEpoch = _raftNode.CurrentTerm
                });
            }

            topics.Add(new BeginQuorumEpochResponse.TopicData
            {
                TopicName = topic.TopicName,
                Partitions = partitions
            });
        }

        return new BeginQuorumEpochResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            Topics = topics
        };
    }

    /// <summary>
    /// Handle EndQuorumEpoch request - leader resigns, triggers election in followers.
    /// </summary>
    private async Task<EndQuorumEpochResponse> HandleEndQuorumEpochAsync(EndQuorumEpochRequest request, CancellationToken ct)
    {
        LogEndQuorumEpochReceived(request.ClusterId);

        // Validate cluster ID if provided
        if (!string.IsNullOrEmpty(request.ClusterId) &&
            !string.IsNullOrEmpty(_config.ClusterId) &&
            request.ClusterId != _config.ClusterId)
        {
            LogClusterIdMismatch(request.ClusterId, _config.ClusterId);
            return CreateEndQuorumEpochErrorResponse(request, ErrorCode.InconsistentClusterId);
        }

        var topics = new List<EndQuorumEpochResponse.TopicData>();

        foreach (var topic in request.Topics)
        {
            var partitions = new List<EndQuorumEpochResponse.PartitionData>();

            foreach (var partition in topic.Partitions)
            {
                // Only handle the metadata partition
                if (topic.TopicName != MetadataTopicName || partition.PartitionIndex != MetadataPartition)
                {
                    partitions.Add(new EndQuorumEpochResponse.PartitionData
                    {
                        PartitionIndex = partition.PartitionIndex,
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        LeaderId = -1,
                        LeaderEpoch = -1
                    });
                    continue;
                }

                // Leader is resigning - if we are the leader, step down
                if (_raftNode!.IsLeader && partition.LeaderId == _config.BrokerId)
                {
                    LogLeaderResigning(partition.LeaderEpoch);
                    await _raftNode.StepDownAsync();
                }

                // The election will be triggered automatically by the Raft timeout mechanism
                // Preferred successors are hints but Raft will elect based on log completeness

                partitions.Add(new EndQuorumEpochResponse.PartitionData
                {
                    PartitionIndex = partition.PartitionIndex,
                    ErrorCode = ErrorCode.None,
                    LeaderId = _raftNode.LeaderId ?? -1,
                    LeaderEpoch = _raftNode.CurrentTerm
                });
            }

            topics.Add(new EndQuorumEpochResponse.TopicData
            {
                TopicName = topic.TopicName,
                Partitions = partitions
            });
        }

        return new EndQuorumEpochResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            Topics = topics
        };
    }

    /// <summary>
    /// Handle DescribeQuorum request - return current quorum state for monitoring.
    /// </summary>
    private DescribeQuorumResponse HandleDescribeQuorum(DescribeQuorumRequest request)
    {
        LogDescribeQuorumReceived();

        var topics = new List<DescribeQuorumResponse.TopicData>();

        foreach (var topic in request.Topics)
        {
            var partitions = new List<DescribeQuorumResponse.PartitionData>();

            foreach (var partition in topic.Partitions)
            {
                // Only handle the metadata partition
                if (topic.TopicName != MetadataTopicName || partition.PartitionIndex != MetadataPartition)
                {
                    partitions.Add(new DescribeQuorumResponse.PartitionData
                    {
                        PartitionIndex = partition.PartitionIndex,
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        LeaderId = -1,
                        LeaderEpoch = -1,
                        HighWatermark = -1,
                        CurrentVoters = [],
                        Observers = []
                    });
                    continue;
                }

                // Build voter list from cluster state
                var currentVoters = new List<DescribeQuorumResponse.ReplicaState>();
                var observers = new List<DescribeQuorumResponse.ReplicaState>();
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                // Add self as a voter
                currentVoters.Add(new DescribeQuorumResponse.ReplicaState
                {
                    ReplicaId = _config.BrokerId,
                    LogEndOffset = _raftNode!.LastLogIndex,
                    LastFetchTimestamp = now,
                    LastCaughtUpTimestamp = now
                });

                // Add other known brokers as voters
                foreach (var broker in _clusterState.Brokers.Values)
                {
                    if (broker.BrokerId != _config.BrokerId)
                    {
                        currentVoters.Add(new DescribeQuorumResponse.ReplicaState
                        {
                            ReplicaId = broker.BrokerId,
                            LogEndOffset = _raftNode.GetPeerMatchIndex(broker.BrokerId),
                            LastFetchTimestamp = _raftNode.GetPeerLastContact(broker.BrokerId)?.ToUnixTimeMilliseconds() ?? -1,
                            LastCaughtUpTimestamp = _raftNode.GetPeerLastContact(broker.BrokerId)?.ToUnixTimeMilliseconds() ?? -1
                        });
                    }
                }

                partitions.Add(new DescribeQuorumResponse.PartitionData
                {
                    PartitionIndex = partition.PartitionIndex,
                    ErrorCode = ErrorCode.None,
                    LeaderId = _raftNode.LeaderId ?? -1,
                    LeaderEpoch = _raftNode.CurrentTerm,
                    HighWatermark = _raftNode.CommitIndex,
                    CurrentVoters = currentVoters,
                    Observers = observers
                });
            }

            topics.Add(new DescribeQuorumResponse.TopicData
            {
                TopicName = topic.TopicName,
                Partitions = partitions
            });
        }

        return new DescribeQuorumResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            Topics = topics
        };
    }

    /// <summary>
    /// Handle FetchSnapshot request - return snapshot data for followers catching up.
    /// </summary>
    private async Task<FetchSnapshotResponse> HandleFetchSnapshotAsync(FetchSnapshotRequest request, CancellationToken ct)
    {
        LogFetchSnapshotReceived(request.ReplicaId);

        // Validate cluster ID if provided (v1+)
        if (!string.IsNullOrEmpty(request.ClusterId) &&
            !string.IsNullOrEmpty(_config.ClusterId) &&
            request.ClusterId != _config.ClusterId)
        {
            LogClusterIdMismatch(request.ClusterId, _config.ClusterId);
            return new FetchSnapshotResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.InconsistentClusterId,
                Topics = []
            };
        }

        var topics = new List<FetchSnapshotResponse.TopicSnapshot>();

        foreach (var topic in request.Topics)
        {
            var partitions = new List<FetchSnapshotResponse.PartitionSnapshot>();

            foreach (var partition in topic.Partitions)
            {
                // Only handle the metadata partition
                if (topic.Name != MetadataTopicName || partition.Partition != MetadataPartition)
                {
                    partitions.Add(new FetchSnapshotResponse.PartitionSnapshot
                    {
                        Index = partition.Partition,
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        SnapshotId = new FetchSnapshotResponse.SnapshotId
                        {
                            EndOffset = 0,
                            Epoch = 0
                        },
                        Size = 0,
                        Position = 0,
                        UnalignedRecords = null
                    });
                    continue;
                }

                // Check if we have the requested snapshot
                if (_raftPersistence == null)
                {
                    partitions.Add(new FetchSnapshotResponse.PartitionSnapshot
                    {
                        Index = partition.Partition,
                        ErrorCode = ErrorCode.SnapshotNotFound,
                        SnapshotId = new FetchSnapshotResponse.SnapshotId
                        {
                            EndOffset = partition.SnapshotId.EndOffset,
                            Epoch = partition.SnapshotId.Epoch
                        },
                        Size = 0,
                        Position = 0,
                        UnalignedRecords = null
                    });
                    continue;
                }

                // Try to load snapshot
                var snapshot = await _raftPersistence.LoadSnapshotAsync(
                    partition.SnapshotId.EndOffset,
                    partition.SnapshotId.Epoch,
                    ct);

                if (snapshot == null)
                {
                    partitions.Add(new FetchSnapshotResponse.PartitionSnapshot
                    {
                        Index = partition.Partition,
                        ErrorCode = ErrorCode.SnapshotNotFound,
                        SnapshotId = new FetchSnapshotResponse.SnapshotId
                        {
                            EndOffset = partition.SnapshotId.EndOffset,
                            Epoch = partition.SnapshotId.Epoch
                        },
                        CurrentLeader = new FetchSnapshotResponse.LeaderIdAndEpoch
                        {
                            LeaderId = _raftNode!.LeaderId ?? -1,
                            LeaderEpoch = _raftNode.CurrentTerm
                        },
                        Size = 0,
                        Position = 0,
                        UnalignedRecords = null
                    });
                    continue;
                }

                // Calculate bytes to return based on position and MaxBytes
                var remainingBytes = snapshot.Data.Length - (int)partition.Position;
                var bytesToReturn = Math.Min(remainingBytes, request.MaxBytes);
                byte[]? data = null;

                if (bytesToReturn > 0 && partition.Position < snapshot.Data.Length)
                {
                    data = new byte[bytesToReturn];
                    Array.Copy(snapshot.Data, partition.Position, data, 0, bytesToReturn);
                }

                partitions.Add(new FetchSnapshotResponse.PartitionSnapshot
                {
                    Index = partition.Partition,
                    ErrorCode = ErrorCode.None,
                    SnapshotId = new FetchSnapshotResponse.SnapshotId
                    {
                        EndOffset = snapshot.EndOffset,
                        Epoch = snapshot.Epoch
                    },
                    CurrentLeader = new FetchSnapshotResponse.LeaderIdAndEpoch
                    {
                        LeaderId = _raftNode!.LeaderId ?? -1,
                        LeaderEpoch = _raftNode.CurrentTerm
                    },
                    Size = snapshot.Data.Length,
                    Position = partition.Position,
                    UnalignedRecords = data
                });

                LogSnapshotFetched(partition.SnapshotId.EndOffset, partition.SnapshotId.Epoch, partition.Position, bytesToReturn);
            }

            topics.Add(new FetchSnapshotResponse.TopicSnapshot
            {
                Name = topic.Name,
                Partitions = partitions
            });
        }

        return new FetchSnapshotResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            Topics = topics
        };
    }

    #region Helper Methods

    private KafkaResponse CreateNotControllerResponse(KafkaRequest request)
    {
        return request switch
        {
            VoteRequest vr => CreateVoteErrorResponse(vr, ErrorCode.NotController),
            BeginQuorumEpochRequest br => CreateBeginQuorumEpochErrorResponse(br, ErrorCode.NotController),
            EndQuorumEpochRequest er => CreateEndQuorumEpochErrorResponse(er, ErrorCode.NotController),
            DescribeQuorumRequest dr => new DescribeQuorumResponse
            {
                CorrelationId = dr.CorrelationId,
                ApiVersion = dr.ApiVersion,
                ErrorCode = ErrorCode.NotController,
                Topics = []
            },
            FetchSnapshotRequest fr => new FetchSnapshotResponse
            {
                CorrelationId = fr.CorrelationId,
                ApiVersion = fr.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.NotController,
                Topics = []
            },
            _ => throw new NotSupportedException()
        };
    }

    private static VoteResponse CreateVoteErrorResponse(VoteRequest request, ErrorCode errorCode)
    {
        return new VoteResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = errorCode,
            Topics = []
        };
    }

    private static BeginQuorumEpochResponse CreateBeginQuorumEpochErrorResponse(BeginQuorumEpochRequest request, ErrorCode errorCode)
    {
        return new BeginQuorumEpochResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = errorCode,
            Topics = []
        };
    }

    private static EndQuorumEpochResponse CreateEndQuorumEpochErrorResponse(EndQuorumEpochRequest request, ErrorCode errorCode)
    {
        return new EndQuorumEpochResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = errorCode,
            Topics = []
        };
    }

    #endregion

    #region Logging

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received Vote request from cluster {ClusterId}")]
    private partial void LogVoteRequestReceived(string? clusterId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Cluster ID mismatch: request={RequestClusterId}, local={LocalClusterId}")]
    private partial void LogClusterIdMismatch(string? requestClusterId, string? localClusterId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Vote result: candidate={CandidateId}, epoch={Epoch}, granted={Granted}")]
    private partial void LogVoteResult(int candidateId, int epoch, bool granted);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received BeginQuorumEpoch request from cluster {ClusterId}")]
    private partial void LogBeginQuorumEpochReceived(string? clusterId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Acknowledged new epoch: leader={LeaderId}, epoch={Epoch}")]
    private partial void LogNewEpochAcknowledged(int leaderId, int epoch);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received EndQuorumEpoch request from cluster {ClusterId}")]
    private partial void LogEndQuorumEpochReceived(string? clusterId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Leader resigning, epoch={Epoch}")]
    private partial void LogLeaderResigning(int epoch);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received DescribeQuorum request")]
    private partial void LogDescribeQuorumReceived();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Received FetchSnapshot request from replica {ReplicaId}")]
    private partial void LogFetchSnapshotReceived(int replicaId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Fetched snapshot: endOffset={EndOffset}, epoch={Epoch}, position={Position}, bytes={Bytes}")]
    private partial void LogSnapshotFetched(long endOffset, int epoch, long position, int bytes);

    #endregion
}
