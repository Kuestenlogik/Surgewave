using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.StreamsGroups;

/// <summary>
/// Manages streams group lifecycle, membership, and topology-aware task assignment (KIP-1071).
/// Similar to Consumer Group v2 but with task-based assignment instead of partition assignment.
/// Tasks correspond to subtopology + partition combinations.
/// </summary>
public sealed class StreamsGroupCoordinator(
    ILogger<StreamsGroupCoordinator> logger,
    LogManager logManager)
{
    private readonly Dictionary<string, StreamsGroupState> _groups = [];
    private readonly Lock _groupLock = new();

    private const int DefaultHeartbeatIntervalMs = 5000;
    private const int DefaultAcceptableRecoveryLag = 10000;
    private const int DefaultTaskOffsetIntervalMs = 30000;
    private static readonly TimeSpan StaleHeartbeatTimeout = TimeSpan.FromSeconds(45);

    // ─────────────────────────────────────────────────────────────
    // StreamsGroupHeartbeat (API Key 88)
    // ─────────────────────────────────────────────────────────────

    public StreamsGroupHeartbeatResponse HandleStreamsGroupHeartbeat(StreamsGroupHeartbeatRequest request)
    {
        lock (_groupLock)
        {
            logger.LogDebug("StreamsGroupHeartbeat: GroupId={GroupId}, MemberId={MemberId}, MemberEpoch={MemberEpoch}",
                request.GroupId, request.MemberId, request.MemberEpoch);

            // MemberEpoch -2 means shutdown (leave with intent to rejoin as static member)
            // MemberEpoch -1 means leave
            if (request.MemberEpoch == -1 || request.MemberEpoch == -2)
            {
                return HandleLeave(request);
            }

            if (!_groups.TryGetValue(request.GroupId, out var group))
            {
                group = new StreamsGroupState { GroupId = request.GroupId };
                _groups[request.GroupId] = group;
                logger.LogInformation("StreamsGroup created: {GroupId}", request.GroupId);
            }

            // Clean stale members
            CleanStaleMembers(group);

            // Store topology if provided (typically on first join, MemberEpoch=0)
            if (request.Topology != null)
            {
                var subtopologies = new List<StoredSubtopology>(request.Topology.Subtopologies.Count);
                foreach (var sub in request.Topology.Subtopologies)
                {
                    subtopologies.Add(new StoredSubtopology
                    {
                        SubtopologyId = sub.SubtopologyId,
                        SourceTopics = new List<string>(sub.SourceTopics)
                    });
                }

                group.Topology = new StoredTopology
                {
                    Epoch = request.Topology.Epoch,
                    Subtopologies = subtopologies
                };
                group.TopologyEpoch = request.Topology.Epoch;
                logger.LogInformation("StreamsGroup {GroupId}: topology updated (epoch={Epoch}, subtopologies={Count})",
                    request.GroupId, request.Topology.Epoch, subtopologies.Count);
            }

            // Generate member ID if epoch is 0 (join)
            var memberId = request.MemberId;
            if (request.MemberEpoch == 0 && string.IsNullOrEmpty(memberId))
            {
                memberId = $"{request.ClientId}-{Guid.NewGuid()}";
            }

            // Add or update member
            bool memberJoined = false;
            if (!group.Members.TryGetValue(memberId, out var member))
            {
                member = new StreamsGroupMember
                {
                    MemberId = memberId,
                    InstanceId = request.InstanceId,
                    RackId = request.RackId,
                    ClientId = request.ClientId,
                    ClientHost = "*",
                    ProcessId = request.ProcessId
                };
                group.Members[memberId] = member;
                memberJoined = true;
                group.GroupEpoch++;
                logger.LogInformation("StreamsGroup {GroupId}: member {MemberId} joined (epoch={Epoch})",
                    request.GroupId, memberId, group.GroupEpoch);
            }

            member.LastHeartbeat = DateTime.UtcNow;

            if (request.InstanceId != null)
            {
                member.InstanceId = request.InstanceId;
            }

            if (request.RackId != null)
            {
                member.RackId = request.RackId;
            }

            if (request.ProcessId != null)
            {
                member.ProcessId = request.ProcessId;
            }

            member.TopologyEpoch = group.TopologyEpoch;

            // Rebalance tasks across members if membership changed
            if (memberJoined)
            {
                RebalanceTasks(group);
            }

            member.MemberEpoch = group.GroupEpoch;

            // Build assignment response
            var activeTasks = ConvertToResponseTaskIds(member.ActiveTasks);
            var standbyTasks = ConvertToResponseTaskIds(member.StandbyTasks);
            var warmupTasks = ConvertToResponseTaskIds(member.WarmupTasks);

            return new StreamsGroupHeartbeatResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ErrorCode = ErrorCode.None,
                MemberId = memberId,
                MemberEpoch = member.MemberEpoch,
                HeartbeatIntervalMs = DefaultHeartbeatIntervalMs,
                AcceptableRecoveryLag = DefaultAcceptableRecoveryLag,
                TaskOffsetIntervalMs = DefaultTaskOffsetIntervalMs,
                ActiveTasks = activeTasks,
                StandbyTasks = standbyTasks,
                WarmupTasks = warmupTasks
            };
        }
    }

    private StreamsGroupHeartbeatResponse HandleLeave(StreamsGroupHeartbeatRequest request)
    {
        if (_groups.TryGetValue(request.GroupId, out var group))
        {
            if (group.Members.Remove(request.MemberId))
            {
                group.GroupEpoch++;
                RebalanceTasks(group);
                logger.LogInformation("StreamsGroup {GroupId}: member {MemberId} left (epoch={Epoch})",
                    request.GroupId, request.MemberId, group.GroupEpoch);
            }
        }

        return new StreamsGroupHeartbeatResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ErrorCode = ErrorCode.None,
            MemberId = request.MemberId,
            MemberEpoch = request.MemberEpoch, // Echo back -1 or -2
            HeartbeatIntervalMs = DefaultHeartbeatIntervalMs,
            AcceptableRecoveryLag = DefaultAcceptableRecoveryLag,
            TaskOffsetIntervalMs = DefaultTaskOffsetIntervalMs
        };
    }

    // ─────────────────────────────────────────────────────────────
    // StreamsGroupDescribe (API Key 89)
    // ─────────────────────────────────────────────────────────────

    public StreamsGroupDescribeResponse HandleStreamsGroupDescribe(StreamsGroupDescribeRequest request)
    {
        lock (_groupLock)
        {
            var groups = new List<StreamsGroupDescribeResponse.DescribedGroup>(request.GroupIds.Count);

            foreach (var groupId in request.GroupIds)
            {
                if (!_groups.TryGetValue(groupId, out var group))
                {
                    groups.Add(new StreamsGroupDescribeResponse.DescribedGroup
                    {
                        ErrorCode = ErrorCode.InvalidGroupId,
                        GroupId = groupId,
                        GroupState = "",
                        Members = []
                    });
                    continue;
                }

                var members = new List<StreamsGroupDescribeResponse.Member>(group.Members.Count);
                foreach (var m in group.Members.Values)
                {
                    members.Add(new StreamsGroupDescribeResponse.Member
                    {
                        MemberId = m.MemberId,
                        MemberEpoch = m.MemberEpoch,
                        InstanceId = m.InstanceId,
                        RackId = m.RackId,
                        ClientId = m.ClientId ?? "",
                        ClientHost = m.ClientHost ?? "*",
                        TopologyEpoch = m.TopologyEpoch,
                        ProcessId = m.ProcessId ?? "",
                        ClientTags = [],
                        TaskOffsets = [],
                        TaskEndOffsets = [],
                        Assignment = ConvertToDescribeAssignment(m.ActiveTasks, m.StandbyTasks, m.WarmupTasks),
                        TargetAssignment = ConvertToDescribeAssignment(m.ActiveTasks, m.StandbyTasks, m.WarmupTasks)
                    });
                }

                // Build topology for describe response
                StreamsGroupDescribeResponse.TopologyInfo? topology = null;
                if (group.Topology != null)
                {
                    var subtopologies = new List<StreamsGroupDescribeResponse.SubtopologyInfo>(group.Topology.Subtopologies.Count);
                    foreach (var sub in group.Topology.Subtopologies)
                    {
                        subtopologies.Add(new StreamsGroupDescribeResponse.SubtopologyInfo
                        {
                            SubtopologyId = sub.SubtopologyId,
                            SourceTopics = new List<string>(sub.SourceTopics),
                            RepartitionSinkTopics = [],
                            StateChangelogTopics = [],
                            RepartitionSourceTopics = []
                        });
                    }

                    topology = new StreamsGroupDescribeResponse.TopologyInfo
                    {
                        Epoch = group.Topology.Epoch,
                        Subtopologies = subtopologies
                    };
                }

                groups.Add(new StreamsGroupDescribeResponse.DescribedGroup
                {
                    ErrorCode = ErrorCode.None,
                    GroupId = groupId,
                    GroupState = GetGroupState(group),
                    GroupEpoch = group.GroupEpoch,
                    // StreamsGroup-State haelt KEIN separates AssignmentEpoch-Feld (anders als
                    // Java/Kafka). AssignmentEpoch == GroupEpoch ist damit by-construction
                    // garantiert — die KAFKA-20442-Invariante (empty group => epochs gleich) gilt
                    // trivial.
                    AssignmentEpoch = group.GroupEpoch,
                    Topology = topology,
                    Members = members
                });
            }

            return new StreamsGroupDescribeResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                Groups = groups
            };
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Task assignment (range-based across subtopology partitions)
    // ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Rebalances tasks across all members using a sticky algorithm (KIP-947). Tasks
    /// already held by a still-present member are kept on that member; only orphaned
    /// tasks (from members that left) and brand-new tasks (from new partitions or
    /// subtopologies) move. After the sticky placement, the assignment is balanced by
    /// migrating one task at a time from the most-loaded to the least-loaded member
    /// until the spread is minimal. This keeps state stores warm across minor
    /// membership changes — the original Kafka Streams motivation for KIP-947.
    /// </summary>
    private void RebalanceTasks(StreamsGroupState group)
    {
        if (group.Members.Count == 0 || group.Topology == null)
        {
            return;
        }

        // Snapshot the previous active assignment so we can preserve sticky placements
        // before clearing.
        var previous = SnapshotPreviousAssignment(group);

        foreach (var member in group.Members.Values)
        {
            member.ActiveTasks.Clear();
            member.StandbyTasks.Clear();
            member.WarmupTasks.Clear();
        }

        // Collect all tasks: for each subtopology, determine partition count from source topics
        var allTasks = new List<(string SubtopologyId, int Partition)>();
        foreach (var subtopology in group.Topology.Subtopologies)
        {
            var partitionCount = GetMaxPartitionCount(subtopology.SourceTopics);
            for (int p = 0; p < partitionCount; p++)
            {
                allTasks.Add((subtopology.SubtopologyId, p));
            }
        }

        if (allTasks.Count == 0)
        {
            return;
        }

        // Sort members by MemberId for deterministic tie-breaks.
        var memberList = new List<StreamsGroupMember>(group.Members.Values);
        memberList.Sort((a, b) => string.Compare(a.MemberId, b.MemberId, StringComparison.Ordinal));
        var memberById = memberList.ToDictionary(m => m.MemberId);
        var taskBuckets = memberList.ToDictionary(
            m => m.MemberId,
            _ => new Dictionary<string, List<int>>(StringComparer.Ordinal));

        // Pass 1 — sticky: re-assign each task to its previous owner if that member is
        // still in the group.
        var unassigned = new List<(string SubtopologyId, int Partition)>();
        foreach (var task in allTasks)
        {
            if (previous.TryGetValue(task, out var prevOwner) && memberById.ContainsKey(prevOwner))
            {
                AddToBucket(taskBuckets[prevOwner], task);
            }
            else
            {
                unassigned.Add(task);
            }
        }

        // Pass 2 — distribute orphaned tasks to whichever member currently has the
        // fewest tasks, breaking ties by MemberId.
        foreach (var task in unassigned)
        {
            var target = memberList.OrderBy(m => CountTasks(taskBuckets[m.MemberId]))
                .ThenBy(m => m.MemberId, StringComparer.Ordinal)
                .First();
            AddToBucket(taskBuckets[target.MemberId], task);
        }

        // Pass 3 — balance: if any member has more than ⌈total/N⌉ tasks while another
        // has fewer than ⌊total/N⌋, migrate one task at a time from heavy → light.
        // This is the KIP-947 step that prevents sticky from becoming "stuck": tasks
        // owned by a still-present member CAN move when balance demands it, but only
        // the minimum needed.
        var ceiling = (allTasks.Count + memberList.Count - 1) / memberList.Count;
        var floor = allTasks.Count / memberList.Count;
        var safety = allTasks.Count + 1; // hard cap so a logic bug can't infinite-loop
        while (safety-- > 0)
        {
            var heaviest = memberList.OrderByDescending(m => CountTasks(taskBuckets[m.MemberId]))
                .ThenBy(m => m.MemberId, StringComparer.Ordinal)
                .First();
            var lightest = memberList.OrderBy(m => CountTasks(taskBuckets[m.MemberId]))
                .ThenBy(m => m.MemberId, StringComparer.Ordinal)
                .First();

            var heavyCount = CountTasks(taskBuckets[heaviest.MemberId]);
            var lightCount = CountTasks(taskBuckets[lightest.MemberId]);

            if (heavyCount <= ceiling && lightCount >= floor) break;
            if (heavyCount - lightCount <= 1) break;

            // Move the LAST task from heaviest to lightest. Picking the last (highest
            // partition number, lexically last subtopology) is deterministic and lets
            // sticky members keep their lower-numbered partitions across rebalances.
            var heavyBucket = taskBuckets[heaviest.MemberId];
            var subtopologyToMove = heavyBucket.Keys.OrderByDescending(s => s, StringComparer.Ordinal).First();
            var partitionToMove = heavyBucket[subtopologyToMove][^1];
            heavyBucket[subtopologyToMove].RemoveAt(heavyBucket[subtopologyToMove].Count - 1);
            if (heavyBucket[subtopologyToMove].Count == 0) heavyBucket.Remove(subtopologyToMove);

            AddToBucket(taskBuckets[lightest.MemberId], (subtopologyToMove, partitionToMove));
        }

        // Pass 3 — flatten: only after the sticky/balance pass do we mutate the group.
        foreach (var member in memberList)
        {
            foreach (var (subtopologyId, partitions) in taskBuckets[member.MemberId])
            {
                partitions.Sort();
                member.ActiveTasks.Add(new StreamsTaskIds
                {
                    SubtopologyId = subtopologyId,
                    Partitions = partitions
                });
            }
        }

        logger.LogDebug("StreamsGroup {GroupId}: rebalanced {TaskCount} tasks across {MemberCount} members (epoch={Epoch}, sticky)",
            group.GroupId, allTasks.Count, memberList.Count, group.GroupEpoch);
    }

    private static Dictionary<(string Subtopology, int Partition), string> SnapshotPreviousAssignment(StreamsGroupState group)
    {
        var snapshot = new Dictionary<(string, int), string>();
        foreach (var member in group.Members.Values)
        {
            foreach (var task in member.ActiveTasks)
            {
                foreach (var partition in task.Partitions)
                {
                    snapshot[(task.SubtopologyId, partition)] = member.MemberId;
                }
            }
        }
        return snapshot;
    }

    private static void AddToBucket(Dictionary<string, List<int>> bucket, (string SubtopologyId, int Partition) task)
    {
        if (!bucket.TryGetValue(task.SubtopologyId, out var partitions))
        {
            partitions = [];
            bucket[task.SubtopologyId] = partitions;
        }
        partitions.Add(task.Partition);
    }

    private static int CountTasks(Dictionary<string, List<int>> bucket)
    {
        var total = 0;
        foreach (var partitions in bucket.Values) total += partitions.Count;
        return total;
    }

    /// <summary>
    /// Returns the maximum partition count across the given source topics.
    /// This determines how many tasks exist for a subtopology.
    /// </summary>
    private int GetMaxPartitionCount(List<string> sourceTopics)
    {
        int maxPartitions = 0;
        foreach (var topic in sourceTopics)
        {
            var metadata = logManager.GetTopicMetadata(topic);
            if (metadata != null && metadata.PartitionCount > maxPartitions)
            {
                maxPartitions = metadata.PartitionCount;
            }
        }
        return maxPartitions;
    }

    // ─────────────────────────────────────────────────────────────
    // Internal helpers
    // ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Background-sweep entry point: evicts stale members across every known streams
    /// group. Triggers a task rebalance for any group whose membership changed.
    /// </summary>
    public void SweepStaleMembers()
    {
        lock (_groupLock)
        {
            foreach (var group in _groups.Values)
            {
                CleanStaleMembers(group);
            }
        }
    }

    private void CleanStaleMembers(StreamsGroupState group)
    {
        var now = DateTime.UtcNow;
        List<string>? staleMembers = null;

        foreach (var kvp in group.Members)
        {
            if (now - kvp.Value.LastHeartbeat > StaleHeartbeatTimeout)
            {
                staleMembers ??= [];
                staleMembers.Add(kvp.Key);
            }
        }

        if (staleMembers != null)
        {
            foreach (var staleMember in staleMembers)
            {
                group.Members.Remove(staleMember);
                logger.LogInformation("StreamsGroup {GroupId}: removed stale member {MemberId}", group.GroupId, staleMember);
            }

            group.GroupEpoch++;
            RebalanceTasks(group);
        }
    }

    private static List<StreamsGroupHeartbeatResponse.TaskIds>? ConvertToResponseTaskIds(List<StreamsTaskIds> tasks)
    {
        if (tasks.Count == 0) return null;

        var result = new List<StreamsGroupHeartbeatResponse.TaskIds>(tasks.Count);
        foreach (var task in tasks)
        {
            result.Add(new StreamsGroupHeartbeatResponse.TaskIds
            {
                SubtopologyId = task.SubtopologyId,
                Partitions = new List<int>(task.Partitions)
            });
        }
        return result;
    }

    private static StreamsGroupDescribeResponse.AssignmentInfo ConvertToDescribeAssignment(
        List<StreamsTaskIds> activeTasks,
        List<StreamsTaskIds> standbyTasks,
        List<StreamsTaskIds> warmupTasks)
    {
        return new StreamsGroupDescribeResponse.AssignmentInfo
        {
            ActiveTasks = ConvertToDescribeTaskIds(activeTasks),
            StandbyTasks = ConvertToDescribeTaskIds(standbyTasks),
            WarmupTasks = ConvertToDescribeTaskIds(warmupTasks)
        };
    }

    private static List<StreamsGroupDescribeResponse.TaskIds> ConvertToDescribeTaskIds(List<StreamsTaskIds> tasks)
    {
        var result = new List<StreamsGroupDescribeResponse.TaskIds>(tasks.Count);
        foreach (var task in tasks)
        {
            result.Add(new StreamsGroupDescribeResponse.TaskIds
            {
                SubtopologyId = task.SubtopologyId,
                Partitions = new List<int>(task.Partitions)
            });
        }
        return result;
    }

    private static string GetGroupState(StreamsGroupState group)
    {
        if (group.Members.Count == 0) return "Empty";
        return "Stable";
    }
}
