using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Control.Models.Collaboration;

namespace Kuestenlogik.Surgewave.Control.Services.Collaboration;

public sealed class CollaborationStateService
{
    private static readonly string[] PresetColors =
    [
        "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4",
        "#FFEAA7", "#DDA0DD", "#98D8C8", "#F7DC6F"
    ];

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, PresenceInfo>> _sessions = new();
    private readonly ConcurrentDictionary<string, List<PipelineComment>> _comments = new();
    private readonly ConcurrentDictionary<string, List<ApprovalRequest>> _approvals = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, string>> _nodeLocks = new();

    // --- Presence ---

    public void UserJoined(string pipelineId, PresenceInfo user)
    {
        var session = _sessions.GetOrAdd(pipelineId, _ => new ConcurrentDictionary<string, PresenceInfo>());
        user.Color = AssignColor(pipelineId);
        session[user.ConnectionId] = user;
    }

    public void UserLeft(string pipelineId, string connectionId)
    {
        if (_sessions.TryGetValue(pipelineId, out var session))
        {
            session.TryRemove(connectionId, out _);

            // Release any locks held by this user
            if (_nodeLocks.TryGetValue(pipelineId, out var locks))
            {
                var heldLocks = locks.Where(kv => kv.Value == connectionId).Select(kv => kv.Key).ToList();
                foreach (var nodeId in heldLocks)
                {
                    locks.TryRemove(nodeId, out _);
                }
            }

            // Clean up empty sessions
            if (session.IsEmpty)
            {
                _sessions.TryRemove(pipelineId, out _);
            }
        }
    }

    public IReadOnlyList<PresenceInfo> GetPresence(string pipelineId)
    {
        if (_sessions.TryGetValue(pipelineId, out var session))
        {
            return session.Values.ToList();
        }
        return [];
    }

    public void UpdateCursor(string pipelineId, string connectionId, double x, double y)
    {
        if (_sessions.TryGetValue(pipelineId, out var session) &&
            session.TryGetValue(connectionId, out var user))
        {
            user.CursorX = x;
            user.CursorY = y;
        }
    }

    // --- Comments ---

    public void AddComment(PipelineComment comment)
    {
        var comments = _comments.GetOrAdd(comment.PipelineId, _ => []);
        lock (comments)
        {
            comments.Add(comment);
        }
    }

    public IReadOnlyList<PipelineComment> GetComments(string pipelineId)
    {
        if (_comments.TryGetValue(pipelineId, out var comments))
        {
            lock (comments)
            {
                return comments.ToList();
            }
        }
        return [];
    }

    public void ResolveComment(string pipelineId, string commentId)
    {
        if (_comments.TryGetValue(pipelineId, out var comments))
        {
            lock (comments)
            {
                var comment = comments.FirstOrDefault(c => c.Id == commentId);
                if (comment != null)
                {
                    comment.Resolved = !comment.Resolved;
                }
            }
        }
    }

    // --- Approvals ---

    public void RequestApproval(ApprovalRequest request)
    {
        var approvals = _approvals.GetOrAdd(request.PipelineId, _ => []);
        lock (approvals)
        {
            approvals.Add(request);
        }
    }

    public void ReviewApproval(string pipelineId, string approvalId, ApprovalStatus status, string reviewer)
    {
        if (_approvals.TryGetValue(pipelineId, out var approvals))
        {
            lock (approvals)
            {
                var approval = approvals.FirstOrDefault(a => a.Id == approvalId);
                if (approval != null)
                {
                    approval.Status = status;
                    approval.ReviewedBy = reviewer;
                    approval.ReviewedAt = DateTimeOffset.UtcNow;
                }
            }
        }
    }

    public IReadOnlyList<ApprovalRequest> GetApprovals(string pipelineId)
    {
        if (_approvals.TryGetValue(pipelineId, out var approvals))
        {
            lock (approvals)
            {
                return approvals.ToList();
            }
        }
        return [];
    }

    // --- Node Locking ---

    public bool TryLockNode(string pipelineId, string nodeId, string connectionId)
    {
        var locks = _nodeLocks.GetOrAdd(pipelineId, _ => new ConcurrentDictionary<string, string>());
        return locks.TryAdd(nodeId, connectionId);
    }

    public void UnlockNode(string pipelineId, string nodeId, string connectionId)
    {
        if (_nodeLocks.TryGetValue(pipelineId, out var locks))
        {
            locks.TryRemove(new KeyValuePair<string, string>(nodeId, connectionId));
        }
    }

    public string? GetNodeLockOwner(string pipelineId, string nodeId)
    {
        if (_nodeLocks.TryGetValue(pipelineId, out var locks) &&
            locks.TryGetValue(nodeId, out var owner))
        {
            return owner;
        }
        return null;
    }

    public IReadOnlyDictionary<string, string> GetNodeLocks(string pipelineId)
    {
        if (_nodeLocks.TryGetValue(pipelineId, out var locks))
        {
            return new Dictionary<string, string>(locks);
        }
        return new Dictionary<string, string>();
    }

    // --- Color Assignment ---

    public string AssignColor(string pipelineId)
    {
        if (_sessions.TryGetValue(pipelineId, out var session))
        {
            var usedColors = session.Values.Select(u => u.Color).ToHashSet();
            foreach (var color in PresetColors)
            {
                if (!usedColors.Contains(color))
                {
                    return color;
                }
            }
        }
        // Fallback: generate a random color if all presets are used
        return $"#{System.Security.Cryptography.RandomNumberGenerator.GetInt32(0x1000000):X6}";
    }
}
