using Kuestenlogik.Surgewave.Control.Models.Collaboration;
using Kuestenlogik.Surgewave.Control.Services.Collaboration;
using Microsoft.AspNetCore.SignalR;

namespace Kuestenlogik.Surgewave.Control.Hubs;

public sealed class PipelineCollaborationHub(CollaborationStateService state) : Hub
{
    /// <summary>Join a pipeline editing session.</summary>
    public async Task JoinPipeline(string pipelineId, string userName)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, pipelineId);

        var user = new PresenceInfo
        {
            ConnectionId = Context.ConnectionId,
            UserName = userName,
            JoinedAt = DateTimeOffset.UtcNow
        };

        state.UserJoined(pipelineId, user);

        // Notify others that a new user joined (with color assigned by the service)
        var presence = state.GetPresence(pipelineId);
        var joinedUser = presence.FirstOrDefault(p => p.ConnectionId == Context.ConnectionId);
        await Clients.Group(pipelineId).SendAsync("UserJoined", joinedUser ?? user);

        // Send full presence list to the newly joined user
        await Clients.Caller.SendAsync("PresenceSnapshot", presence);

        // Send existing comments and approvals to the newly joined user
        var comments = state.GetComments(pipelineId);
        await Clients.Caller.SendAsync("CommentsSnapshot", comments);

        var approvals = state.GetApprovals(pipelineId);
        await Clients.Caller.SendAsync("ApprovalsSnapshot", approvals);

        // Send current node locks
        var locks = state.GetNodeLocks(pipelineId);
        await Clients.Caller.SendAsync("NodeLocksSnapshot", locks);
    }

    /// <summary>Leave a pipeline editing session.</summary>
    public async Task LeavePipeline(string pipelineId)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, pipelineId);
        state.UserLeft(pipelineId, Context.ConnectionId);
        await Clients.Group(pipelineId).SendAsync("UserLeft", Context.ConnectionId);
    }

    /// <summary>Broadcast cursor position to other editors.</summary>
    public async Task UpdateCursor(string pipelineId, double x, double y)
    {
        state.UpdateCursor(pipelineId, Context.ConnectionId, x, y);
        await Clients.OthersInGroup(pipelineId).SendAsync("CursorMoved", Context.ConnectionId, x, y);
    }

    /// <summary>Broadcast that a node was moved.</summary>
    public async Task NodeMoved(string pipelineId, string nodeId, double x, double y)
    {
        await Clients.OthersInGroup(pipelineId).SendAsync("NodeMoved", Context.ConnectionId, nodeId, x, y);
    }

    /// <summary>Broadcast that a node was added.</summary>
    public async Task NodeAdded(string pipelineId, string nodeJson)
    {
        await Clients.OthersInGroup(pipelineId).SendAsync("NodeAdded", Context.ConnectionId, nodeJson);
    }

    /// <summary>Broadcast that a node was removed.</summary>
    public async Task NodeRemoved(string pipelineId, string nodeId)
    {
        await Clients.OthersInGroup(pipelineId).SendAsync("NodeRemoved", Context.ConnectionId, nodeId);
    }

    /// <summary>Broadcast that a node's configuration changed.</summary>
    public async Task NodeConfigChanged(string pipelineId, string nodeId, string configJson)
    {
        await Clients.OthersInGroup(pipelineId).SendAsync("NodeConfigChanged", Context.ConnectionId, nodeId, configJson);
    }

    /// <summary>Broadcast that a connection was added.</summary>
    public async Task ConnectionAdded(string pipelineId, string connectionJson)
    {
        await Clients.OthersInGroup(pipelineId).SendAsync("ConnectionAdded", Context.ConnectionId, connectionJson);
    }

    /// <summary>Broadcast that a connection was removed.</summary>
    public async Task ConnectionRemoved(string pipelineId, string connectionId)
    {
        await Clients.OthersInGroup(pipelineId).SendAsync("ConnectionRemoved", Context.ConnectionId, connectionId);
    }

    /// <summary>Lock a node for editing (prevents concurrent edits on the same node).</summary>
    public async Task LockNode(string pipelineId, string nodeId)
    {
        if (state.TryLockNode(pipelineId, nodeId, Context.ConnectionId))
        {
            await Clients.Group(pipelineId).SendAsync("NodeLocked", Context.ConnectionId, nodeId);
        }
        else
        {
            var owner = state.GetNodeLockOwner(pipelineId, nodeId);
            await Clients.Caller.SendAsync("LockDenied", nodeId, owner);
        }
    }

    /// <summary>Unlock a node after editing.</summary>
    public async Task UnlockNode(string pipelineId, string nodeId)
    {
        state.UnlockNode(pipelineId, nodeId, Context.ConnectionId);
        await Clients.Group(pipelineId).SendAsync("NodeUnlocked", Context.ConnectionId, nodeId);
    }

    /// <summary>Add a comment to the pipeline or a specific node.</summary>
    public async Task AddComment(string pipelineId, string author, string text, string? nodeId)
    {
        var comment = new PipelineComment
        {
            PipelineId = pipelineId,
            Author = author,
            Text = text,
            NodeId = nodeId
        };
        state.AddComment(comment);
        await Clients.Group(pipelineId).SendAsync("CommentAdded", comment);
    }

    /// <summary>Toggle the resolved state of a comment.</summary>
    public async Task ResolveComment(string pipelineId, string commentId)
    {
        state.ResolveComment(pipelineId, commentId);
        await Clients.Group(pipelineId).SendAsync("CommentResolved", commentId);
    }

    /// <summary>Request approval for the current pipeline state.</summary>
    public async Task RequestApproval(string pipelineId, string requestedBy, string? message)
    {
        var request = new ApprovalRequest
        {
            PipelineId = pipelineId,
            RequestedBy = requestedBy,
            Message = message
        };
        state.RequestApproval(request);
        await Clients.Group(pipelineId).SendAsync("ApprovalRequested", request);
    }

    /// <summary>Approve or reject a pending approval.</summary>
    public async Task ReviewApproval(string pipelineId, string approvalId, bool approved, string reviewer)
    {
        var status = approved ? ApprovalStatus.Approved : ApprovalStatus.Rejected;
        state.ReviewApproval(pipelineId, approvalId, status, reviewer);
        await Clients.Group(pipelineId).SendAsync("ApprovalReviewed", approvalId, status, reviewer);
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        // The hub doesn't track which groups a user is in, but the state service
        // cleans up locks when UserLeft is called. Clients should call LeavePipeline
        // on dispose, but as a safety net we could iterate sessions if needed.
        await base.OnDisconnectedAsync(exception);
    }
}
