using System.Text.Json;
using Kuestenlogik.Surgewave.Control.Components.Pipeline;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Manages undo/redo state and clipboard for pipeline editor.
/// </summary>
public sealed class PipelineEditorState
{
    private readonly Stack<PipelineSnapshot> _undoStack = new();
    private readonly Stack<PipelineSnapshot> _redoStack = new();
    private readonly List<ClipboardNode> _clipboard = [];

    public const int MaxUndoSteps = 50;

    public bool CanUndo => _undoStack.Count > 0;
    public bool CanRedo => _redoStack.Count > 0;
    public bool HasClipboard => _clipboard.Count > 0;
    public int ClipboardCount => _clipboard.Count;

    public event Action? StateChanged;

    /// <summary>
    /// Saves the current state for undo.
    /// </summary>
    public void SaveState(IEnumerable<ConnectorNode> nodes, IEnumerable<PipelineLink> links, string description)
    {
        var snapshot = new PipelineSnapshot
        {
            Description = description,
            Timestamp = DateTime.UtcNow,
            Nodes = nodes.Select(SerializeNode).ToList(),
            Links = links.Select(SerializeLink).ToList()
        };

        _undoStack.Push(snapshot);
        _redoStack.Clear(); // Clear redo stack on new action

        // Limit stack size
        while (_undoStack.Count > MaxUndoSteps)
        {
            // Remove oldest items by converting to list and back
            var items = _undoStack.ToList();
            items.RemoveAt(items.Count - 1);
            _undoStack.Clear();
            foreach (var item in items.AsEnumerable().Reverse())
            {
                _undoStack.Push(item);
            }
        }

        StateChanged?.Invoke();
    }

    /// <summary>
    /// Restores the previous state (undo).
    /// </summary>
    public PipelineSnapshot? Undo(IEnumerable<ConnectorNode> currentNodes, IEnumerable<PipelineLink> currentLinks)
    {
        if (!CanUndo) return null;

        // Save current state to redo stack
        var currentSnapshot = new PipelineSnapshot
        {
            Description = "Current state",
            Timestamp = DateTime.UtcNow,
            Nodes = currentNodes.Select(SerializeNode).ToList(),
            Links = currentLinks.Select(SerializeLink).ToList()
        };
        _redoStack.Push(currentSnapshot);

        var previous = _undoStack.Pop();
        StateChanged?.Invoke();
        return previous;
    }

    /// <summary>
    /// Restores the next state (redo).
    /// </summary>
    public PipelineSnapshot? Redo(IEnumerable<ConnectorNode> currentNodes, IEnumerable<PipelineLink> currentLinks)
    {
        if (!CanRedo) return null;

        // Save current state to undo stack
        var currentSnapshot = new PipelineSnapshot
        {
            Description = "Current state",
            Timestamp = DateTime.UtcNow,
            Nodes = currentNodes.Select(SerializeNode).ToList(),
            Links = currentLinks.Select(SerializeLink).ToList()
        };
        _undoStack.Push(currentSnapshot);

        var next = _redoStack.Pop();
        StateChanged?.Invoke();
        return next;
    }

    /// <summary>
    /// Copies nodes to the clipboard.
    /// </summary>
    public void CopyNodes(IEnumerable<ConnectorNode> nodes)
    {
        var nodeList = nodes.ToList();
        _clipboard.Clear();

        foreach (var node in nodeList)
        {
            _clipboard.Add(new ClipboardNode
            {
                ConnectorType = node.ConnectorType,
                Label = node.Label,
                Config = new Dictionary<string, string>(node.Config),
                RelativeX = 0,
                RelativeY = 0,
                SubPipelineId = node.SubPipelineId,
                SubPipelineName = node.SubPipelineName
            });
        }

        // Calculate relative positions from first node
        if (_clipboard.Count > 1)
        {
            var firstNode = nodeList[0];
            var baseX = firstNode.Position?.X ?? 0;
            var baseY = firstNode.Position?.Y ?? 0;

            for (var i = 0; i < nodeList.Count; i++)
            {
                _clipboard[i].RelativeX = (nodeList[i].Position?.X ?? 0) - baseX;
                _clipboard[i].RelativeY = (nodeList[i].Position?.Y ?? 0) - baseY;
            }
        }

        StateChanged?.Invoke();
    }

    /// <summary>
    /// Cuts nodes (copies and marks for deletion).
    /// </summary>
    public void CutNodes(IEnumerable<ConnectorNode> nodes)
    {
        CopyNodes(nodes);
        // Caller is responsible for deleting the nodes
    }

    /// <summary>
    /// Gets nodes from clipboard for pasting.
    /// </summary>
    public IReadOnlyList<ClipboardNode> GetClipboardNodes() => _clipboard.AsReadOnly();

    /// <summary>
    /// Clears all state.
    /// </summary>
    public void Clear()
    {
        _undoStack.Clear();
        _redoStack.Clear();
        _clipboard.Clear();
        StateChanged?.Invoke();
    }

    private static SerializedNode SerializeNode(ConnectorNode node) => new()
    {
        NodeId = node.NodeId,
        ConnectorType = node.ConnectorType,
        Label = node.Label,
        X = node.Position?.X ?? 0,
        Y = node.Position?.Y ?? 0,
        Config = new Dictionary<string, string>(node.Config),
        IsSource = node.IsSource,
        IsSink = node.IsSink,
        SubPipelineId = node.SubPipelineId,
        SubPipelineName = node.SubPipelineName
    };

    private static SerializedLink SerializeLink(PipelineLink link) => new()
    {
        ConnectionId = link.ConnectionId,
        SourceNodeId = (link.Source?.Model as ConnectorNode)?.NodeId ?? "",
        TargetNodeId = (link.Target?.Model as ConnectorNode)?.NodeId ?? "",
        InternalTopic = link.InternalTopic
    };
}

/// <summary>
/// Represents a snapshot of the pipeline state.
/// </summary>
public sealed class PipelineSnapshot
{
    public required string Description { get; init; }
    public DateTime Timestamp { get; init; }
    public required List<SerializedNode> Nodes { get; init; }
    public required List<SerializedLink> Links { get; init; }
}

/// <summary>
/// Serialized node for undo/redo.
/// </summary>
public sealed class SerializedNode
{
    public required string NodeId { get; init; }
    public required string ConnectorType { get; init; }
    public string? Label { get; init; }
    public double X { get; init; }
    public double Y { get; init; }
    public required Dictionary<string, string> Config { get; init; }
    public bool IsSource { get; init; }
    public bool IsSink { get; init; }
    public string? SubPipelineId { get; init; }
    public string? SubPipelineName { get; init; }
}

/// <summary>
/// Serialized link for undo/redo.
/// </summary>
public sealed class SerializedLink
{
    public required string ConnectionId { get; init; }
    public required string SourceNodeId { get; init; }
    public required string TargetNodeId { get; init; }
    public string? InternalTopic { get; init; }
}

/// <summary>
/// Node data in clipboard.
/// </summary>
public sealed class ClipboardNode
{
    public required string ConnectorType { get; init; }
    public string? Label { get; init; }
    public required Dictionary<string, string> Config { get; init; }
    public double RelativeX { get; set; }
    public double RelativeY { get; set; }
    public string? SubPipelineId { get; init; }
    public string? SubPipelineName { get; init; }
}
