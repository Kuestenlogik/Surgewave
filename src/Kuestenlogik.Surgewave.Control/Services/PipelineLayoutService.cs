using Microsoft.Msagl.Core.Geometry;
using Microsoft.Msagl.Core.Geometry.Curves;
using Microsoft.Msagl.Core.Layout;
using Microsoft.Msagl.Layout.Layered;
using Kuestenlogik.Surgewave.Control.Components.Pipeline;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Layout direction for hierarchical pipeline layout.
/// </summary>
public enum LayoutDirection
{
    TopToBottom,
    LeftToRight
}

/// <summary>
/// Result of a layout computation with node positions.
/// </summary>
public sealed record LayoutResult(Dictionary<string, (double X, double Y)> Positions);

/// <summary>
/// Wraps Microsoft.Msagl Sugiyama layout engine for hierarchical pipeline layout.
/// </summary>
public sealed class PipelineLayoutService
{
    private const double NodeWidth = 180;
    private const double NodeHeight = 60;
    private const double NodeSeparation = 40;
    private const double LayerSeparation = 80;
    private const double Padding = 50;

    /// <summary>
    /// Computes hierarchical layout for pipeline nodes and links.
    /// </summary>
    public LayoutResult ComputeLayout(
        IReadOnlyList<ConnectorNode> nodes,
        IReadOnlyList<(string SourceId, string TargetId)> links,
        LayoutDirection direction = LayoutDirection.TopToBottom)
    {
        if (nodes.Count == 0)
            return new LayoutResult(new Dictionary<string, (double, double)>());

        var graph = new GeometryGraph();
        var nodeMap = new Dictionary<string, Node>();

        // Create MSAGL nodes
        foreach (var node in nodes)
        {
            var msaglNode = new Node(
                CurveFactory.CreateRectangle(NodeWidth, NodeHeight, new Point(0, 0)),
                node.NodeId);
            graph.Nodes.Add(msaglNode);
            nodeMap[node.NodeId] = msaglNode;
        }

        // Create edges
        foreach (var (sourceId, targetId) in links)
        {
            if (nodeMap.TryGetValue(sourceId, out var sourceNode) &&
                nodeMap.TryGetValue(targetId, out var targetNode))
            {
                var edge = new Edge(sourceNode, targetNode);
                graph.Edges.Add(edge);
            }
        }

        // Configure Sugiyama layout
        var settings = new SugiyamaLayoutSettings
        {
            NodeSeparation = NodeSeparation,
            LayerSeparation = LayerSeparation
        };

        // For left-to-right, apply rotation
        if (direction == LayoutDirection.LeftToRight)
        {
            settings.Transformation = PlaneTransformation.Rotation(Math.PI / 2);
        }

        // Run layout
        var layout = new LayeredLayout(graph, settings);
        layout.Run();

        // Extract positions and normalize
        return ExtractPositions(graph, nodeMap);
    }

    /// <summary>
    /// Computes hierarchical layout with group/cluster awareness.
    /// Falls back to flat layout if clustering fails.
    /// </summary>
    public LayoutResult ComputeLayoutWithGroups(
        IReadOnlyList<ConnectorNode> nodes,
        IReadOnlyList<(string SourceId, string TargetId)> links,
        IReadOnlyList<(string GroupName, IReadOnlyList<string> NodeIds)> groups,
        LayoutDirection direction = LayoutDirection.TopToBottom)
    {
        if (nodes.Count == 0)
            return new LayoutResult(new Dictionary<string, (double, double)>());

        try
        {
            var graph = new GeometryGraph();
            var nodeMap = new Dictionary<string, Node>();

            // Create all MSAGL nodes first
            foreach (var node in nodes)
            {
                var msaglNode = new Node(
                    CurveFactory.CreateRectangle(NodeWidth, NodeHeight, new Point(0, 0)),
                    node.NodeId);
                nodeMap[node.NodeId] = msaglNode;
            }

            // Group assignment tracking
            var assignedNodes = new HashSet<string>();

            // Create clusters for groups
            foreach (var (groupName, nodeIds) in groups)
            {
                var cluster = new Cluster();
                foreach (var nodeId in nodeIds)
                {
                    if (nodeMap.TryGetValue(nodeId, out var msaglNode))
                    {
                        cluster.AddChild(msaglNode);
                        assignedNodes.Add(nodeId);
                    }
                }
                graph.RootCluster.AddChild(cluster);
            }

            // Add ungrouped nodes to root
            foreach (var node in nodes)
            {
                if (!assignedNodes.Contains(node.NodeId))
                {
                    graph.RootCluster.AddChild(nodeMap[node.NodeId]);
                }
            }

            // Create edges
            foreach (var (sourceId, targetId) in links)
            {
                if (nodeMap.TryGetValue(sourceId, out var sourceNode) &&
                    nodeMap.TryGetValue(targetId, out var targetNode))
                {
                    var edge = new Edge(sourceNode, targetNode);
                    graph.Edges.Add(edge);
                }
            }

            // Configure layout
            var settings = new SugiyamaLayoutSettings
            {
                NodeSeparation = NodeSeparation,
                LayerSeparation = LayerSeparation
            };

            if (direction == LayoutDirection.LeftToRight)
            {
                settings.Transformation = PlaneTransformation.Rotation(Math.PI / 2);
            }

            var layout = new LayeredLayout(graph, settings);
            layout.Run();

            return ExtractPositions(graph, nodeMap);
        }
        catch
        {
            // Fallback to flat layout if clustering fails
            return ComputeLayout(nodes, links, direction);
        }
    }

    private static LayoutResult ExtractPositions(GeometryGraph graph, Dictionary<string, Node> nodeMap)
    {
        var positions = new Dictionary<string, (double X, double Y)>();

        // Find bounds for normalization
        double minX = double.MaxValue, minY = double.MaxValue;

        foreach (var (nodeId, msaglNode) in nodeMap)
        {
            // MSAGL uses center coordinates; convert to top-left
            var x = msaglNode.Center.X - NodeWidth / 2;
            var y = msaglNode.Center.Y - NodeHeight / 2;

            if (x < minX) minX = x;
            if (y < minY) minY = y;

            positions[nodeId] = (x, y);
        }

        // Normalize: shift so min is at Padding, and flip Y axis (MSAGL Y-up → Browser Y-down)
        double maxY = double.MinValue;
        foreach (var (nodeId, (x, y)) in positions)
        {
            if (y > maxY) maxY = y;
        }

        var normalized = new Dictionary<string, (double X, double Y)>();
        foreach (var (nodeId, (x, y)) in positions)
        {
            // Flip Y: maxY - y gives browser-style top-down coordinates
            var nx = x - minX + Padding;
            var ny = (maxY - y) + Padding;
            normalized[nodeId] = (nx, ny);
        }

        return new LayoutResult(normalized);
    }
}
