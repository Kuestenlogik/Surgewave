using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Core.Lineage;

/// <summary>
/// Exports a <see cref="LineageGraph"/> to standard visualization and interchange formats.
/// </summary>
public static class LineageExporter
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };

    /// <summary>
    /// Exports the graph as a Graphviz DOT language string.
    /// </summary>
    public static string ToDot(LineageGraph graph)
    {
        ArgumentNullException.ThrowIfNull(graph);

        var sb = new StringBuilder();
        sb.AppendLine("digraph Lineage {");
        sb.AppendLine("  rankdir=LR;");
        sb.AppendLine();

        foreach (var node in graph.Nodes)
        {
            var shape = node.Type switch
            {
                LineageNodeType.Topic => "cylinder",
                LineageNodeType.Producer => "box",
                LineageNodeType.Consumer => "box",
                LineageNodeType.StreamsApp => "hexagon",
                LineageNodeType.Connector => "parallelogram",
                _ => "ellipse"
            };

            var sanitizedId = SanitizeDotId(node.Id);
            var label = EscapeDotLabel(node.Name);
            sb.AppendLine(CultureInfo.InvariantCulture, $"  {sanitizedId} [label=\"{label}\" shape={shape}];");
        }

        sb.AppendLine();

        foreach (var edge in graph.Edges)
        {
            var sourceId = SanitizeDotId(edge.SourceId);
            var targetId = SanitizeDotId(edge.TargetId);
            var label = EscapeDotLabel(edge.Type.ToString());
            sb.AppendLine(CultureInfo.InvariantCulture, $"  {sourceId} -> {targetId} [label=\"{label}\"];");
        }

        sb.AppendLine("}");
        return sb.ToString();
    }

    /// <summary>
    /// Exports the graph as a Mermaid diagram string suitable for Markdown rendering.
    /// </summary>
    public static string ToMermaid(LineageGraph graph)
    {
        ArgumentNullException.ThrowIfNull(graph);

        var sb = new StringBuilder();
        sb.AppendLine("graph LR");

        foreach (var node in graph.Nodes)
        {
            var sanitizedId = SanitizeMermaidId(node.Id);
            var (open, close) = node.Type switch
            {
                LineageNodeType.Topic => ("[(", ")]"),
                LineageNodeType.Producer => ("[", "]"),
                LineageNodeType.Consumer => ("[", "]"),
                LineageNodeType.StreamsApp => ("{{", "}}"),
                LineageNodeType.Connector => ("[/", "/]"),
                _ => ("(", ")")
            };

            var label = EscapeMermaidLabel(node.Name);
            sb.AppendLine(CultureInfo.InvariantCulture, $"  {sanitizedId}{open}\"{label}\"{close}");
        }

        sb.AppendLine();

        foreach (var edge in graph.Edges)
        {
            var sourceId = SanitizeMermaidId(edge.SourceId);
            var targetId = SanitizeMermaidId(edge.TargetId);
            var label = edge.Type.ToString();
            sb.AppendLine(CultureInfo.InvariantCulture, $"  {sourceId} -->|\"{label}\"| {targetId}");
        }

        return sb.ToString();
    }

    /// <summary>
    /// Exports the graph as a JSON string.
    /// </summary>
    public static string ToJson(LineageGraph graph)
    {
        ArgumentNullException.ThrowIfNull(graph);
        return JsonSerializer.Serialize(graph, JsonOptions);
    }

    /// <summary>
    /// Replaces characters that are invalid in DOT identifiers with underscores.
    /// </summary>
    private static string SanitizeDotId(string id) =>
        id.Replace(':', '_').Replace('-', '_').Replace('.', '_');

    /// <summary>
    /// Escapes characters that are special in DOT label strings.
    /// </summary>
    private static string EscapeDotLabel(string label) =>
        label.Replace("\"", "\\\"");

    /// <summary>
    /// Replaces characters that are invalid in Mermaid node identifiers with underscores.
    /// </summary>
    private static string SanitizeMermaidId(string id) =>
        id.Replace(':', '_').Replace('-', '_').Replace('.', '_');

    /// <summary>
    /// Escapes characters that are special in Mermaid label strings.
    /// </summary>
    private static string EscapeMermaidLabel(string label) =>
        label.Replace("\"", "#quot;");
}
