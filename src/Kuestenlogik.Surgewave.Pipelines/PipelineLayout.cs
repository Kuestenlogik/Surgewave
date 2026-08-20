namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// Assigns visual editor positions to nodes: left-to-right by graph depth, stacked vertically
/// within a depth level. Purely cosmetic — the layout makes DSL-built pipelines open cleanly
/// in the visual pipeline editor.
/// </summary>
internal static class PipelineLayout
{
    private const double OriginX = 120;
    private const double OriginY = 160;
    private const double ColumnWidth = 260;
    private const double RowHeight = 140;

    public static IReadOnlyDictionary<string, (double X, double Y)> Assign(
        IReadOnlyList<NodeDraft> nodes,
        IReadOnlyList<(string SourceId, string TargetId, bool Error)> connections)
    {
        var depths = new Dictionary<string, int>(StringComparer.Ordinal);
        var incoming = connections.ToLookup(c => c.TargetId, c => c.SourceId);

        foreach (var node in nodes)
        {
            ComputeDepth(node.Id);
        }

        var positions = new Dictionary<string, (double X, double Y)>(StringComparer.Ordinal);
        var laneCounts = new Dictionary<int, int>();

        foreach (var node in nodes)
        {
            var depth = depths[node.Id];
            var lane = laneCounts.GetValueOrDefault(depth);
            laneCounts[depth] = lane + 1;
            positions[node.Id] = (OriginX + depth * ColumnWidth, OriginY + lane * RowHeight);
        }

        return positions;

        int ComputeDepth(string id)
        {
            if (depths.TryGetValue(id, out var depth))
            {
                return depth;
            }

            depths[id] = 0; // breaks ties if a cycle slipped through
            var sources = incoming[id].ToList();
            var computed = sources.Count == 0 ? 0 : sources.Max(ComputeDepth) + 1;
            depths[id] = computed;
            return computed;
        }
    }
}
