namespace Kuestenlogik.Surgewave.Core.Lineage;

/// <summary>
/// The type of edge in a lineage graph, describing the relationship between two nodes.
/// </summary>
public enum LineageEdgeType
{
    /// <summary>Producer writes to a Topic.</summary>
    Produces,

    /// <summary>Topic is read by a Consumer.</summary>
    Consumes,

    /// <summary>Topic is read by a StreamsApp (source).</summary>
    StreamsFrom,

    /// <summary>StreamsApp writes to a Topic (sink).</summary>
    StreamsTo,

    /// <summary>External source feeds into a Connector.</summary>
    ConnectsFrom,

    /// <summary>Connector writes to a Topic.</summary>
    ConnectsTo
}
