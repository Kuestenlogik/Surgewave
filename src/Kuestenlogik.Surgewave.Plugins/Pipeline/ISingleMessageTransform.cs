using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Plugins.Pipeline;

/// <summary>
/// Lightweight inline transform applied on connections between nodes.
/// SMTs do not create their own task or topic — they run inline
/// within the connection pipeline.
/// </summary>
public interface ISingleMessageTransform : IPlugin
{
    /// <summary>
    /// Applies the transform to a single record.
    /// </summary>
    /// <param name="key">The record key (may be null).</param>
    /// <param name="value">The record value.</param>
    /// <param name="headers">The record headers.</param>
    /// <returns>The transformed record, or null to filter/drop the record.</returns>
    (byte[]? Key, byte[] Value, IDictionary<string, string>? Headers)? Apply(
        byte[]? key,
        byte[] value,
        IDictionary<string, string>? headers);

    /// <summary>
    /// Configuration schema for this transform.
    /// </summary>
    ConfigDef Config { get; }

    /// <summary>
    /// Configures the transform with the provided settings.
    /// </summary>
    void Configure(IDictionary<string, string> config);
}
