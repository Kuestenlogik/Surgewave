namespace Kuestenlogik.Surgewave.Core.Transforms;

/// <summary>
/// Inline transform that executes in the broker's produce/fetch path.
/// Implementations can be C# plugins or WASM modules.
/// </summary>
public interface IInlineTransform : IDisposable
{
    /// <summary>
    /// Unique name identifying this transform.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Initializes the transform with configuration key-value pairs.
    /// Called once after the transform is loaded.
    /// </summary>
    void Initialize(IReadOnlyDictionary<string, string> config);

    /// <summary>
    /// Transforms a single record. Called on the produce or fetch hot path.
    /// Must be thread-safe.
    /// </summary>
    TransformResult Transform(TransformContext context);
}
