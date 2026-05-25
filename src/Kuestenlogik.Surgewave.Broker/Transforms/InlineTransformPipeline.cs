using Kuestenlogik.Surgewave.Core.Transforms;

namespace Kuestenlogik.Surgewave.Broker.Transforms;

/// <summary>
/// Chains multiple inline transforms for a specific topic/phase combination.
/// Immutable once constructed — replace the entire pipeline on config changes.
/// Thread-safe by design (all state is readonly after construction).
/// </summary>
public sealed class InlineTransformPipeline
{
    /// <summary>
    /// A shared empty pipeline instance that performs no transformations.
    /// </summary>
    public static readonly InlineTransformPipeline Empty = new([]);

    private readonly IInlineTransform[] _transforms;

    public InlineTransformPipeline(IReadOnlyList<IInlineTransform> transforms)
    {
        _transforms = [.. transforms];
    }

    /// <summary>
    /// Number of transforms in this pipeline.
    /// </summary>
    public int Count => _transforms.Length;

    /// <summary>
    /// Whether this pipeline has any transforms.
    /// </summary>
    public bool IsEmpty => _transforms.Length == 0;

    /// <summary>
    /// Executes the transform chain in order. Short-circuits on Drop.
    /// For Route results, the RouteTopic is carried forward through subsequent transforms.
    /// </summary>
    public TransformResult Execute(TransformContext context)
    {
        if (_transforms.Length == 0)
        {
            return TransformResult.Pass(context.Key, context.Value, context.Headers);
        }

        var currentKey = context.Key;
        var currentValue = context.Value;
        var currentHeaders = context.Headers;
        string? routeTopic = null;

        for (var i = 0; i < _transforms.Length; i++)
        {
            var ctx = new TransformContext
            {
                Topic = routeTopic ?? context.Topic,
                Partition = context.Partition,
                Key = currentKey,
                Value = currentValue,
                Headers = currentHeaders,
                Timestamp = context.Timestamp,
                Phase = context.Phase
            };

            var result = _transforms[i].Transform(ctx);

            if (result.Dropped)
            {
                return TransformResult.Drop();
            }

            currentKey = result.Key;
            currentValue = result.Value;
            currentHeaders = result.Headers ?? currentHeaders;

            if (result.RouteTopic != null)
            {
                routeTopic = result.RouteTopic;
            }
        }

        return routeTopic != null
            ? TransformResult.Route(routeTopic, currentKey, currentValue, currentHeaders)
            : TransformResult.Pass(currentKey, currentValue, currentHeaders);
    }
}
