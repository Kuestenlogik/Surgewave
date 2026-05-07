using Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;
using Kuestenlogik.Surgewave.Client.Native.Operations.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Extensions;

/// <summary>
/// Extension methods for conditional builder configuration.
/// </summary>
public static class BuilderExtensions
{
    /// <summary>
    /// Apply configuration only when condition is true.
    /// </summary>
    public static SendBuilder When(this SendBuilder builder, bool condition, Action<SendBuilder> configure)
    {
        if (condition) configure(builder);
        return builder;
    }

    /// <summary>
    /// Apply configuration only when condition is true.
    /// </summary>
    public static ReceiveBuilder When(this ReceiveBuilder builder, bool condition, Action<ReceiveBuilder> configure)
    {
        if (condition) configure(builder);
        return builder;
    }

    /// <summary>
    /// Apply configuration only when condition is true.
    /// </summary>
    public static TopicCreateBuilder When(this TopicCreateBuilder builder, bool condition, Action<TopicCreateBuilder> configure)
    {
        if (condition) configure(builder);
        return builder;
    }

    /// <summary>
    /// Apply configuration only when condition is true.
    /// </summary>
    public static SchemaBuilder When(this SchemaBuilder builder, bool condition, Action<SchemaBuilder> configure)
    {
        if (condition) configure(builder);
        return builder;
    }

    /// <summary>
    /// Apply configuration only when condition is true.
    /// </summary>
    public static JoinGroupBuilder When(this JoinGroupBuilder builder, bool condition, Action<JoinGroupBuilder> configure)
    {
        if (condition) configure(builder);
        return builder;
    }

    /// <summary>
    /// Apply configuration only when condition is true.
    /// </summary>
    public static TBuilder When<TBuilder>(this TBuilder builder, bool condition, Action<TBuilder> configure)
        where TBuilder : class
    {
        if (condition) configure(builder);
        return builder;
    }

    /// <summary>
    /// Apply configuration only when condition is true (async version).
    /// </summary>
    public static async Task<TBuilder> WhenAsync<TBuilder>(this TBuilder builder, bool condition, Func<TBuilder, Task> configure)
        where TBuilder : class
    {
        if (condition) await configure(builder);
        return builder;
    }

    /// <summary>
    /// Apply one of two configurations based on condition.
    /// </summary>
    public static TBuilder WhenElse<TBuilder>(this TBuilder builder, bool condition, Action<TBuilder> whenTrue, Action<TBuilder> whenFalse)
        where TBuilder : class
    {
        if (condition)
            whenTrue(builder);
        else
            whenFalse(builder);
        return builder;
    }
}
