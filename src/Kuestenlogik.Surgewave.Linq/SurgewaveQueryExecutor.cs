using System.Linq.Expressions;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Linq;

/// <summary>
/// Executes LINQ queries by scanning Surgewave topic partitions,
/// applying Where/Select/Take/Skip in-process after deserialization.
/// </summary>
internal sealed class SurgewaveQueryExecutor
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly object _source;

    public SurgewaveQueryExecutor(object source)
    {
        _source = source;
    }

    public TResult Execute<TResult>(Expression expression)
    {
        var sourceType = _source.GetType();
        if (!sourceType.IsGenericType)
            throw new InvalidOperationException("Query source must be TopicQuerySource<T>.");

        var elementType = sourceType.GetGenericArguments()[0];
        var method = typeof(SurgewaveQueryExecutor)
            .GetMethod(nameof(ExecuteTyped), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(elementType);

        var result = method.Invoke(this, [expression]);
        return (TResult)result!;
    }

    private object ExecuteTyped<T>(Expression expression) where T : class
    {
        var source = (TopicQuerySource<T>)_source;

        // Parse expression tree
        var visitor = new SurgewaveExpressionVisitor();
        visitor.Visit(expression);

        // Build combined predicate
        Func<T, bool>? predicate = null;
        if (visitor.WherePredicates.Count > 0)
        {
            var compiledPredicates = visitor.WherePredicates
                .Select(p => (Func<T, bool>)p.Compile())
                .ToList();
            predicate = item => compiledPredicates.All(p => p(item));
        }

        // Scan topic
        var results = ScanTopic(source, predicate, visitor.TakeCount, visitor.SkipCount);

        // Apply Select projection
        if (visitor.SelectProjection != null)
        {
            var compiled = visitor.SelectProjection.Compile();
            results = results.Select(item => (T)compiled.DynamicInvoke(item)!);
        }

        // Handle terminal operations
        if (visitor.IsCount)
            return results.Count();
        if (visitor.IsAny)
            return results.Any();
        if (visitor.IsFirst)
            return results.First()!;
        if (visitor.IsFirstOrDefault)
            return results.FirstOrDefault()!;
        if (visitor.IsSingle)
            return results.Single()!;

        return results.ToList();
    }

    private static IEnumerable<T> ScanTopic<T>(
        TopicQuerySource<T> source,
        Func<T, bool>? predicate,
        int? take,
        int? skip) where T : class
    {
        var options = source.Options;
        var (host, port) = ParseBootstrapServers(options.BootstrapServers);
        var client = new SurgewaveNativeClient(host, port);
        client.ConnectAsync().GetAwaiter().GetResult();

        try
        {
            // Determine partitions to scan
            var partitions = source.Partition.HasValue
                ? [source.Partition.Value]
                : GetPartitions(client, source.Topic);

            var maxMessages = options.MaxScanMessages;
            var yielded = 0;
            var skipped = 0;
            var totalLimit = take.HasValue ? (skip ?? 0) + take.Value : int.MaxValue;

            foreach (var partition in partitions)
            {
                var offset = source.FromOffset;
                var messagesRead = 0;

                while ((maxMessages < 0 || messagesRead < maxMessages) && yielded < totalLimit)
                {
                    var response = client.Messaging
                        .ReceiveAsync(source.Topic, partition, offset)
                        .GetAwaiter().GetResult();

                    if (response.Messages.Count == 0)
                        break;

                    foreach (var msg in response.Messages)
                    {
                        messagesRead++;
                        offset = msg.Offset + 1;

                        if (msg.Value.Length == 0)
                            continue;

                        T? item;
                        try
                        {
                            item = JsonSerializer.Deserialize<T>(msg.Value, JsonOptions);
                        }
                        catch
                        {
                            continue;
                        }

                        if (item == null) continue;
                        if (predicate != null && !predicate(item)) continue;

                        if (skip.HasValue && skipped < skip.Value)
                        {
                            skipped++;
                            continue;
                        }

                        yield return item;
                        yielded++;

                        if (take.HasValue && yielded >= take.Value)
                            yield break;
                    }

                    if (source.ToOffset.HasValue && offset >= source.ToOffset.Value)
                        break;
                }
            }
        }
        finally
        {
            client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    private static int[] GetPartitions(SurgewaveNativeClient client, string topic)
    {
        var description = client.Topics.DescribeAsync(topic)
            .GetAwaiter().GetResult();
        return Enumerable.Range(0, description.PartitionCount).ToArray();
    }

    private static (string Host, int Port) ParseBootstrapServers(string bootstrapServers)
    {
        var parts = bootstrapServers.Split(':');
        var host = parts[0];
        var port = parts.Length > 1 && int.TryParse(parts[1], out var p) ? p : 9092;
        return (host, port);
    }
}
