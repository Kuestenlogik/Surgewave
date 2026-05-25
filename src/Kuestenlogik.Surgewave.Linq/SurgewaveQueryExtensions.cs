namespace Kuestenlogik.Surgewave.Linq;

/// <summary>
/// Async extension methods for Surgewave LINQ queries.
/// </summary>
public static class SurgewaveQueryExtensions
{
    /// <summary>
    /// Executes the query and returns all results as a list.
    /// </summary>
    public static Task<List<T>> ToListAsync<T>(this IQueryable<T> queryable, CancellationToken ct = default)
        => Task.Run(() => queryable.ToList(), ct);

    /// <summary>
    /// Executes the query and returns the first matching result.
    /// </summary>
    public static Task<T> FirstAsync<T>(this IQueryable<T> queryable, CancellationToken ct = default)
        => Task.Run(() => queryable.First(), ct);

    /// <summary>
    /// Executes the query and returns the first matching result, or default if none.
    /// </summary>
    public static Task<T?> FirstOrDefaultAsync<T>(this IQueryable<T> queryable, CancellationToken ct = default)
        => Task.Run(() => queryable.FirstOrDefault(), ct);

    /// <summary>
    /// Executes the query and returns the count of matching results.
    /// </summary>
    public static Task<int> CountAsync<T>(this IQueryable<T> queryable, CancellationToken ct = default)
        => Task.Run(() => queryable.Count(), ct);

    /// <summary>
    /// Executes the query and returns whether any results match.
    /// </summary>
    public static Task<bool> AnyAsync<T>(this IQueryable<T> queryable, CancellationToken ct = default)
        => Task.Run(() => queryable.Any(), ct);
}
