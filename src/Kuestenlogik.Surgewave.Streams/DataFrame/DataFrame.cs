using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Streams.DataFrame;

/// <summary>
/// A distributed collection of data organized into named columns.
/// Provides a Spark DataFrame-style API for stream processing.
/// </summary>
public interface IDataFrame : IEnumerable<Row>
{
    /// <summary>
    /// The schema of this DataFrame.
    /// </summary>
    StructType Schema { get; }

    /// <summary>
    /// The column names.
    /// </summary>
    IReadOnlyList<string> Columns { get; }

    // Schema operations
    IDataFrame PrintSchema();

    // Selection operations
    IDataFrame Select(params string[] columns);
    IDataFrame Select(params Column[] columns);
    IDataFrame SelectExpr(params string[] expressions);
    IDataFrame Drop(params string[] columns);

    // Filtering
    IDataFrame Filter(Column condition);
    IDataFrame Filter(Func<Row, bool> predicate);
    IDataFrame Where(Column condition);
    IDataFrame Where(Func<Row, bool> predicate);

    // Column operations
    IDataFrame WithColumn(string name, Column column);
    IDataFrame WithColumn(string name, Func<Row, object?> expression);
    IDataFrame WithColumnRenamed(string existing, string newName);

    // Null handling
    IDataFrame Na();
    IDataFrame DropNulls();
    IDataFrame DropNulls(params string[] columns);
    IDataFrame FillNulls(object value);
    IDataFrame FillNulls(Dictionary<string, object> values);
    IDataFrame Replace<T>(T toReplace, T replacement);

    // Sorting
    IDataFrame Sort(params string[] columns);
    IDataFrame Sort(params Column[] columns);
    IDataFrame OrderBy(params string[] columns);
    IDataFrame OrderBy(params Column[] columns);

    // Grouping
    IGroupedDataFrame GroupBy(params string[] columns);
    IGroupedDataFrame GroupBy(params Column[] columns);

    // Aggregation (without grouping)
    IDataFrame Agg(params Column[] aggregates);

    // Joins
    IDataFrame Join(IDataFrame other, string column);
    IDataFrame Join(IDataFrame other, params string[] columns);
    IDataFrame Join(IDataFrame other, Column condition);
    IDataFrame Join(IDataFrame other, Column condition, string joinType);
    IDataFrame CrossJoin(IDataFrame other);

    // Set operations
    IDataFrame Union(IDataFrame other);
    IDataFrame UnionAll(IDataFrame other);
    IDataFrame UnionByName(IDataFrame other);
    IDataFrame Intersect(IDataFrame other);
    IDataFrame Except(IDataFrame other);
    IDataFrame Distinct();
    IDataFrame DropDuplicates(params string[] columns);

    // Limiting
    IDataFrame Limit(int n);
    IDataFrame Take(int n);
    Row? First();
    Row? Head();
    IReadOnlyList<Row> Head(int n);
    IReadOnlyList<Row> Tail(int n);
    IReadOnlyList<Row> Collect();

    // Statistics
    long Count();
    IDataFrame Describe(params string[] columns);
    IDataFrame Summary(params string[] statistics);

    // Sampling
    IDataFrame Sample(double fraction, bool withReplacement = false, long? seed = null);

    // Explode/Flatten
    IDataFrame Explode(string column);
    IDataFrame ExplodeOuter(string column);

    // Pivot
    IGroupedDataFrame Rollup(params string[] columns);
    IGroupedDataFrame Cube(params string[] columns);

    // Output
    void Show(int numRows = 20, bool truncate = true);
    IDataFrame Cache();
    IDataFrame Persist();
    IDataFrame Coalesce(int numPartitions);
    IDataFrame Repartition(int numPartitions);
    IDataFrame Repartition(int numPartitions, params Column[] columns);

    // Convert to other types
    IStream<Row, Row> ToStream();
    IEnumerable<T> As<T>() where T : new();

    // Write operations
    DataFrameWriter Write();
}

/// <summary>
/// A grouped DataFrame for aggregation operations.
/// </summary>
public interface IGroupedDataFrame
{
    IDataFrame Count();
    IDataFrame Sum(params string[] columns);
    IDataFrame Avg(params string[] columns);
    IDataFrame Mean(params string[] columns);
    IDataFrame Min(params string[] columns);
    IDataFrame Max(params string[] columns);
    IDataFrame Agg(params Column[] aggregates);
    IDataFrame Pivot(string pivotColumn);
    IDataFrame Pivot(string pivotColumn, params object[] values);
}

/// <summary>
/// Writer for DataFrame output.
/// </summary>
public sealed class DataFrameWriter
{
    private readonly IDataFrame _df;
    private string _format = "json";
    private string _mode = "append";
    private readonly Dictionary<string, string> _options = new();
    private readonly List<string> _partitionBy = new();

    internal DataFrameWriter(IDataFrame df)
    {
        _df = df;
    }

    public DataFrameWriter Format(string format)
    {
        _format = format;
        return this;
    }

    public DataFrameWriter Mode(string mode)
    {
        _mode = mode;
        return this;
    }

    public DataFrameWriter Option(string key, string value)
    {
        _options[key] = value;
        return this;
    }

    public DataFrameWriter Options(Dictionary<string, string> options)
    {
        foreach (var kv in options) _options[kv.Key] = kv.Value;
        return this;
    }

    public DataFrameWriter PartitionBy(params string[] columns)
    {
        _partitionBy.AddRange(columns);
        return this;
    }

    public void Json(string path) => Save(path, "json");
    public void Csv(string path) => Save(path, "csv");
    public void Parquet(string path) => Save(path, "parquet");

    public void Save(string path) => Save(path, _format);

    public void Save(string path, string format)
    {
        // In a real implementation, this would write to the storage system
        Console.WriteLine($"Writing {_df.Count()} rows to {path} as {format}");
    }

    /// <summary>
    /// Writes the DataFrame to a Surgewave topic.
    /// </summary>
    public void ToTopic(string topic)
    {
        Console.WriteLine($"Streaming {_df.Count()} rows to topic {topic}");
    }
}

/// <summary>
/// In-memory implementation of DataFrame.
/// </summary>
internal sealed class DataFrameImpl : IDataFrame
{
    private readonly List<Row> _rows;
    private readonly StructType _schema;
#pragma warning disable CS0414 // Field is assigned but never used
    private bool _cached;
#pragma warning restore CS0414

    public DataFrameImpl(IEnumerable<Row> rows, StructType schema)
    {
        _rows = rows.ToList();
        _schema = schema;
    }

    public StructType Schema => _schema;
    public IReadOnlyList<string> Columns => _schema.Fields.Select(f => f.Name).ToList();

    public IDataFrame PrintSchema()
    {
        Console.WriteLine(_schema.TreeString());
        return this;
    }

    public IDataFrame Select(params string[] columns)
    {
        var indices = columns.Select(c => _schema.FieldIndex(c)).ToArray();
        var newFields = indices.Select(i => _schema.Fields[i]).ToList();
        var newSchema = new StructType(newFields);

        var newRows = _rows.Select(row =>
        {
            var values = indices.Select(i => row[i]).ToArray();
            return new Row(values, newSchema);
        });

        return new DataFrameImpl(newRows, newSchema);
    }

    public IDataFrame Select(params Column[] columns)
    {
        // Simplified: just select by column name
        return Select(columns.Select(c => c.Name).ToArray());
    }

    public IDataFrame SelectExpr(params string[] expressions)
    {
        // Would need expression parser in full implementation
        return Select(expressions);
    }

    public IDataFrame Drop(params string[] columns)
    {
        var keepColumns = Columns.Except(columns).ToArray();
        return Select(keepColumns);
    }

    public IDataFrame Filter(Column condition)
    {
        // Simplified filter - in real impl would evaluate column expression
        return this;
    }

    public IDataFrame Filter(Func<Row, bool> predicate)
    {
        return new DataFrameImpl(_rows.Where(predicate), _schema);
    }

    public IDataFrame Where(Column condition) => Filter(condition);
    public IDataFrame Where(Func<Row, bool> predicate) => Filter(predicate);

    public IDataFrame WithColumn(string name, Column column)
    {
        var existingIndex = _schema.FieldIndex(name);
        StructType newSchema;
        Func<Row, object?[]> rowTransform;

        if (existingIndex >= 0)
        {
            newSchema = _schema;
            rowTransform = row =>
            {
                var values = row.ToArray();
                // In real impl would evaluate column expression
                return values;
            };
        }
        else
        {
            newSchema = _schema.Add(name, DataType.String);
            rowTransform = row =>
            {
                var values = row.ToList();
                values.Add(null);
                return values.ToArray();
            };
        }

        var newRows = _rows.Select(row => new Row(rowTransform(row), newSchema));
        return new DataFrameImpl(newRows, newSchema);
    }

    public IDataFrame WithColumn(string name, Func<Row, object?> expression)
    {
        var existingIndex = _schema.FieldIndex(name);
        StructType newSchema;

        if (existingIndex >= 0)
        {
            newSchema = _schema;
        }
        else
        {
            newSchema = _schema.Add(name, DataType.String);
        }

        var newRows = _rows.Select(row =>
        {
            var values = row.ToList();
            var newValue = expression(row);

            if (existingIndex >= 0)
            {
                values[existingIndex] = newValue;
            }
            else
            {
                values.Add(newValue);
            }

            return new Row(values.ToArray(), newSchema);
        });

        return new DataFrameImpl(newRows, newSchema);
    }

    public IDataFrame WithColumnRenamed(string existing, string newName)
    {
        var newFields = _schema.Fields.Select(f =>
            f.Name == existing ? new StructField(newName, f.DataType, f.Nullable, f.Metadata) : f
        ).ToList();
        var newSchema = new StructType(newFields);

        var newRows = _rows.Select(row => new Row(row.ToArray(), newSchema));
        return new DataFrameImpl(newRows, newSchema);
    }

    public IDataFrame Na() => this;
    public IDataFrame DropNulls() => Filter(row => !row.Any(v => v == null));
    public IDataFrame DropNulls(params string[] columns)
    {
        var indices = columns.Select(c => _schema.FieldIndex(c)).ToArray();
        return Filter(row => !indices.Any(i => row[i] == null));
    }

    public IDataFrame FillNulls(object value)
    {
        var newRows = _rows.Select(row =>
        {
            var values = row.Select(v => v ?? value).ToArray();
            return new Row(values, _schema);
        });
        return new DataFrameImpl(newRows, _schema);
    }

    public IDataFrame FillNulls(Dictionary<string, object> values)
    {
        var newRows = _rows.Select(row =>
        {
            var rowValues = row.ToArray();
            foreach (var (col, val) in values)
            {
                var idx = _schema.FieldIndex(col);
                if (idx >= 0 && rowValues[idx] == null)
                    rowValues[idx] = val;
            }
            return new Row(rowValues, _schema);
        });
        return new DataFrameImpl(newRows, _schema);
    }

    public IDataFrame Replace<T>(T toReplace, T replacement)
    {
        var newRows = _rows.Select(row =>
        {
            var values = row.Select(v => Equals(v, toReplace) ? (object?)replacement : v).ToArray();
            return new Row(values, _schema);
        });
        return new DataFrameImpl(newRows, _schema);
    }

    public IDataFrame Sort(params string[] columns) => OrderBy(columns);
    public IDataFrame Sort(params Column[] columns) => OrderBy(columns);

    public IDataFrame OrderBy(params string[] columns)
    {
        if (columns.Length == 0)
            return new DataFrameImpl(_rows, _schema);

        IOrderedEnumerable<Row>? ordered = null;
        foreach (var col in columns)
        {
            var idx = _schema.FieldIndex(col);
            if (ordered == null)
                ordered = _rows.OrderBy(r => r[idx]);
            else
                ordered = ordered.ThenBy(r => r[idx]);
        }
        return new DataFrameImpl(ordered!, _schema);
    }

    public IDataFrame OrderBy(params Column[] columns)
    {
        return OrderBy(columns.Select(c => c.Name).ToArray());
    }

    public IGroupedDataFrame GroupBy(params string[] columns)
    {
        return new GroupedDataFrameImpl(this, columns);
    }

    public IGroupedDataFrame GroupBy(params Column[] columns)
    {
        return GroupBy(columns.Select(c => c.Name).ToArray());
    }

    public IDataFrame Agg(params Column[] aggregates)
    {
        // Single row aggregation without grouping
        var values = new List<object?>();
        var fields = new List<StructField>();

        foreach (var agg in aggregates)
        {
            fields.Add(new StructField(agg.Name, DataType.Double));
            // Simplified: would need proper aggregate evaluation
            values.Add(_rows.Count);
        }

        var schema = new StructType(fields);
        return new DataFrameImpl([new Row(values.ToArray(), schema)], schema);
    }

    public IDataFrame Join(IDataFrame other, string column) => Join(other, [column]);

    public IDataFrame Join(IDataFrame other, params string[] columns)
    {
        return Join(other, columns, "inner");
    }

    public IDataFrame Join(IDataFrame other, Column condition)
    {
        return Join(other, condition, "inner");
    }

    public IDataFrame Join(IDataFrame other, Column condition, string joinType)
    {
        // Simplified cross join with filter
        return CrossJoin(other);
    }

    private IDataFrame Join(IDataFrame other, string[] columns, string joinType)
    {
        var leftIndices = columns.Select(c => _schema.FieldIndex(c)).ToArray();
        var rightIndices = columns.Select(c => other.Schema.FieldIndex(c)).ToArray();

        // Build new schema
        var newFields = _schema.Fields.ToList();
        foreach (var field in other.Schema.Fields)
        {
            if (!columns.Contains(field.Name))
                newFields.Add(field);
        }
        var newSchema = new StructType(newFields);

        var result = new List<Row>();
        foreach (var left in _rows)
        {
            foreach (var right in other)
            {
                bool match = true;
                for (int i = 0; i < columns.Length && match; i++)
                {
                    match = Equals(left[leftIndices[i]], right[rightIndices[i]]);
                }

                if (match)
                {
                    var values = left.ToList();
                    for (int i = 0; i < other.Schema.Fields.Count; i++)
                    {
                        if (!columns.Contains(other.Schema.Fields[i].Name))
                            values.Add(right[i]);
                    }
                    result.Add(new Row(values.ToArray(), newSchema));
                }
            }
        }

        return new DataFrameImpl(result, newSchema);
    }

    public IDataFrame CrossJoin(IDataFrame other)
    {
        var newFields = _schema.Fields.Concat(other.Schema.Fields).ToList();
        var newSchema = new StructType(newFields);

        var result = new List<Row>();
        foreach (var left in _rows)
        {
            foreach (var right in other)
            {
                var values = left.Concat(right).ToArray();
                result.Add(new Row(values, newSchema));
            }
        }

        return new DataFrameImpl(result, newSchema);
    }

    public IDataFrame Union(IDataFrame other) => UnionAll(other).Distinct();
    public IDataFrame UnionAll(IDataFrame other)
    {
        return new DataFrameImpl(_rows.Concat(other), _schema);
    }

    public IDataFrame UnionByName(IDataFrame other)
    {
        var newRows = other.Select(row =>
        {
            var values = _schema.Fields.Select(f =>
            {
                var idx = other.Schema.FieldIndex(f.Name);
                return idx >= 0 ? row[idx] : null;
            }).ToArray();
            return new Row(values, _schema);
        });
        return new DataFrameImpl(_rows.Concat(newRows), _schema);
    }

    public IDataFrame Intersect(IDataFrame other)
    {
        var otherSet = other.ToHashSet(RowComparer.Instance);
        return new DataFrameImpl(_rows.Where(r => otherSet.Contains(r)), _schema);
    }

    public IDataFrame Except(IDataFrame other)
    {
        var otherSet = other.ToHashSet(RowComparer.Instance);
        return new DataFrameImpl(_rows.Where(r => !otherSet.Contains(r)), _schema);
    }

    public IDataFrame Distinct()
    {
        return new DataFrameImpl(_rows.Distinct(RowComparer.Instance), _schema);
    }

    public IDataFrame DropDuplicates(params string[] columns)
    {
        if (columns.Length == 0) return Distinct();

        var indices = columns.Select(c => _schema.FieldIndex(c)).ToArray();
        var seen = new HashSet<string>();
        var result = new List<Row>();

        foreach (var row in _rows)
        {
            var key = string.Join("|", indices.Select(i => row[i]?.ToString() ?? ""));
            if (seen.Add(key))
                result.Add(row);
        }

        return new DataFrameImpl(result, _schema);
    }

    public IDataFrame Limit(int n) => new DataFrameImpl(_rows.Take(n), _schema);
    public IDataFrame Take(int n) => Limit(n);
    public Row? First() => _rows.FirstOrDefault();
    public Row? Head() => First();
    public IReadOnlyList<Row> Head(int n) => _rows.Take(n).ToList();
    public IReadOnlyList<Row> Tail(int n) => _rows.TakeLast(n).ToList();
    public IReadOnlyList<Row> Collect() => _rows;

    public long Count() => _rows.Count;

    public IDataFrame Describe(params string[] columns)
    {
        // Statistics: count, mean, stddev, min, max
        var cols = columns.Length > 0 ? columns : Columns.ToArray();
        // Simplified implementation
        return this;
    }

    public IDataFrame Summary(params string[] statistics) => Describe();

    [SuppressMessage("Security", "CA5394:Do not use insecure randomness", Justification = "Random used for statistical sampling, not security")]
    public IDataFrame Sample(double fraction, bool withReplacement = false, long? seed = null)
    {
        var rng = seed.HasValue ? new Random((int)seed.Value) : new Random();
        var sampled = _rows.Where(_ => rng.NextDouble() < fraction);
        return new DataFrameImpl(sampled, _schema);
    }

    public IDataFrame Explode(string column)
    {
        var idx = _schema.FieldIndex(column);
        var result = new List<Row>();

        foreach (var row in _rows)
        {
            var value = row[idx];
            if (value is IEnumerable enumerable and not string)
            {
                foreach (var item in enumerable)
                {
                    var values = row.ToArray();
                    values[idx] = item;
                    result.Add(new Row(values, _schema));
                }
            }
            else
            {
                result.Add(row);
            }
        }

        return new DataFrameImpl(result, _schema);
    }

    public IDataFrame ExplodeOuter(string column)
    {
        var idx = _schema.FieldIndex(column);
        var result = new List<Row>();

        foreach (var row in _rows)
        {
            var value = row[idx];
            if (value is IEnumerable enumerable and not string)
            {
                var items = enumerable.Cast<object?>().ToList();
                if (items.Count == 0)
                {
                    var values = row.ToArray();
                    values[idx] = null;
                    result.Add(new Row(values, _schema));
                }
                else
                {
                    foreach (var item in items)
                    {
                        var values = row.ToArray();
                        values[idx] = item;
                        result.Add(new Row(values, _schema));
                    }
                }
            }
            else
            {
                result.Add(row);
            }
        }

        return new DataFrameImpl(result, _schema);
    }

    public IGroupedDataFrame Rollup(params string[] columns) => GroupBy(columns);
    public IGroupedDataFrame Cube(params string[] columns) => GroupBy(columns);

    public void Show(int numRows = 20, bool truncate = true)
    {
        var colWidths = Columns.Select(c => c.Length).ToArray();
        var rowsToShow = _rows.Take(numRows).ToList();

        // Calculate column widths
        foreach (var row in rowsToShow)
        {
            for (int i = 0; i < colWidths.Length; i++)
            {
                var str = row[i]?.ToString() ?? "null";
                if (truncate && str.Length > 20) str = str[..17] + "...";
                colWidths[i] = Math.Max(colWidths[i], str.Length);
            }
        }

        // Print header
        var separator = "+" + string.Join("+", colWidths.Select(w => new string('-', w + 2))) + "+";
        Console.WriteLine(separator);
        Console.WriteLine("|" + string.Join("|", Columns.Select((c, i) => $" {c.PadRight(colWidths[i])} ")) + "|");
        Console.WriteLine(separator);

        // Print rows
        foreach (var row in rowsToShow)
        {
            var values = new List<string>();
            for (int i = 0; i < colWidths.Length; i++)
            {
                var str = row[i]?.ToString() ?? "null";
                if (truncate && str.Length > 20) str = str[..17] + "...";
                values.Add($" {str.PadRight(colWidths[i])} ");
            }
            Console.WriteLine("|" + string.Join("|", values) + "|");
        }
        Console.WriteLine(separator);

        if (_rows.Count > numRows)
            Console.WriteLine($"only showing top {numRows} rows");
    }

    public IDataFrame Cache()
    {
        _cached = true;
        return this;
    }

    public IDataFrame Persist() => Cache();
    public IDataFrame Coalesce(int numPartitions) => this;
    public IDataFrame Repartition(int numPartitions) => this;
    public IDataFrame Repartition(int numPartitions, params Column[] columns) => this;

    public IStream<Row, Row> ToStream()
    {
        throw new NotImplementedException("ToStream requires StreamsBuilder context");
    }

    public IEnumerable<T> As<T>() where T : new()
    {
        var type = typeof(T);
        var props = type.GetProperties().ToDictionary(p => p.Name, StringComparer.OrdinalIgnoreCase);

        foreach (var row in _rows)
        {
            var obj = new T();
            foreach (var field in _schema.Fields)
            {
                if (props.TryGetValue(field.Name, out var prop))
                {
                    var value = row[field.Name];
                    if (value != null)
                    {
                        try
                        {
                            var converted = Convert.ChangeType(value, prop.PropertyType);
                            prop.SetValue(obj, converted);
                        }
                        catch { }
                    }
                }
            }
            yield return obj;
        }
    }

    public DataFrameWriter Write() => new(this);

    public IEnumerator<Row> GetEnumerator() => _rows.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private sealed class RowComparer : IEqualityComparer<Row>
    {
        public static readonly RowComparer Instance = new();
        public bool Equals(Row? x, Row? y) =>
            x == null && y == null || x != null && y != null && x.SequenceEqual(y);
        public int GetHashCode(Row obj) =>
            obj.Aggregate(17, (h, v) => h * 31 + (v?.GetHashCode() ?? 0));
    }
}

/// <summary>
/// Implementation of grouped DataFrame.
/// </summary>
internal sealed class GroupedDataFrameImpl : IGroupedDataFrame
{
    private readonly DataFrameImpl _df;
    private readonly string[] _groupColumns;

    public GroupedDataFrameImpl(DataFrameImpl df, string[] groupColumns)
    {
        _df = df;
        _groupColumns = groupColumns;
    }

    public IDataFrame Count()
    {
        return AggregateInternal((rows, _) => rows.Count());
    }

    public IDataFrame Sum(params string[] columns) => AggregateInternal((rows, col) =>
        rows.Sum(r => Convert.ToDouble(r[col] ?? 0)), columns);

    public IDataFrame Avg(params string[] columns) => AggregateInternal((rows, col) =>
        rows.Average(r => Convert.ToDouble(r[col] ?? 0)), columns);

    public IDataFrame Mean(params string[] columns) => Avg(columns);

    public IDataFrame Min(params string[] columns) => AggregateInternal((rows, col) =>
        rows.Min(r => r[col]), columns);

    public IDataFrame Max(params string[] columns) => AggregateInternal((rows, col) =>
        rows.Max(r => r[col]), columns);

    private IDataFrame AggregateInternal(Func<IEnumerable<Row>, string, object?> aggFunc, params string[] columns)
    {
        var groupIndices = _groupColumns.Select(c => _df.Schema.FieldIndex(c)).ToArray();

        var groups = _df.GroupBy(row =>
            string.Join("|", groupIndices.Select(i => row[i]?.ToString() ?? "")));

        var newFields = _groupColumns.Select(c => _df.Schema.Fields[_df.Schema.FieldIndex(c)]).ToList();

        if (columns.Length == 0)
        {
            newFields.Add(new StructField("count", DataType.Long));
        }
        else
        {
            foreach (var col in columns)
                newFields.Add(new StructField($"{col}_agg", DataType.Double));
        }

        var newSchema = new StructType(newFields);
        var result = new List<Row>();

        foreach (var group in groups)
        {
            var values = new List<object?>();
            var firstRow = group.First();

            foreach (var idx in groupIndices)
                values.Add(firstRow[idx]);

            if (columns.Length == 0)
            {
                values.Add(group.Count());
            }
            else
            {
                foreach (var col in columns)
                    values.Add(aggFunc(group, col));
            }

            result.Add(new Row(values.ToArray(), newSchema));
        }

        return new DataFrameImpl(result, newSchema);
    }

    public IDataFrame Agg(params Column[] aggregates)
    {
        // Simplified implementation
        return Count();
    }

    public IDataFrame Pivot(string pivotColumn)
    {
        return _df;
    }

    public IDataFrame Pivot(string pivotColumn, params object[] values)
    {
        return _df;
    }
}
