using System.Text.Json;

namespace Kuestenlogik.Surgewave.Streams.DataFrame;

/// <summary>
/// Entry point for DataFrame operations, similar to Spark's SparkSession.
/// </summary>
public sealed class SparkSession
{
    private readonly Dictionary<string, IDataFrame> _tempViews = new();
    private static SparkSession? _active;

    private SparkSession(string appName)
    {
        AppName = appName;
    }

    /// <summary>
    /// Application name.
    /// </summary>
    public string AppName { get; }

    /// <summary>
    /// Gets the active SparkSession.
    /// </summary>
    public static SparkSession? Active => _active;

    /// <summary>
    /// Creates a builder for SparkSession.
    /// </summary>
    public static Builder GetOrCreate() => new();

    /// <summary>
    /// Creates a DataFrame from a sequence of objects.
    /// </summary>
    public IDataFrame CreateDataFrame<T>(IEnumerable<T> data)
    {
        var schema = StructType.FromType<T>();
        var rows = data.Select(item => CreateRowFromObject(item, schema));
        return new DataFrameImpl(rows, schema);
    }

    /// <summary>
    /// Creates a DataFrame from rows with explicit schema.
    /// </summary>
    public IDataFrame CreateDataFrame(IEnumerable<Row> rows, StructType schema)
    {
        return new DataFrameImpl(rows, schema);
    }

    /// <summary>
    /// Creates a DataFrame from JSON strings.
    /// </summary>
    public IDataFrame CreateDataFrameFromJson(IEnumerable<string> jsonLines)
    {
        var rows = new List<Row>();
        StructType? schema = null;

        foreach (var line in jsonLines)
        {
            var doc = JsonDocument.Parse(line);
            var root = doc.RootElement;

            if (schema == null)
            {
                schema = InferSchemaFromJson(root);
            }

            var values = schema.Fields.Select(f =>
                root.TryGetProperty(f.Name, out var prop) ? JsonElementToObject(prop) : null
            ).ToArray();

            rows.Add(new Row(values, schema));
        }

        return new DataFrameImpl(rows, schema ?? new StructType());
    }

    /// <summary>
    /// Creates an empty DataFrame with the given schema.
    /// </summary>
    public IDataFrame EmptyDataFrame(StructType schema)
    {
        return new DataFrameImpl(Array.Empty<Row>(), schema);
    }

    /// <summary>
    /// Creates a DataFrame with a range of numbers.
    /// </summary>
    public IDataFrame Range(long end)
    {
        return Range(0, end, 1);
    }

    /// <summary>
    /// Creates a DataFrame with a range of numbers.
    /// </summary>
    public IDataFrame Range(long start, long end, long step = 1)
    {
        var schema = new StructType([new StructField("id", DataType.Long, false)]);
        var rows = new List<Row>();

        for (long i = start; i < end; i += step)
        {
            rows.Add(new Row([i], schema));
        }

        return new DataFrameImpl(rows, schema);
    }

    /// <summary>
    /// Reads data from various sources.
    /// </summary>
    public DataFrameReader Read() => new(this);

    /// <summary>
    /// Executes a SQL query against registered temp views.
    /// </summary>
    public IDataFrame Sql(string query)
    {
        // Simplified SQL parsing - would need a full SQL parser
        var lower = query.ToLowerInvariant().Trim();

        if (lower.StartsWith("select", StringComparison.Ordinal))
        {
            // Extract table name from FROM clause
            var fromIdx = lower.IndexOf("from", StringComparison.Ordinal);
            if (fromIdx > 0)
            {
                var afterFrom = query[(fromIdx + 4)..].Trim();
                var tableName = afterFrom.Split(' ', '\n', '\t')[0].Trim();

                if (_tempViews.TryGetValue(tableName, out var df))
                {
                    return df;
                }
            }
        }

        throw new InvalidOperationException($"Cannot parse SQL: {query}");
    }

    /// <summary>
    /// Registers a DataFrame as a temporary view.
    /// </summary>
    public void CreateTempView(string name, IDataFrame df)
    {
        _tempViews[name] = df;
    }

    /// <summary>
    /// Registers or replaces a temporary view.
    /// </summary>
    public void CreateOrReplaceTempView(string name, IDataFrame df)
    {
        _tempViews[name] = df;
    }

    /// <summary>
    /// Gets a registered temporary view.
    /// </summary>
    public IDataFrame? GetTempView(string name)
    {
        return _tempViews.TryGetValue(name, out var df) ? df : null;
    }

    /// <summary>
    /// Lists all registered temporary views.
    /// </summary>
    public IReadOnlyCollection<string> ListTables() => _tempViews.Keys;

    /// <summary>
    /// Drops a temporary view.
    /// </summary>
    public bool DropTempView(string name) => _tempViews.Remove(name);

    /// <summary>
    /// Stops the session.
    /// </summary>
    public void Stop()
    {
        _tempViews.Clear();
        if (_active == this) _active = null;
    }

    private static Row CreateRowFromObject<T>(T obj, StructType schema)
    {
        var values = new object?[schema.Fields.Count];
        var type = typeof(T);

        for (int i = 0; i < schema.Fields.Count; i++)
        {
            var field = schema.Fields[i];
            var prop = type.GetProperty(field.Name);
            values[i] = prop?.GetValue(obj);
        }

        return new Row(values, schema);
    }

    private static StructType InferSchemaFromJson(JsonElement element)
    {
        var fields = new List<StructField>();

        foreach (var prop in element.EnumerateObject())
        {
            var dataType = InferDataTypeFromJson(prop.Value);
            fields.Add(new StructField(prop.Name, dataType));
        }

        return new StructType(fields);
    }

    private static DataType InferDataTypeFromJson(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.True or JsonValueKind.False => DataType.Boolean,
            JsonValueKind.Number when element.TryGetInt64(out _) => DataType.Long,
            JsonValueKind.Number => DataType.Double,
            JsonValueKind.String => DataType.String,
            JsonValueKind.Array => new ArrayType(
                element.GetArrayLength() > 0
                    ? InferDataTypeFromJson(element[0])
                    : DataType.String),
            JsonValueKind.Object => InferSchemaFromJson(element),
            _ => DataType.String
        };
    }

    private static object? JsonElementToObject(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number when element.TryGetInt64(out var l) => l,
            JsonValueKind.Number => element.GetDouble(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Null => null,
            JsonValueKind.Array => element.EnumerateArray().Select(JsonElementToObject).ToList(),
            JsonValueKind.Object => element,
            _ => element.GetRawText()
        };
    }

    /// <summary>
    /// Builder for SparkSession.
    /// </summary>
    public sealed class Builder
    {
        private string _appName = "Surgewave-DataFrame";
        private readonly Dictionary<string, string> _config = new();

        public Builder AppName(string name)
        {
            _appName = name;
            return this;
        }

        public Builder Config(string key, string value)
        {
            _config[key] = value;
            return this;
        }

        public Builder Master(string master)
        {
            _config["spark.master"] = master;
            return this;
        }

        public Builder EnableHiveSupport()
        {
            _config["spark.sql.catalogImplementation"] = "hive";
            return this;
        }

        public SparkSession Build()
        {
            var session = new SparkSession(_appName);
            _active = session;
            return session;
        }
    }
}

/// <summary>
/// Reader for loading data into DataFrames.
/// </summary>
public sealed class DataFrameReader
{
    private readonly SparkSession _session;
    private string _format = "json";
    private readonly Dictionary<string, string> _options = new();
    private StructType? _schema;

    internal DataFrameReader(SparkSession session)
    {
        _session = session;
    }

    public DataFrameReader Format(string format)
    {
        _format = format;
        return this;
    }

    public DataFrameReader Option(string key, string value)
    {
        _options[key] = value;
        return this;
    }

    public DataFrameReader Options(Dictionary<string, string> options)
    {
        foreach (var kv in options) _options[kv.Key] = kv.Value;
        return this;
    }

    public DataFrameReader Schema(StructType schema)
    {
        _schema = schema;
        return this;
    }

    public DataFrameReader Schema(string schemaString)
    {
        // Would parse DDL-style schema string
        return this;
    }

    public IDataFrame Json(string path)
    {
        var lines = File.ReadAllLines(path);
        return _session.CreateDataFrameFromJson(lines);
    }

    public IDataFrame Csv(string path)
    {
        var lines = File.ReadAllLines(path);
        var hasHeader = _options.TryGetValue("header", out var h) && h == "true";
        var delimiter = _options.TryGetValue("delimiter", out var d) ? d[0] : ',';

        var headers = hasHeader
            ? lines[0].Split(delimiter)
            : Enumerable.Range(0, lines[0].Split(delimiter).Length).Select(i => $"_c{i}").ToArray();

        var dataLines = hasHeader ? lines.Skip(1) : lines;

        var schema = _schema ?? new StructType(headers.Select(h => new StructField(h, DataType.String)));
        var rows = new List<Row>();

        foreach (var line in dataLines)
        {
            var values = line.Split(delimiter).Cast<object?>().ToArray();
            if (values.Length == schema.Fields.Count)
                rows.Add(new Row(values, schema));
        }

        return new DataFrameImpl(rows, schema);
    }

    public IDataFrame Parquet(string path)
    {
        throw new NotSupportedException("Parquet format requires additional dependencies");
    }

    public IDataFrame Text(string path)
    {
        var lines = File.ReadAllLines(path);
        var schema = new StructType([new StructField("value", DataType.String)]);
        var rows = lines.Select(l => new Row([l], schema));
        return new DataFrameImpl(rows, schema);
    }

    public IDataFrame Load(string path)
    {
        return _format switch
        {
            "json" => Json(path),
            "csv" => Csv(path),
            "parquet" => Parquet(path),
            "text" => Text(path),
            _ => throw new NotSupportedException($"Format {_format} not supported")
        };
    }

    /// <summary>
    /// Reads from a Surgewave topic as a DataFrame.
    /// </summary>
    public IDataFrame Topic(string topic)
    {
        // Would integrate with Surgewave consumer
        return _session.EmptyDataFrame(new StructType([
            new StructField("key", DataType.String),
            new StructField("value", DataType.String),
            new StructField("timestamp", DataType.Timestamp),
            new StructField("partition", DataType.Int),
            new StructField("offset", DataType.Long)
        ]));
    }
}
