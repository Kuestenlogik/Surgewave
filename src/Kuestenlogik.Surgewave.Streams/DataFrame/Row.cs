using System.Collections;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Streams.DataFrame;

/// <summary>
/// Represents a row in a DataFrame, similar to Spark's Row class.
/// </summary>
public sealed class Row : IReadOnlyList<object?>
{
    private readonly object?[] _values;
    private readonly StructType _schema;

    public Row(object?[] values, StructType schema)
    {
        if (values.Length != schema.Fields.Count)
            throw new ArgumentException("Values count must match schema field count");

        _values = values;
        _schema = schema;
    }

    /// <summary>
    /// Gets the schema of this row.
    /// </summary>
    public StructType Schema => _schema;

    /// <summary>
    /// Gets the number of fields.
    /// </summary>
    public int Size => _values.Length;

    /// <summary>
    /// Gets a value by index.
    /// </summary>
    public object? this[int index] => _values[index];

    /// <summary>
    /// Gets a value by field name.
    /// </summary>
    public object? this[string fieldName]
    {
        get
        {
            var index = _schema.FieldIndex(fieldName);
            return index >= 0 ? _values[index] : null;
        }
    }

    /// <summary>
    /// Gets a typed value by index.
    /// </summary>
    public T? GetAs<T>(int index) => (T?)_values[index];

    /// <summary>
    /// Gets a typed value by field name.
    /// </summary>
    public T? GetAs<T>(string fieldName) => (T?)this[fieldName];

    /// <summary>
    /// Checks if a field is null.
    /// </summary>
    public bool IsNullAt(int index) => _values[index] == null;

    /// <summary>
    /// Gets the value as a specific type with convenience methods.
    /// </summary>
    public int GetInt(int index) => Convert.ToInt32(_values[index]);
    public long GetLong(int index) => Convert.ToInt64(_values[index]);
    public double GetDouble(int index) => Convert.ToDouble(_values[index]);
    public float GetFloat(int index) => Convert.ToSingle(_values[index]);
    public decimal GetDecimal(int index) => Convert.ToDecimal(_values[index]);
    public string? GetString(int index) => _values[index]?.ToString();
    public bool GetBoolean(int index) => Convert.ToBoolean(_values[index]);
    public DateTime GetDateTime(int index) => Convert.ToDateTime(_values[index]);

    /// <summary>
    /// Gets the value as a sequence (for array fields).
    /// </summary>
    public IEnumerable<T> GetSeq<T>(int index)
    {
        return _values[index] switch
        {
            IEnumerable<T> seq => seq,
            JsonElement je when je.ValueKind == JsonValueKind.Array =>
                je.EnumerateArray().Select(e => e.Deserialize<T>()!),
            _ => throw new InvalidCastException($"Cannot convert {_values[index]?.GetType()} to sequence")
        };
    }

    /// <summary>
    /// Gets the value as a map (for map fields).
    /// </summary>
    public IReadOnlyDictionary<TKey, TValue> GetMap<TKey, TValue>(int index)
        where TKey : notnull
    {
        return _values[index] switch
        {
            IReadOnlyDictionary<TKey, TValue> dict => dict,
            JsonElement je when je.ValueKind == JsonValueKind.Object =>
                je.Deserialize<Dictionary<TKey, TValue>>()!,
            _ => throw new InvalidCastException($"Cannot convert {_values[index]?.GetType()} to map")
        };
    }

    /// <summary>
    /// Gets the value as a nested Row (for struct fields).
    /// </summary>
    public Row GetStruct(int index)
    {
        var field = _schema.Fields[index];
        if (field.DataType is not StructType structType)
            throw new InvalidCastException($"Field {field.Name} is not a struct type");

        var value = _values[index];
        if (value is Row row) return row;
        if (value is JsonElement je)
        {
            var values = structType.Fields.Select(f =>
                je.TryGetProperty(f.Name, out var prop) ? JsonToObject(prop, f.DataType) : null
            ).ToArray();
            return new Row(values, structType);
        }
        throw new InvalidCastException($"Cannot convert {value?.GetType()} to Row");
    }

    private static object? JsonToObject(JsonElement element, DataType type)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number when type == DataType.Int => element.GetInt32(),
            JsonValueKind.Number when type == DataType.Long => element.GetInt64(),
            JsonValueKind.Number when type == DataType.Double => element.GetDouble(),
            JsonValueKind.Number when type == DataType.Float => element.GetSingle(),
            JsonValueKind.Number when type == DataType.Decimal => element.GetDecimal(),
            JsonValueKind.Number => element.GetDouble(),
            JsonValueKind.String when type == DataType.Timestamp => element.GetDateTime(),
            JsonValueKind.String => element.GetString(),
            _ => element
        };
    }

    /// <summary>
    /// Converts the row to a dictionary.
    /// </summary>
    public Dictionary<string, object?> ToDict()
    {
        var dict = new Dictionary<string, object?>();
        for (int i = 0; i < _schema.Fields.Count; i++)
        {
            dict[_schema.Fields[i].Name] = _values[i];
        }
        return dict;
    }

    /// <summary>
    /// Creates a copy of this row with some values changed.
    /// </summary>
    public Row Copy(params (string Field, object? Value)[] updates)
    {
        var newValues = (object?[])_values.Clone();
        foreach (var (field, value) in updates)
        {
            var index = _schema.FieldIndex(field);
            if (index >= 0) newValues[index] = value;
        }
        return new Row(newValues, _schema);
    }

    public override string ToString()
    {
        return $"[{string.Join(", ", _values.Select(v => v?.ToString() ?? "null"))}]";
    }

    // IReadOnlyList implementation
    public int Count => _values.Length;
    public IEnumerator<object?> GetEnumerator() => ((IEnumerable<object?>)_values).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => _values.GetEnumerator();
}

/// <summary>
/// Factory for creating rows.
/// </summary>
public static class RowFactory
{
    /// <summary>
    /// Creates a row from values without schema validation.
    /// </summary>
    public static Row Create(params object?[] values)
    {
        var fields = values.Select((v, i) => new StructField($"_{i}", InferType(v))).ToList();
        var schema = new StructType(fields);
        return new Row(values, schema);
    }

    private static DataType InferType(object? value) => value switch
    {
        null => DataType.String,
        bool => DataType.Boolean,
        int => DataType.Int,
        long => DataType.Long,
        float => DataType.Float,
        double => DataType.Double,
        decimal => DataType.Decimal,
        string => DataType.String,
        DateTime => DataType.Timestamp,
        DateTimeOffset => DataType.Timestamp,
        byte[] => DataType.Binary,
        _ => DataType.String
    };
}
