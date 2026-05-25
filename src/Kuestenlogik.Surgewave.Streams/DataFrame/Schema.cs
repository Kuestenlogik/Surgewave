namespace Kuestenlogik.Surgewave.Streams.DataFrame;

/// <summary>
/// Base class for data types in the schema.
/// </summary>
public abstract class DataType
{
    public abstract string TypeName { get; }

    public static readonly DataType Boolean = new AtomicType("boolean");
    public static readonly DataType Byte = new AtomicType("byte");
    public static readonly DataType Short = new AtomicType("short");
    public static readonly DataType Int = new AtomicType("int");
    public static readonly DataType Long = new AtomicType("long");
    public static readonly DataType Float = new AtomicType("float");
    public static readonly DataType Double = new AtomicType("double");
    public static readonly DataType Decimal = new AtomicType("decimal");
    public static readonly DataType String = new AtomicType("string");
    public static readonly DataType Binary = new AtomicType("binary");
    public static readonly DataType Timestamp = new AtomicType("timestamp");
    public static readonly DataType Date = new AtomicType("date");
    public static readonly DataType Null = new AtomicType("null");

    public override string ToString() => TypeName;
}

/// <summary>
/// Atomic (primitive) data types.
/// </summary>
internal sealed class AtomicType : DataType
{
    private readonly string _name;
    public AtomicType(string name) => _name = name;
    public override string TypeName => _name;
}

/// <summary>
/// Array data type.
/// </summary>
public sealed class ArrayType : DataType
{
    public DataType ElementType { get; }
    public bool ContainsNull { get; }

    public ArrayType(DataType elementType, bool containsNull = true)
    {
        ElementType = elementType;
        ContainsNull = containsNull;
    }

    public override string TypeName => $"array<{ElementType.TypeName}>";
}

/// <summary>
/// Map data type.
/// </summary>
public sealed class MapType : DataType
{
    public DataType KeyType { get; }
    public DataType ValueType { get; }
    public bool ValueContainsNull { get; }

    public MapType(DataType keyType, DataType valueType, bool valueContainsNull = true)
    {
        KeyType = keyType;
        ValueType = valueType;
        ValueContainsNull = valueContainsNull;
    }

    public override string TypeName => $"map<{KeyType.TypeName}, {ValueType.TypeName}>";
}

/// <summary>
/// A field in a struct type.
/// </summary>
public sealed class StructField
{
    public string Name { get; }
    public DataType DataType { get; }
    public bool Nullable { get; }
    public Dictionary<string, string>? Metadata { get; }

    public StructField(string name, DataType dataType, bool nullable = true, Dictionary<string, string>? metadata = null)
    {
        Name = name;
        DataType = dataType;
        Nullable = nullable;
        Metadata = metadata;
    }

    public override string ToString() => $"{Name}: {DataType.TypeName}{(Nullable ? "" : " NOT NULL")}";
}

/// <summary>
/// A struct type representing a row schema.
/// </summary>
public sealed class StructType : DataType
{
    private readonly List<StructField> _fields;
    private readonly Dictionary<string, int> _fieldIndex;

    public StructType() : this(new List<StructField>()) { }

    public StructType(IEnumerable<StructField> fields)
    {
        _fields = fields.ToList();
        _fieldIndex = _fields.Select((f, i) => (f.Name, i)).ToDictionary(x => x.Name, x => x.i);
    }

    public override string TypeName => "struct";

    public IReadOnlyList<StructField> Fields => _fields;
    public IReadOnlyCollection<string> FieldNames => _fieldIndex.Keys;

    /// <summary>
    /// Gets the index of a field by name.
    /// </summary>
    public int FieldIndex(string name) => _fieldIndex.TryGetValue(name, out var idx) ? idx : -1;

    /// <summary>
    /// Gets a field by name.
    /// </summary>
    public StructField? this[string name]
    {
        get
        {
            var idx = FieldIndex(name);
            return idx >= 0 ? _fields[idx] : null;
        }
    }

    /// <summary>
    /// Adds a field to the schema.
    /// </summary>
    public StructType Add(string name, DataType dataType, bool nullable = true, Dictionary<string, string>? metadata = null)
    {
        var newFields = _fields.Append(new StructField(name, dataType, nullable, metadata));
        return new StructType(newFields);
    }

    /// <summary>
    /// Adds a field to the schema.
    /// </summary>
    public StructType Add(StructField field)
    {
        var newFields = _fields.Append(field);
        return new StructType(newFields);
    }

    /// <summary>
    /// Creates a StructType from a type (reflection-based).
    /// </summary>
    public static StructType FromType<T>()
    {
        var type = typeof(T);
        var fields = new List<StructField>();

        foreach (var prop in type.GetProperties())
        {
            var dataType = MapClrTypeToDataType(prop.PropertyType);
            var nullable = !prop.PropertyType.IsValueType ||
                          Nullable.GetUnderlyingType(prop.PropertyType) != null;
            fields.Add(new StructField(prop.Name, dataType, nullable));
        }

        return new StructType(fields);
    }

    private static DataType MapClrTypeToDataType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        if (underlying == typeof(bool)) return Boolean;
        if (underlying == typeof(byte)) return Byte;
        if (underlying == typeof(short)) return Short;
        if (underlying == typeof(int)) return Int;
        if (underlying == typeof(long)) return Long;
        if (underlying == typeof(float)) return Float;
        if (underlying == typeof(double)) return Double;
        if (underlying == typeof(decimal)) return Decimal;
        if (underlying == typeof(string)) return String;
        if (underlying == typeof(byte[])) return Binary;
        if (underlying == typeof(DateTime)) return Timestamp;
        if (underlying == typeof(DateTimeOffset)) return Timestamp;
        if (underlying == typeof(DateOnly)) return Date;

        if (type.IsGenericType)
        {
            var genericDef = type.GetGenericTypeDefinition();
            if (genericDef == typeof(List<>) || genericDef == typeof(IList<>) ||
                genericDef == typeof(IEnumerable<>) || genericDef == typeof(IReadOnlyList<>))
            {
                var elementType = MapClrTypeToDataType(type.GetGenericArguments()[0]);
                return new ArrayType(elementType);
            }
            if (genericDef == typeof(Dictionary<,>) || genericDef == typeof(IDictionary<,>) ||
                genericDef == typeof(IReadOnlyDictionary<,>))
            {
                var keyType = MapClrTypeToDataType(type.GetGenericArguments()[0]);
                var valueType = MapClrTypeToDataType(type.GetGenericArguments()[1]);
                return new MapType(keyType, valueType);
            }
        }

        // For complex types, create a nested struct
        if (type.IsClass && type != typeof(string))
        {
            var nestedFields = type.GetProperties()
                .Select(p => new StructField(p.Name, MapClrTypeToDataType(p.PropertyType)))
                .ToList();
            return new StructType(nestedFields);
        }

        return String;
    }

    public string TreeString()
    {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine("root");
        foreach (var field in _fields)
        {
            AppendField(sb, field, 1);
        }
        return sb.ToString();
    }

    private void AppendField(System.Text.StringBuilder sb, StructField field, int level)
    {
        var prefix = new string(' ', level * 2) + "|-- ";
        var nullStr = field.Nullable ? "nullable = true" : "nullable = false";
        sb.AppendLine($"{prefix}{field.Name}: {field.DataType.TypeName} ({nullStr})");

        if (field.DataType is StructType nested)
        {
            foreach (var nestedField in nested.Fields)
            {
                AppendField(sb, nestedField, level + 1);
            }
        }
        else if (field.DataType is ArrayType arr && arr.ElementType is StructType elemStruct)
        {
            var elemPrefix = new string(' ', (level + 1) * 2) + "|-- ";
            sb.AppendLine($"{elemPrefix}element: struct");
            foreach (var nestedField in elemStruct.Fields)
            {
                AppendField(sb, nestedField, level + 2);
            }
        }
    }

    public override string ToString() => $"StructType({string.Join(", ", _fields.Select(f => f.ToString()))})";
}
