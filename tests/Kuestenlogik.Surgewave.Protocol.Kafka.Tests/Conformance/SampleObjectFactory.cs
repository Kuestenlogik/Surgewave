using System.Collections;
using System.Reflection;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests.Conformance;

/// <summary>
/// Builds an instance of a response type with a distinct value in every settable property, collections with one
/// element, so the bytes its writer produces can be read back against the schema and compared value by value.
/// </summary>
internal sealed class SampleObjectFactory
{
    private const int MaxDepth = 8;
    private int _sequence;

    public object Create(Type type) => Create(type, 0) ?? throw new NotSupportedException($"cannot build {type}");

    private object? Create(Type type, int depth)
    {
        if (depth > MaxDepth)
            return null;

        var underlying = Nullable.GetUnderlyingType(type);
        if (underlying is not null)
            return Create(underlying, depth);

        var n = ++_sequence;
        if (type == typeof(string)) return $"s{n}";
        if (type == typeof(bool)) return true;
        if (type == typeof(sbyte)) return (sbyte)(n % 100 + 1);
        if (type == typeof(byte)) return (byte)(n % 100 + 1);
        if (type == typeof(short)) return (short)(1000 + n);
        if (type == typeof(ushort)) return (ushort)(2000 + n);
        if (type == typeof(int)) return 100_000 + n;
        if (type == typeof(uint)) return (uint)(200_000 + n);
        if (type == typeof(long)) return 10_000_000_000L + n;
        if (type == typeof(double)) return n + 0.5;
        if (type == typeof(Guid)) return new Guid(n, 0x5a5a, 0x5a5a, 1, 2, 3, 4, 5, 6, 7, 8);
        if (type == typeof(byte[])) return new[] { (byte)n, (byte)(n + 1) };
        if (type == typeof(ReadOnlyMemory<byte>)) return new ReadOnlyMemory<byte>([(byte)n, (byte)(n + 1)]);
        if (type == typeof(Memory<byte>)) return new Memory<byte>([(byte)n, (byte)(n + 1)]);
        if (type.IsEnum)
        {
            var values = Enum.GetValues(type);
            // A value other than the first (usually None / 0), so a writer that ignores it shows.
            return values.Length > 1 ? values.GetValue(1) : values.GetValue(0);
        }

        if (type.IsArray)
        {
            var element = type.GetElementType()!;
            var item = Create(element, depth + 1);
            var array = Array.CreateInstance(element, item is null ? 0 : 1);
            if (item is not null)
                array.SetValue(item, 0);
            return array;
        }

        if (type.IsGenericType)
        {
            var definition = type.GetGenericTypeDefinition();
            var args = type.GetGenericArguments();
            if (args.Length == 2 && (definition == typeof(Dictionary<,>) || definition == typeof(IDictionary<,>)
                || definition == typeof(IReadOnlyDictionary<,>)))
            {
                var dictionary = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(args))!;
                var key = Create(args[0], depth + 1);
                var value = Create(args[1], depth + 1);
                if (key is not null && value is not null)
                    dictionary.Add(key, value);
                return dictionary;
            }
            if (args.Length == 1 && (definition == typeof(List<>) || definition == typeof(IList<>) || definition == typeof(IReadOnlyList<>)
                || definition == typeof(IEnumerable<>) || definition == typeof(ICollection<>) || definition == typeof(IReadOnlyCollection<>)))
            {
                var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(args))!;
                var item = Create(args[0], depth + 1);
                if (item is not null)
                    list.Add(item);
                return list;
            }
        }

        if (type.IsInterface || type.IsAbstract)
            return null;

        // A class or struct: the widest constructor, then every settable property.
        var constructor = type.GetConstructors().OrderByDescending(c => c.GetParameters().Length).FirstOrDefault();
        object instance;
        if (constructor is null)
        {
            if (!type.IsValueType)
                return null;
            instance = Activator.CreateInstance(type)!;
        }
        else
        {
            var arguments = constructor.GetParameters().Select(p => Create(p.ParameterType, depth + 1)).ToArray();
            instance = constructor.Invoke(arguments);
        }

        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!property.CanWrite || property.GetIndexParameters().Length > 0 || property.SetMethod is not { IsPublic: true })
                continue;
            if (depth == 0 && property.Name is "CorrelationId" or "ApiVersion" or "ApiKey")
                continue;
            var value = Create(property.PropertyType, depth + 1);
            if (value is not null)
                property.SetValue(instance, value);
        }
        return instance;
    }
}
