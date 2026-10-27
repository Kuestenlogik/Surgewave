using System.Collections;
using System.Reflection;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests.Conformance;

/// <summary>
/// Compares a parsed request object with the values the schema encoder wrote, field by field, matching schema
/// field names to property names. A field the request type does not expose is not compared — that is a field the
/// broker does not use, not a misread. What is compared must hold the value written for it.
/// </summary>
internal static class ParsedValueComparer
{
    public static List<string> Compare(object actual, ExpectedStruct expected, string path = "", List<string>? unexposed = null)
    {
        var mismatches = new List<string>();
        foreach (var (name, value) in expected.Fields)
        {
            if (ReferenceEquals(value, ExpectedStruct.Unchecked))
                continue;
            var property = actual.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            var fieldPath = path.Length == 0 ? name : $"{path}.{name}";
            if (property is null || property.GetIndexParameters().Length > 0)
            {
                unexposed?.Add(fieldPath);
                continue;
            }
            CompareValue(property.GetValue(actual), value, fieldPath, mismatches, unexposed);
        }
        return mismatches;
    }

    private static void CompareValue(object? actual, object expected, string path, List<string> mismatches, List<string>? unexposed)
    {
        switch (expected)
        {
            case ExpectedStruct inner:
                if (actual is null)
                    mismatches.Add($"{path}: expected a value, got null");
                else
                    mismatches.AddRange(Compare(actual, inner, path, unexposed));
                return;

            case ExpectedArray array:
            {
                if (actual is null)
                {
                    mismatches.Add($"{path}: expected {array.Elements.Count} element(s), got null");
                    return;
                }
                if (actual is string || actual is not IEnumerable enumerable)
                    return; // not a collection on our side: a different shape, not comparable by name
                var items = enumerable.Cast<object?>().ToList();
                if (items.Count != array.Elements.Count)
                {
                    mismatches.Add($"{path}: expected {array.Elements.Count} element(s), got {items.Count}");
                    return;
                }
                for (var i = 0; i < items.Count; i++)
                    CompareValue(items[i], array.Elements[i], $"{path}[{i}]", mismatches, unexposed);
                return;
            }

            default:
                if (!PrimitiveEquals(actual, expected))
                    mismatches.Add($"{path}: expected {Show(expected)}, got {Show(actual)}");
                return;
        }
    }

    private static bool PrimitiveEquals(object? actual, object expected)
    {
        switch (expected)
        {
            case long l:
                return actual switch
                {
                    null => false,
                    Enum e => Convert.ToInt64(e) == l,
                    IConvertible c when actual is not string and not bool => Convert.ToInt64(c) == l,
                    _ => false,
                };
            case double d:
                return actual is IConvertible dc && actual is not string && Math.Abs(Convert.ToDouble(dc) - d) < 1e-9;
            case bool b:
                return actual is bool ab && ab == b;
            case string s:
                return actual is string a ? a == s : actual?.ToString() == s;
            case Guid g:
                return actual is Guid ag ? ag == g : actual?.ToString() == g.ToString();
            case byte[] bytes:
                return actual switch
                {
                    byte[] actualBytes => actualBytes.AsSpan().SequenceEqual(bytes),
                    ReadOnlyMemory<byte> m => m.Span.SequenceEqual(bytes),
                    Memory<byte> m => m.Span.SequenceEqual(bytes),
                    ArraySegment<byte> seg => seg.AsSpan().SequenceEqual(bytes),
                    _ => false,
                };
            default:
                return Equals(actual, expected);
        }
    }

    private static string Show(object? value) => value switch
    {
        null => "null",
        byte[] b => $"[{string.Join(",", b)}]",
        ReadOnlyMemory<byte> m => $"[{string.Join(",", m.ToArray())}]",
        string s => $"\"{s}\"",
        _ => value.ToString() ?? "?",
    };
}
