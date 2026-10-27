using System.Text.Json;
using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests.Conformance;

/// <summary>
/// A Kafka message schema as Kafka declares it in clients/src/main/resources/common/message/*.json — the source
/// Kafka generates its own codecs from. Read here so the broker's hand-written codecs can be checked against it.
/// </summary>
internal sealed partial class KafkaMessageSchema
{
    public required string Name { get; init; }
    public required short ApiKey { get; init; }
    public required string Type { get; init; }
    public required VersionRange ValidVersions { get; init; }
    public required VersionRange FlexibleVersions { get; init; }
    public required IReadOnlyList<SchemaField> Fields { get; init; }

    public static KafkaMessageSchema Load(string path)
    {
        // The schema files carry a licence header and comments as // lines, which JSON does not allow.
        var json = CommentLine().Replace(File.ReadAllText(path), string.Empty);
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        var commonStructs = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        if (root.TryGetProperty("commonStructs", out var structs))
        {
            foreach (var s in structs.EnumerateArray())
                commonStructs[s.GetProperty("name").GetString()!] = s.Clone();
        }

        return new KafkaMessageSchema
        {
            Name = root.GetProperty("name").GetString()!,
            ApiKey = root.TryGetProperty("apiKey", out var key) ? key.GetInt16() : (short)-1,
            Type = root.GetProperty("type").GetString()!,
            ValidVersions = VersionRange.Parse(root.GetProperty("validVersions").GetString()),
            FlexibleVersions = VersionRange.Parse(root.TryGetProperty("flexibleVersions", out var flex) ? flex.GetString() : "none"),
            Fields = ReadFields(root.GetProperty("fields"), commonStructs),
        };
    }

    private static List<SchemaField> ReadFields(JsonElement fields, Dictionary<string, JsonElement> commonStructs)
    {
        var result = new List<SchemaField>();
        foreach (var f in fields.EnumerateArray())
        {
            var type = f.GetProperty("type").GetString()!;
            var elementType = type.StartsWith("[]", StringComparison.Ordinal) ? type[2..] : null;

            IReadOnlyList<SchemaField>? structFields = null;
            if (f.TryGetProperty("fields", out var inline))
                structFields = ReadFields(inline, commonStructs);
            else if (elementType is not null && commonStructs.TryGetValue(elementType, out var common))
                structFields = ReadFields(common.GetProperty("fields"), commonStructs);
            else if (elementType is null && commonStructs.TryGetValue(type, out var commonStruct))
                structFields = ReadFields(commonStruct.GetProperty("fields"), commonStructs);

            result.Add(new SchemaField
            {
                Name = f.GetProperty("name").GetString()!,
                Type = type,
                ElementType = elementType,
                Versions = VersionRange.Parse(f.GetProperty("versions").GetString()),
                NullableVersions = VersionRange.Parse(f.TryGetProperty("nullableVersions", out var n) ? n.GetString() : "none"),
                TaggedVersions = VersionRange.Parse(f.TryGetProperty("taggedVersions", out var t) ? t.GetString() : "none"),
                Tag = f.TryGetProperty("tag", out var tag) ? tag.GetInt32() : -1,
                StructFields = structFields,
            });
        }
        return result;
    }

    [GeneratedRegex(@"^\s*//.*$", RegexOptions.Multiline)]
    private static partial Regex CommentLine();
}

internal sealed class SchemaField
{
    public required string Name { get; init; }

    /// <summary>The declared type: int8 … int64, uint16, float64, bool, uuid, string, bytes, records, a struct name or []element.</summary>
    public required string Type { get; init; }

    /// <summary>The element type of an array field, null otherwise.</summary>
    public string? ElementType { get; init; }

    public required VersionRange Versions { get; init; }
    public required VersionRange NullableVersions { get; init; }
    public required VersionRange TaggedVersions { get; init; }
    public required int Tag { get; init; }

    /// <summary>The fields of a struct field or of an array's struct elements.</summary>
    public IReadOnlyList<SchemaField>? StructFields { get; init; }

    public bool IsArray => ElementType is not null;
    public bool IsStruct => StructFields is not null && !IsArray;
    public bool IsTagged(short version) => Tag >= 0 && TaggedVersions.Contains(version);
}

/// <summary>A Kafka version range: "none", "3", "3+" or "3-5".</summary>
internal readonly record struct VersionRange(short Lowest, short Highest)
{
    public static readonly VersionRange None = new(1, 0);

    public bool Contains(short version) => version >= Lowest && version <= Highest;

    public static VersionRange Parse(string? text)
    {
        if (string.IsNullOrEmpty(text) || text == "none")
            return None;
        if (text.EndsWith('+'))
            return new VersionRange(short.Parse(text[..^1]), short.MaxValue);
        var dash = text.IndexOf('-');
        return dash < 0
            ? new VersionRange(short.Parse(text), short.Parse(text))
            : new VersionRange(short.Parse(text[..dash]), short.Parse(text[(dash + 1)..]));
    }
}
