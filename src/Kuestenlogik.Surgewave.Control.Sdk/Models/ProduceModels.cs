namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Request to produce a message to a topic.
/// </summary>
public sealed class ProduceMessageRequest
{
    public string? Key { get; set; }
    public string? Value { get; set; }
    public Dictionary<string, string>? Headers { get; set; }
    public int? Partition { get; set; }
}

/// <summary>
/// Result of producing a message.
/// </summary>
public sealed record ProduceMessageResult(
    string Topic,
    int Partition,
    long Offset,
    DateTimeOffset Timestamp);

/// <summary>
/// A saved message template for quick re-use.
/// </summary>
public sealed class MessageTemplate
{
    public string Name { get; set; } = "";
    public string? Key { get; set; }
    public string? Value { get; set; }
    public Dictionary<string, string>? Headers { get; set; }
    public string Format { get; set; } = "JSON";
}

/// <summary>
/// Configuration for data masking rules.
/// </summary>
public sealed class DataMaskingConfig
{
    public List<MaskingRule> Rules { get; set; } = [];
    public bool Enabled { get; set; } = true;
}

/// <summary>
/// A single data masking rule targeting specific fields.
/// </summary>
public sealed class MaskingRule
{
    public string Name { get; set; } = "";
    public string Pattern { get; set; } = "";
    public string Replacement { get; set; } = "***";
    public MaskingRuleType Type { get; set; } = MaskingRuleType.Regex;
    public bool Enabled { get; set; } = true;
}

/// <summary>
/// Type of masking rule pattern matching.
/// </summary>
public enum MaskingRuleType
{
    Regex,
    JsonPath
}
