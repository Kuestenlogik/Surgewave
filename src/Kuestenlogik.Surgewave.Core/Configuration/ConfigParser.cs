using System.Collections.Frozen;
using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Core.Configuration;

/// <summary>
/// Parses topic and broker configuration values with support for human-readable formats.
/// Supports both Kafka-compatible numeric values and human-readable alternatives.
/// </summary>
public static partial class ConfigParser
{
    // Config key mappings: short name -> (Kafka name, value type)
    private static readonly FrozenDictionary<string, (string KafkaName, ConfigValueType Type)> s_configMappings =
        new Dictionary<string, (string KafkaName, ConfigValueType Type)>
        {
            // Byte-based configs
            ["segment"] = ("segment.bytes", ConfigValueType.Bytes),
            ["segment.bytes"] = ("segment.bytes", ConfigValueType.Bytes),
            ["max.message"] = ("max.message.bytes", ConfigValueType.Bytes),
            ["max.message.bytes"] = ("max.message.bytes", ConfigValueType.Bytes),
            ["retention.bytes"] = ("retention.bytes", ConfigValueType.Bytes),

            // Time-based configs
            ["retention"] = ("retention.ms", ConfigValueType.Milliseconds),
            ["retention.ms"] = ("retention.ms", ConfigValueType.Milliseconds),
            ["segment.ms"] = ("segment.ms", ConfigValueType.Milliseconds),
            ["min.cleanable.dirty.ratio"] = ("min.cleanable.dirty.ratio", ConfigValueType.Double),
            ["delete.retention"] = ("delete.retention.ms", ConfigValueType.Milliseconds),
            ["delete.retention.ms"] = ("delete.retention.ms", ConfigValueType.Milliseconds),
            ["flush.ms"] = ("flush.ms", ConfigValueType.Milliseconds),

            // Count-based configs
            ["min.insync.replicas"] = ("min.insync.replicas", ConfigValueType.Integer),
            ["flush.messages"] = ("flush.messages", ConfigValueType.Long),

            // Ephemeral buffer config
            ["ephemeral.buffer"] = ("ephemeral.buffer.bytes", ConfigValueType.Bytes),
            ["ephemeral.buffer.bytes"] = ("ephemeral.buffer.bytes", ConfigValueType.Bytes),

            // String configs
            ["cleanup.policy"] = ("cleanup.policy", ConfigValueType.String),
            ["compression.type"] = ("compression.type", ConfigValueType.String),
        }.ToFrozenDictionary();

    private enum ConfigValueType
    {
        Bytes,
        Milliseconds,
        Integer,
        Long,
        Double,
        String
    }

    /// <summary>
    /// Get segment bytes from config, supporting both "segment.bytes" and "segment" keys.
    /// Accepts: numeric values, or human-readable like "100MB", "1GB", "512KB"
    /// </summary>
    public static long GetSegmentBytes(Dictionary<string, string> config, long defaultValue)
    {
        // Try short name first (takes precedence)
        if (config.TryGetValue("segment", out var segmentValue))
        {
            if (TryParseBytes(segmentValue, out var bytes))
                return bytes;
        }

        // Fall back to Kafka-compatible name
        if (config.TryGetValue("segment.bytes", out var segmentBytesValue))
        {
            if (TryParseBytes(segmentBytesValue, out var bytes))
                return bytes;
        }

        return defaultValue;
    }

    /// <summary>
    /// Get retention milliseconds from config, supporting both "retention.ms" and "retention" keys.
    /// Accepts: numeric values (ms), or human-readable like "7d", "24h", "30m", "60s"
    /// </summary>
    public static long GetRetentionMs(Dictionary<string, string> config, long defaultValue)
    {
        // Try short name first (takes precedence)
        if (config.TryGetValue("retention", out var retentionValue))
        {
            if (TryParseMilliseconds(retentionValue, out var ms))
                return ms;
        }

        // Fall back to Kafka-compatible name
        if (config.TryGetValue("retention.ms", out var retentionMsValue))
        {
            if (TryParseMilliseconds(retentionMsValue, out var ms))
                return ms;
        }

        return defaultValue;
    }

    /// <summary>
    /// Get max message bytes from config.
    /// Accepts: numeric values, or human-readable like "1MB", "10KB"
    /// </summary>
    public static long GetMaxMessageBytes(Dictionary<string, string> config, long defaultValue)
    {
        if (config.TryGetValue("max.message", out var value) ||
            config.TryGetValue("max.message.bytes", out value))
        {
            if (TryParseBytes(value, out var bytes))
                return bytes;
        }

        return defaultValue;
    }

    /// <summary>
    /// Get ephemeral buffer bytes from config.
    /// Accepts: numeric values, or human-readable like "64MB", "256MB"
    /// Default: 64MB
    /// </summary>
    public static long GetEphemeralBufferBytes(Dictionary<string, string> config, long defaultValue = 64 * 1024 * 1024)
    {
        if (config.TryGetValue("ephemeral.buffer", out var value) ||
            config.TryGetValue("ephemeral.buffer.bytes", out value))
        {
            if (TryParseBytes(value, out var bytes))
                return bytes;
        }

        return defaultValue;
    }

    /// <summary>
    /// Get retention bytes from config.
    /// Accepts: numeric values, or human-readable like "10GB", "500MB"
    /// Use -1 for unlimited.
    /// </summary>
    public static long GetRetentionBytes(Dictionary<string, string> config, long defaultValue)
    {
        if (config.TryGetValue("retention.bytes", out var value))
        {
            if (TryParseBytes(value, out var bytes))
                return bytes;
        }

        return defaultValue;
    }

    /// <summary>
    /// Parse a byte value from string.
    /// Supports: plain numbers, KB, MB, GB, TB (case-insensitive)
    /// Examples: "1024", "100KB", "1MB", "1.5GB"
    /// </summary>
    public static bool TryParseBytes(string value, out long bytes)
    {
        bytes = 0;
        if (string.IsNullOrWhiteSpace(value))
            return false;

        value = value.Trim().ToUpperInvariant();

        // Try plain number first
        if (long.TryParse(value, out bytes))
            return bytes > 0;

        // Try with unit suffix
        var match = BytesRegex().Match(value);
        if (!match.Success)
            return false;

        if (!double.TryParse(match.Groups[1].Value, out var number))
            return false;

        var unit = match.Groups[2].Value;
        var multiplier = unit switch
        {
            "B" => 1L,
            "K" or "KB" => 1024L,
            "M" or "MB" => 1024L * 1024,
            "G" or "GB" => 1024L * 1024 * 1024,
            "T" or "TB" => 1024L * 1024 * 1024 * 1024,
            _ => 0L
        };

        if (multiplier == 0)
            return false;

        bytes = (long)(number * multiplier);
        return bytes > 0;
    }

    /// <summary>
    /// Parse a time value to milliseconds.
    /// Supports: plain numbers (treated as ms), s, m, h, d (case-insensitive)
    /// Examples: "1000", "60s", "30m", "24h", "7d"
    /// </summary>
    public static bool TryParseMilliseconds(string value, out long milliseconds)
    {
        milliseconds = 0;
        if (string.IsNullOrWhiteSpace(value))
            return false;

        value = value.Trim().ToUpperInvariant();

        // Try plain number first (interpreted as milliseconds)
        if (long.TryParse(value, out milliseconds))
            return milliseconds >= 0;

        // Try with unit suffix
        var match = TimeRegex().Match(value);
        if (!match.Success)
            return false;

        if (!double.TryParse(match.Groups[1].Value, out var number))
            return false;

        var unit = match.Groups[2].Value;
        var multiplier = unit switch
        {
            "MS" => 1L,
            "S" => 1000L,
            "M" => 60L * 1000,
            "H" => 60L * 60 * 1000,
            "D" => 24L * 60 * 60 * 1000,
            "W" => 7L * 24 * 60 * 60 * 1000,
            _ => 0L
        };

        if (multiplier == 0)
            return false;

        milliseconds = (long)(number * multiplier);
        return milliseconds >= 0;
    }

    /// <summary>
    /// Format bytes as human-readable string.
    /// </summary>
    public static string FormatBytes(long bytes)
    {
        if (bytes < 1024)
            return $"{bytes}B";
        if (bytes < 1024 * 1024)
            return $"{bytes / 1024.0:0.##}KB";
        if (bytes < 1024 * 1024 * 1024)
            return $"{bytes / (1024.0 * 1024):0.##}MB";
        if (bytes < 1024L * 1024 * 1024 * 1024)
            return $"{bytes / (1024.0 * 1024 * 1024):0.##}GB";
        return $"{bytes / (1024.0 * 1024 * 1024 * 1024):0.##}TB";
    }

    /// <summary>
    /// Format milliseconds as human-readable string.
    /// </summary>
    public static string FormatMilliseconds(long ms)
    {
        if (ms < 1000)
            return $"{ms}ms";
        if (ms < 60 * 1000)
            return $"{ms / 1000.0:0.##}s";
        if (ms < 60 * 60 * 1000)
            return $"{ms / (60.0 * 1000):0.##}m";
        if (ms < 24 * 60 * 60 * 1000)
            return $"{ms / (60.0 * 60 * 1000):0.##}h";
        return $"{ms / (24.0 * 60 * 60 * 1000):0.##}d";
    }

    /// <summary>
    /// Normalize config dictionary: convert short names to Kafka names and parse values.
    /// Returns a new dictionary with Kafka-compatible keys and numeric string values.
    /// </summary>
    public static Dictionary<string, string> NormalizeConfig(Dictionary<string, string> config)
    {
        var result = new Dictionary<string, string>();

        foreach (var (key, value) in config)
        {
            if (!s_configMappings.TryGetValue(key, out var mapping))
            {
                // Unknown config, pass through as-is
                result[key] = value;
                continue;
            }

            var normalizedValue = mapping.Type switch
            {
                ConfigValueType.Bytes when TryParseBytes(value, out var bytes) => bytes.ToString(),
                ConfigValueType.Milliseconds when TryParseMilliseconds(value, out var ms) => ms.ToString(),
                _ => value
            };

            // Use Kafka name, short name values override long name values
            result[mapping.KafkaName] = normalizedValue;
        }

        return result;
    }

    [GeneratedRegex(@"^(\d+(?:\.\d+)?)\s*(B|K|KB|M|MB|G|GB|T|TB)$")]
    private static partial Regex BytesRegex();

    [GeneratedRegex(@"^(\d+(?:\.\d+)?)\s*(MS|S|M|H|D|W)$")]
    private static partial Regex TimeRegex();
}
