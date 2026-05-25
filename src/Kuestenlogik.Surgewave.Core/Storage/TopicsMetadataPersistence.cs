using System.Collections.Concurrent;
using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Core.Json;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Handles loading and saving topic metadata to JSON file.
/// Used when Raft is disabled (persistTopicsToFile = true).
/// </summary>
internal sealed class TopicsMetadataPersistence
{
    private readonly string _metadataPath;
    private readonly string _dataDirectory;
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly bool _enabled;

    public TopicsMetadataPersistence(
        string dataDirectory,
        ILogSegmentFactory segmentFactory,
        bool enabled)
    {
        _dataDirectory = dataDirectory;
        _segmentFactory = segmentFactory;
        _enabled = enabled && segmentFactory.IsPersistent;

        var metadataDir = Path.Combine(dataDirectory, ".metadata");
        if (segmentFactory.IsPersistent)
        {
            Directory.CreateDirectory(metadataDir);
        }
        _metadataPath = Path.Combine(metadataDir, "topics.json");
    }

    /// <summary>
    /// Load topics metadata from JSON file on startup.
    /// </summary>
    public void Load(
        ConcurrentDictionary<string, TopicMetadata> topics,
        ConcurrentDictionary<Guid, string> topicIdToName,
        ConcurrentDictionary<TopicPartition, IPartitionLog> logs)
    {
        if (!_enabled || !File.Exists(_metadataPath))
        {
            return;
        }

        try
        {
            var jsonText = File.ReadAllText(_metadataPath);
            var topicList = JsonSerializer.Deserialize(jsonText, CoreJsonContext.Default.ListTopicMetadata);

            if (topicList == null)
            {
                return;
            }

            var needsSave = false;
            foreach (var metadata in topicList)
            {
                // Migration: if TopicId is empty (from older version), generate one
                var finalMetadata = metadata;
                if (metadata.TopicId == Guid.Empty)
                {
                    finalMetadata = metadata with { TopicId = Guid.NewGuid() };
                    needsSave = true;
                }

                topics[finalMetadata.Name] = finalMetadata;
                topicIdToName[finalMetadata.TopicId] = finalMetadata.Name;

                // Create partition logs for each topic
                for (int i = 0; i < finalMetadata.PartitionCount; i++)
                {
                    var topicPartition = new TopicPartition { Topic = finalMetadata.Name, Partition = i };
                    IPartitionLog log;
                    if (finalMetadata.CleanupPolicy == CleanupPolicy.Ephemeral)
                    {
                        var bufferBytes = ConfigParser.GetEphemeralBufferBytes(finalMetadata.Config);
                        log = new EphemeralPartitionLog(topicPartition, bufferBytes);
                    }
                    else
                    {
                        var segmentBytes = GetSegmentBytesFromConfig(finalMetadata.Config);
                        log = new PartitionLog(_dataDirectory, topicPartition, _segmentFactory, segmentBytes);
                    }
                    logs[topicPartition] = log;
                }
            }

            // Save migrated metadata if any topics were missing TopicId
            if (needsSave)
            {
                Save(topics);
            }
        }
        catch (JsonException)
        {
            // Corrupted file - start fresh
        }
    }

    /// <summary>
    /// Save topics metadata to JSON file.
    /// </summary>
    public void Save(ConcurrentDictionary<string, TopicMetadata> topics)
    {
        if (!_enabled)
        {
            return;
        }

        try
        {
            var topicList = topics.Values.ToList();
            var json = JsonSerializer.Serialize(topicList, CoreJsonContext.Default.ListTopicMetadata);

            // Write to temp file first, then rename for atomicity
            var tempPath = _metadataPath + ".tmp";
            File.WriteAllText(tempPath, json);
            File.Move(tempPath, _metadataPath, overwrite: true);
        }
        catch
        {
            // Log error but don't fail the operation
        }
    }

    /// <summary>
    /// Get segment bytes from topic config, or default if not specified.
    /// </summary>
    private static long GetSegmentBytesFromConfig(Dictionary<string, string> config)
    {
        return ConfigParser.GetSegmentBytes(config, ILogSegment.DefaultMaxSegmentSize);
    }
}
