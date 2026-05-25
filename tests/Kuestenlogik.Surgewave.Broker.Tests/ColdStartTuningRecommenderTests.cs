using Kuestenlogik.Surgewave.Broker.AutoTuning;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

public sealed class ColdStartTuningRecommenderTests
{
    private static readonly ColdStartBrokerSnapshot DefaultBroker = new(
        DefaultNumPartitions: 1,
        ProducerBatchSizeBytes: 16_384,
        ReplicaFetchMaxBytes: 1_048_576,
        LogSegmentBytes: 100L * 1024 * 1024);

    [Fact]
    public void High_Throughput_Topic_Triggers_Partition_Count_Recommendation()
    {
        // 24 GB observed across 1 hour on one topic = ~7 MB/s — under
        // threshold. Push to 50 GB to cross the 10 MB/s line.
        var profile = NewProfile(
            observedFor: TimeSpan.FromHours(1),
            topicBytes: ("hot", 50L * 1024 * 1024 * 1024));

        var recs = ColdStartTuningRecommender.Recommend(profile, DefaultBroker);

        var partitionRec = Assert.Single(recs, r => r.RuleId == "cold-start.num-partitions");
        Assert.Equal("num.partitions", partitionRec.ConfigKey);
        Assert.Equal("1", partitionRec.CurrentValue);
        // suggested = min(1*2, 12) = 2
        Assert.Equal("2", partitionRec.SuggestedValue);
        Assert.Contains("hot", partitionRec.Reason);
    }

    [Fact]
    public void Low_Throughput_Topic_Does_Not_Suggest_More_Partitions()
    {
        var profile = NewProfile(
            observedFor: TimeSpan.FromHours(1),
            topicBytes: ("cold", 100L * 1024 * 1024)); // ~28 KB/s

        var recs = ColdStartTuningRecommender.Recommend(profile, DefaultBroker);

        Assert.DoesNotContain(recs, r => r.RuleId == "cold-start.num-partitions");
    }

    [Fact]
    public void Small_Records_Trigger_Batch_Size_Recommendation()
    {
        // 100 000 records, 5 MB total → avg 50 B per record.
        var profile = NewProfile(
            observedFor: TimeSpan.FromMinutes(10),
            topicBytes: ("orders", 5L * 1024 * 1024),
            totalRecords: 100_000);

        var recs = ColdStartTuningRecommender.Recommend(profile, DefaultBroker);

        var batchRec = Assert.Single(recs, r => r.RuleId == "cold-start.batch-size");
        Assert.Equal("producer.batch.size", batchRec.ConfigKey);
        Assert.Equal("16384", batchRec.CurrentValue);
        // suggested = min(max(16384*4, 65536), 262144) = 65536
        Assert.Equal("65536", batchRec.SuggestedValue);
    }

    [Fact]
    public void Quiet_Workload_Does_Not_Recommend_Batch_Size_Change()
    {
        // Less than the 10 000 records threshold — recommender stays silent.
        var profile = NewProfile(
            observedFor: TimeSpan.FromMinutes(10),
            topicBytes: ("orders", 1024),
            totalRecords: 100);

        var recs = ColdStartTuningRecommender.Recommend(profile, DefaultBroker);
        Assert.DoesNotContain(recs, r => r.RuleId == "cold-start.batch-size");
    }

    [Fact]
    public void Replication_Lag_Triggers_Fetch_Size_Recommendation()
    {
        var profile = NewProfile(
            observedFor: TimeSpan.FromHours(1),
            topicBytes: ("topic", 1024),
            replicationLagSamples: 50,
            maxLagMs: 5000,
            avgLagMs: 1500);

        var recs = ColdStartTuningRecommender.Recommend(profile, DefaultBroker);

        var fetchRec = Assert.Single(recs, r => r.RuleId == "cold-start.replica-fetch-size");
        Assert.Equal("replica.fetch.max.bytes", fetchRec.ConfigKey);
        Assert.Equal("1048576", fetchRec.CurrentValue);
        Assert.Equal("2097152", fetchRec.SuggestedValue); // 1 MB * 2 = 2 MB
    }

    [Fact]
    public void Healthy_Replication_Does_Not_Trigger_Fetch_Size_Change()
    {
        var profile = NewProfile(
            observedFor: TimeSpan.FromHours(1),
            topicBytes: ("topic", 1024),
            replicationLagSamples: 50,
            maxLagMs: 200, // < 1 s threshold
            avgLagMs: 100);

        var recs = ColdStartTuningRecommender.Recommend(profile, DefaultBroker);
        Assert.DoesNotContain(recs, r => r.RuleId == "cold-start.replica-fetch-size");
    }

    [Fact]
    public void High_Peak_Throughput_Triggers_Log_Segment_Size_Recommendation()
    {
        var profile = NewProfile(
            observedFor: TimeSpan.FromMinutes(10),
            topicBytes: ("torrent", 1024),
            peakBytesPerSecond: 200L * 1024 * 1024); // 200 MB/s

        var recs = ColdStartTuningRecommender.Recommend(profile, DefaultBroker);

        var segRec = Assert.Single(recs, r => r.RuleId == "cold-start.log-segment-size");
        Assert.Equal("log.segment.bytes", segRec.ConfigKey);
        Assert.Equal((1024L * 1024 * 1024).ToString(), segRec.SuggestedValue);
    }

    [Fact]
    public void Recommender_Skips_Rules_When_Already_At_Or_Above_Target()
    {
        // Broker already tuned beyond every threshold → no recommendations.
        var tunedBroker = new ColdStartBrokerSnapshot(
            DefaultNumPartitions: 12,
            ProducerBatchSizeBytes: 262_144,
            ReplicaFetchMaxBytes: 4 * 1024 * 1024,
            LogSegmentBytes: 1024L * 1024 * 1024);

        var profile = NewProfile(
            observedFor: TimeSpan.FromHours(1),
            topicBytes: ("hot", 50L * 1024 * 1024 * 1024),
            totalRecords: 100_000,
            peakBytesPerSecond: 200L * 1024 * 1024,
            replicationLagSamples: 50,
            maxLagMs: 5000,
            avgLagMs: 1500);

        var recs = ColdStartTuningRecommender.Recommend(profile, tunedBroker);
        Assert.Empty(recs);
    }

    [Fact]
    public void Recommend_Throws_On_Null_Inputs()
    {
        Assert.Throws<ArgumentNullException>(() =>
            ColdStartTuningRecommender.Recommend(null!, DefaultBroker));
        Assert.Throws<ArgumentNullException>(() =>
            ColdStartTuningRecommender.Recommend(EmptyProfile(), null!));
    }

    private static WorkloadProfile NewProfile(
        TimeSpan observedFor,
        (string topic, long bytes) topicBytes,
        long? totalRecords = null,
        long? peakBytesPerSecond = null,
        long replicationLagSamples = 0,
        long maxLagMs = 0,
        long avgLagMs = 0)
    {
        var (topic, bytes) = topicBytes;
        return new WorkloadProfile(
            StartedAt: DateTimeOffset.UtcNow - observedFor,
            ObservedFor: observedFor,
            IsComplete: true,
            TotalRecords: totalRecords ?? 1000,
            TotalBytes: bytes,
            TopicCardinality: 1,
            PeakRecordsPerSecond: 0,
            PeakBytesPerSecond: peakBytesPerSecond ?? 0,
            AverageReplicationLagMs: avgLagMs,
            MaxReplicationLagMs: maxLagMs,
            ReplicationLagSamples: replicationLagSamples,
            Topics: [new TopicWorkloadSnapshot(topic, totalRecords ?? 1000, bytes)]);
    }

    private static WorkloadProfile EmptyProfile() => new(
        StartedAt: DateTimeOffset.UtcNow,
        ObservedFor: TimeSpan.Zero,
        IsComplete: false,
        TotalRecords: 0,
        TotalBytes: 0,
        TopicCardinality: 0,
        PeakRecordsPerSecond: 0,
        PeakBytesPerSecond: 0,
        AverageReplicationLagMs: 0,
        MaxReplicationLagMs: 0,
        ReplicationLagSamples: 0,
        Topics: []);
}
