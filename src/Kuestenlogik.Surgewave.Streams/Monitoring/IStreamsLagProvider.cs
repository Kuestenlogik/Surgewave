namespace Kuestenlogik.Surgewave.Streams.Monitoring;

/// <summary>
/// Provides consumer lag information for a streams application.
/// </summary>
public interface IStreamsLagProvider
{
    ApplicationLag GetApplicationLag();
    StreamsPartitionLag? GetPartitionLag(string topic, int partition);
    long GetTotalLag();
}
