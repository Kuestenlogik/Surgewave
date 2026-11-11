using Kuestenlogik.Surgewave.Clustering.Replication;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Kuestenlogik.Surgewave.Broker.Health;

/// <summary>
/// Unhealthy until the broker has caught up with its metadata log (#250): until then it accepts no client and answers
/// no API request, so a readiness probe must not send it traffic. A broker that cannot reach a quorum stays here.
/// </summary>
public sealed class MetadataCaughtUpHealthCheck : IHealthCheck
{
    private readonly ClusterController _controller;

    public MetadataCaughtUpHealthCheck(ClusterController controller)
    {
        _controller = controller;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) =>
        Task.FromResult(_controller.IsMetadataCaughtUp
            ? HealthCheckResult.Healthy("The broker has caught up with the metadata log.")
            : HealthCheckResult.Unhealthy("The broker has not caught up with the metadata log yet."));
}
