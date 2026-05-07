namespace Kuestenlogik.Surgewave.Control.Models.Collaboration;

public sealed class ApprovalRequest
{
    public string Id { get; init; } = Guid.NewGuid().ToString("N");
    public required string PipelineId { get; init; }
    public required string RequestedBy { get; init; }
    public string? Message { get; init; }
    public ApprovalStatus Status { get; set; } = ApprovalStatus.Pending;
    public string? ReviewedBy { get; set; }
    public DateTimeOffset RequestedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? ReviewedAt { get; set; }
}
