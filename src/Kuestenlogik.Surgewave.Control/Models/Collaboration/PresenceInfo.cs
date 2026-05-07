namespace Kuestenlogik.Surgewave.Control.Models.Collaboration;

public sealed class PresenceInfo
{
    public required string ConnectionId { get; init; }
    public required string UserName { get; init; }
    public string Color { get; set; } = "#FF0000";
    public double CursorX { get; set; }
    public double CursorY { get; set; }
    public DateTimeOffset JoinedAt { get; init; } = DateTimeOffset.UtcNow;
    public string? LockedNodeId { get; set; }
}
