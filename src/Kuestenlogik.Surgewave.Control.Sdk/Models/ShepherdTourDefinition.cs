namespace Kuestenlogik.Surgewave.Control.Models;

public sealed class ShepherdTourDefinition
{
    public required string Id { get; init; }
    public required string Title { get; init; }
    public required List<ShepherdStep> Steps { get; init; }
}

public sealed class ShepherdStep
{
    public required string Id { get; init; }
    public required string Title { get; init; }
    public required string Text { get; init; }
    public string? Element { get; init; }
    public string Position { get; init; } = "bottom";
}
