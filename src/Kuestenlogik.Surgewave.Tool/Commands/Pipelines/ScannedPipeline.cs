using Kuestenlogik.Surgewave.Pipelines;

namespace Kuestenlogik.Surgewave.Cli.Commands.Pipelines;

/// <summary>One pipeline definition discovered in a user assembly.</summary>
internal sealed record ScannedPipeline
{
    /// <summary>The implementing class's full name, for diagnostics.</summary>
    public required string TypeName { get; init; }

    /// <summary>The built pipeline; its name falls back to the class name in kebab-case.</summary>
    public required BuiltPipeline Pipeline { get; init; }
}
