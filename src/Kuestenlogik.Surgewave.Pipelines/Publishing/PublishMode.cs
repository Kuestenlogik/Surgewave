namespace Kuestenlogik.Surgewave.Pipelines.Publishing;

/// <summary>How a published pipeline relates to pipelines already on the broker.</summary>
public enum PublishMode
{
    /// <summary>Always create a new pipeline, even when one with the same name exists.</summary>
    CreateNew,

    /// <summary>
    /// Update the existing pipeline with the same name (stopping it first when running,
    /// restarting it afterwards), or create it when none exists. This is the redeploy mode.
    /// </summary>
    ReplaceByName,
}
