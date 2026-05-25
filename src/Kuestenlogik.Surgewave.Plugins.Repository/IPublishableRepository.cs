namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Interface for repositories that support publishing packages.
/// Not all repository types support publishing.
/// </summary>
public interface IPublishableRepository
{
    /// <summary>
    /// Whether this repository supports publishing.
    /// </summary>
    bool CanPublish { get; }

    /// <summary>
    /// Publish a package to the repository.
    /// </summary>
    Task<PackagePublishResult> PublishAsync(string packagePath, bool force = false, CancellationToken cancellationToken = default);
}
