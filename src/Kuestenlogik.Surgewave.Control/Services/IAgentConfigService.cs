using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Service for managing saved agent configurations.
/// </summary>
public interface IAgentConfigService
{
    /// <summary>
    /// List all saved agent configurations.
    /// </summary>
    Task<IReadOnlyList<AgentConfig>> ListAsync();

    /// <summary>
    /// Get an agent configuration by ID.
    /// </summary>
    Task<AgentConfig?> GetAsync(string id);

    /// <summary>
    /// Save an agent configuration (create or update).
    /// </summary>
    Task SaveAsync(AgentConfig config);

    /// <summary>
    /// Delete an agent configuration by ID.
    /// </summary>
    Task DeleteAsync(string id);
}
