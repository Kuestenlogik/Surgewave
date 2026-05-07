using Blazored.LocalStorage;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// LocalStorage-backed implementation of <see cref="IAgentConfigService"/>.
/// Agent configurations are stored per-user in the browser's LocalStorage.
/// </summary>
public sealed class AgentConfigService(ILocalStorageService localStorage) : IAgentConfigService
{
    private const string StorageKey = "surgewave-agent-configs";

    public async Task<IReadOnlyList<AgentConfig>> ListAsync()
    {
        var configs = await LoadAsync();
        return configs.OrderByDescending(c => c.CreatedAt).ToList();
    }

    public async Task<AgentConfig?> GetAsync(string id)
    {
        var configs = await LoadAsync();
        return configs.FirstOrDefault(c => c.Id == id);
    }

    public async Task SaveAsync(AgentConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        var configs = await LoadAsync();
        var existing = configs.FirstOrDefault(c => c.Id == config.Id);
        if (existing != null)
        {
            configs.Remove(existing);
        }
        configs.Add(config);
        await PersistAsync(configs);
    }

    public async Task DeleteAsync(string id)
    {
        var configs = await LoadAsync();
        var config = configs.FirstOrDefault(c => c.Id == id);
        if (config != null)
        {
            configs.Remove(config);
            await PersistAsync(configs);
        }
    }

    private async Task<List<AgentConfig>> LoadAsync()
    {
        try
        {
            return await localStorage.GetItemAsync<List<AgentConfig>>(StorageKey) ?? [];
        }
        catch
        {
            return [];
        }
    }

    private async Task PersistAsync(List<AgentConfig> configs)
    {
        await localStorage.SetItemAsync(StorageKey, configs);
    }
}
