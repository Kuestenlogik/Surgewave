using System.CommandLine;
using System.Reflection;
using System.Runtime.Loader;
using Kuestenlogik.Surgewave.Plugins;

namespace Kuestenlogik.Surgewave.Cli;

internal static class CliPluginDiscovery
{
    public static IEnumerable<Command> DiscoverCommands(string pluginsDirectory)
    {
        var pluginsDir = Path.GetFullPath(pluginsDirectory);
        if (!Directory.Exists(pluginsDir))
            yield break;

        foreach (var pluginDir in Directory.GetDirectories(pluginsDir))
        {
            foreach (var dll in Directory.GetFiles(pluginDir, "*.dll"))
            {
                ICliPlugin[] plugins;
                try
                {
                    var context = new AssemblyLoadContext(Path.GetFileNameWithoutExtension(dll), isCollectible: false);
                    var assembly = context.LoadFromAssemblyPath(Path.GetFullPath(dll));
                    plugins = assembly.GetTypes()
                        .Where(t => t is { IsAbstract: false, IsInterface: false } && typeof(ICliPlugin).IsAssignableFrom(t))
                        .Select(t => (ICliPlugin?)Activator.CreateInstance(t))
                        .Where(p => p is not null)
                        .ToArray()!;
                }
                catch
                {
                    continue;
                }

                foreach (var plugin in plugins)
                {
                    foreach (var command in plugin.GetCommands())
                    {
                        yield return command;
                    }
                }
            }
        }
    }
}
