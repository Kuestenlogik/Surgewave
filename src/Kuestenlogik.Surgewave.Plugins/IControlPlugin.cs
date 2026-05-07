using System.Reflection;

namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Plugin that contributes pages and navigation items to Surgewave Control UI.
/// Discovered at startup from assemblies in the <c>plugins/</c> directory.
/// Pages use <c>@page</c> directives and are routed via <c>AdditionalAssemblies</c>.
/// </summary>
public interface IControlPlugin : IPlugin
{
    /// <summary>
    /// Returns the navigation items this plugin provides.
    /// Each item becomes a link in the Control UI sidebar.
    /// </summary>
    IEnumerable<ControlNavItem> GetNavItems();

    /// <summary>
    /// The assembly containing Razor pages with <c>@page</c> directives.
    /// Added to Blazor Router's <c>AdditionalAssemblies</c> for dynamic routing.
    /// </summary>
    Assembly PageAssembly { get; }
}

/// <summary>
/// A navigation item contributed by a Control UI plugin.
/// </summary>
public sealed record ControlNavItem(
    string Title,
    string Href,
    string Icon,
    string? Group = null,
    int Order = 100);
