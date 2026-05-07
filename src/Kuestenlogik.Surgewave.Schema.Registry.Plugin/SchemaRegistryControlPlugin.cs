using System.Reflection;
using Kuestenlogik.Surgewave.Plugins;

namespace Kuestenlogik.Surgewave.Schema.Registry.Plugin;

public sealed class SchemaRegistryControlPlugin : IControlPlugin
{
    public string FeatureId => "Surgewave.SchemaRegistry.Control";
    public string DisplayName => "Schema Registry UI";

    public Assembly PageAssembly => typeof(SchemaRegistryControlPlugin).Assembly;

    public IEnumerable<ControlNavItem> GetNavItems()
    {
        yield return new ControlNavItem("Schema Registry", "/schemas", "Schema", "Integration", 30);
        yield return new ControlNavItem("Schema Graph", "/schemas/graph", "DeviceHub", "Integration", 31);
        yield return new ControlNavItem("Schema Evolution", "/schemas/evolution", "CompareArrows", "Integration", 32);
        yield return new ControlNavItem("Schema Migration", "/schemas/migration", "Transform", "Integration", 33);
        yield return new ControlNavItem("Schema Linking", "/schemas/linking", "SyncAlt", "Integration", 34);
    }
}
