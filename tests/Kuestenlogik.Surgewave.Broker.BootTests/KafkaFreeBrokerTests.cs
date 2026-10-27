using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// The broker is Kafka-free (#59): the Kafka wire protocol reaches it only as a plugin. For a dev run the build
/// stages that plugin into the broker's plugins/ folder (CopyKafkaPluginForDevRun, ordered by a
/// non-referencing ProjectReference), and none of that may make the broker itself depend on it.
/// </summary>
[Trait("Category", "Integration")]
public sealed class KafkaFreeBrokerTests
{
    private const string KafkaProtocolAssembly = "Kuestenlogik.Surgewave.Protocol.Kafka";
    private const string KafkaPluginId = "kuestenlogik.surgewave.protocol.kafka";

    private static string BrokerDir => Path.GetDirectoryName(BrokerBootSmokeTests.ResolveBrokerDll())!;

    [Fact]
    public void TheBrokersDependencies_DoNotNameTheKafkaProtocol()
    {
        var deps = File.ReadAllText(Path.Combine(BrokerDir, "surgewave-broker.deps.json"));

        Assert.DoesNotContain(KafkaProtocolAssembly, deps, StringComparison.Ordinal);
        Assert.False(File.Exists(Path.Combine(BrokerDir, KafkaProtocolAssembly + ".dll")),
            "the Kafka protocol assembly sits next to the broker, outside plugins/");
    }

    [Fact]
    public void NoBrokerAssembly_ReferencesTheKafkaProtocol()
    {
        var offenders = Directory.GetFiles(BrokerDir, "*.dll")
            .Where(path => Path.GetFileName(path).StartsWith("Kuestenlogik.Surgewave.", StringComparison.Ordinal)
                || Path.GetFileName(path) == "surgewave-broker.dll")
            .Where(path => References(path).Contains(KafkaProtocolAssembly))
            .Select(Path.GetFileName)
            .ToList();

        Assert.Empty(offenders);
    }

    [Fact]
    public void TheStagedPlugin_HasThePluginButNotTheHostsAssemblies()
    {
        var pluginDir = Path.Combine(BrokerDir, "plugins", KafkaPluginId);

        Assert.True(File.Exists(Path.Combine(pluginDir, "plugin.json")), $"no plugin.json in {pluginDir}");
        Assert.True(File.Exists(Path.Combine(pluginDir, KafkaProtocolAssembly + ".dll")), $"no plugin assembly in {pluginDir}");

        // A second copy of a host assembly would give the plugin its own type identities for what it shares
        // with the host.
        var hostCopies = Directory.GetFiles(pluginDir, "Kuestenlogik.Surgewave.*.dll")
            .Select(Path.GetFileName)
            .Where(name => name != KafkaProtocolAssembly + ".dll")
            .ToList();
        Assert.Empty(hostCopies);
    }

    private static HashSet<string> References(string assemblyPath)
    {
        using var stream = File.OpenRead(assemblyPath);
        using var pe = new PEReader(stream);
        if (!pe.HasMetadata)
            return [];
        var metadata = pe.GetMetadataReader();
        return metadata.AssemblyReferences
            .Select(handle => metadata.GetString(metadata.GetAssemblyReference(handle).Name))
            .ToHashSet(StringComparer.Ordinal);
    }
}
