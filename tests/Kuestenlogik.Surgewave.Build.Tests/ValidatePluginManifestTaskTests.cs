using Kuestenlogik.Surgewave.Build.Tasks;
using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using Xunit;

namespace Kuestenlogik.Surgewave.Build.Tests;

public sealed class ValidatePluginManifestTaskTests : IDisposable
{
    private readonly string _tempDir;

    public ValidatePluginManifestTaskTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "swv-build-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public void Execute_returns_true_for_minimal_valid_manifest()
    {
        var path = WriteManifest("""
            {
              "id": "kuestenlogik.surgewave.example",
              "name": "Example Plugin",
              "version": "1.0.0",
              "assemblies": ["Example.dll"]
            }
            """);
        var (task, engine) = NewTask(path);

        var ok = task.Execute();

        Assert.True(ok);
        Assert.Empty(engine.Errors);
    }

    [Fact]
    public void Execute_fails_when_required_field_missing()
    {
        var path = WriteManifest("""
            {
              "id": "kuestenlogik.surgewave.example",
              "name": "Example Plugin",
              "version": "1.0.0"
            }
            """);
        var (task, engine) = NewTask(path);

        var ok = task.Execute();

        Assert.False(ok);
        Assert.Contains(engine.Errors, e => (e.Message ?? "").Contains("assemblies", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Execute_fails_when_id_pattern_violated()
    {
        // Spaces aren't allowed in the id (pattern: ^[a-zA-Z0-9._-]+$)
        var path = WriteManifest("""
            {
              "id": "has spaces",
              "name": "Example",
              "version": "1.0.0",
              "assemblies": ["Example.dll"]
            }
            """);
        var (task, engine) = NewTask(path);

        var ok = task.Execute();

        Assert.False(ok);
        Assert.NotEmpty(engine.Errors);
    }

    [Fact]
    public void Execute_fails_when_version_pattern_violated()
    {
        var path = WriteManifest("""
            {
              "id": "kuestenlogik.surgewave.example",
              "name": "Example",
              "version": "not-a-version",
              "assemblies": ["Example.dll"]
            }
            """);
        var (task, engine) = NewTask(path);

        var ok = task.Execute();

        Assert.False(ok);
    }

    [Fact]
    public void Execute_fails_when_assembly_entry_not_dll()
    {
        var path = WriteManifest("""
            {
              "id": "kuestenlogik.surgewave.example",
              "name": "Example",
              "version": "1.0.0",
              "assemblies": ["Example.exe"]
            }
            """);
        var (task, engine) = NewTask(path);

        var ok = task.Execute();

        Assert.False(ok);
    }

    [Fact]
    public void Execute_fails_when_manifest_is_malformed_json()
    {
        var path = WriteManifest("{ not json");
        var (task, engine) = NewTask(path);

        var ok = task.Execute();

        Assert.False(ok);
        Assert.Contains(engine.Errors, e => e.Code == "SWV-MANIFEST-PARSE");
    }

    [Fact]
    public void Execute_fails_when_manifest_path_does_not_exist()
    {
        var (task, engine) = NewTask(Path.Combine(_tempDir, "missing.json"));

        var ok = task.Execute();

        Assert.False(ok);
        Assert.Contains(engine.Errors, e => (e.Message ?? "").Contains("not found", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Execute_passes_full_manifest_with_dependencies()
    {
        var path = WriteManifest("""
            {
              "id": "kuestenlogik.surgewave.protocol.mqtt",
              "name": "MQTT Protocol",
              "version": "1.2.3-preview",
              "description": "Adds an MQTT broker surface to Surgewave.",
              "authors": ["Kuestenlogik <info@kuestenlogik.com>"],
              "license": "Apache-2.0",
              "projectUrl": "https://github.com/Kuestenlogik/Surgewave",
              "tags": ["protocol", "mqtt", "iot"],
              "minRuntimeVersion": "0.1.0",
              "dependencies": { "Microsoft.Extensions.Logging.Abstractions": "9.0.0" },
              "surgewaveDependencies": [
                { "id": "kuestenlogik.surgewave.protocol.core", "version": ">=0.1.0" }
              ],
              "assemblies": ["Kuestenlogik.Surgewave.Protocol.Mqtt.dll"],
              "pluginSettings": "mqtt-defaults.json"
            }
            """);
        var (task, engine) = NewTask(path);

        var ok = task.Execute();

        Assert.True(ok);
        Assert.Empty(engine.Errors);
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private string WriteManifest(string body)
    {
        var path = Path.Combine(_tempDir, "plugin.json");
        File.WriteAllText(path, body);
        return path;
    }

    private static (ValidatePluginManifestTask task, MockBuildEngine engine) NewTask(string manifestPath)
    {
        var engine = new MockBuildEngine();
        var task = new ValidatePluginManifestTask
        {
            BuildEngine = engine,
            ManifestPath = manifestPath,
        };
        return (task, engine);
    }

    private sealed class MockBuildEngine : IBuildEngine
    {
        public List<BuildErrorEventArgs> Errors { get; } = [];

        public bool ContinueOnError => false;
        public int LineNumberOfTaskNode => 0;
        public int ColumnNumberOfTaskNode => 0;
        public string ProjectFileOfTaskNode => "";

        public bool BuildProjectFile(string projectFileName, string[] targetNames, System.Collections.IDictionary globalProperties, System.Collections.IDictionary targetOutputs) => true;
        public void LogCustomEvent(CustomBuildEventArgs e) { }
        public void LogErrorEvent(BuildErrorEventArgs e) => Errors.Add(e);
        public void LogMessageEvent(BuildMessageEventArgs e) { }
        public void LogWarningEvent(BuildWarningEventArgs e) { }
    }
}
