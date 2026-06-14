using System.Reflection;
using Microsoft.Build.Framework;
using NJsonSchema;
using NJsonSchema.Validation;
using MsBuildTask = Microsoft.Build.Utilities.Task;

namespace Kuestenlogik.Surgewave.Build.Tasks;

/// <summary>
/// MSBuild task that validates a plugin's <c>plugin.json</c> against the
/// canonical schema <c>schemas/plugin-manifest/v1.json</c>, fails the
/// build on any violation. Runs at Build time when the manifest lives
/// next to the csproj so the plugin author sees the error in their IDE
/// before they try to pack the <c>.swpkg</c>.
///
/// The schema is shipped as an embedded resource — the task is fully
/// self-contained, no schema-path probe on the build machine.
/// </summary>
public sealed class ValidatePluginManifestTask : MsBuildTask
{
    private const string SchemaResourceName =
        "Kuestenlogik.Surgewave.Build.Schemas.plugin-manifest-v1.json";

    /// <summary>Path to the plugin.json file under inspection.</summary>
    [Required]
    public string ManifestPath { get; set; } = "";

    public override bool Execute()
    {
        if (!File.Exists(ManifestPath))
        {
            Log.LogError($"Surgewave: plugin manifest not found: {ManifestPath}");
            return false;
        }

        string schemaJson;
        try
        {
            schemaJson = LoadEmbeddedSchema();
        }
        catch (Exception ex)
        {
            Log.LogError($"Surgewave: failed to load embedded plugin-manifest schema: {ex.Message}");
            return false;
        }

        JsonSchema schema;
        try
        {
            schema = JsonSchema.FromJsonAsync(schemaJson).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            Log.LogError($"Surgewave: plugin-manifest schema is malformed: {ex.Message}");
            return false;
        }

        var manifestJson = File.ReadAllText(ManifestPath);
        ICollection<ValidationError> errors;
        try
        {
            errors = schema.Validate(manifestJson);
        }
        catch (Exception ex)
        {
            // Parse error in the manifest itself — surface as file-scoped MSBuild error.
            Log.LogError(
                subcategory: "Surgewave",
                errorCode: "SWV-MANIFEST-PARSE",
                helpKeyword: null,
                file: ManifestPath,
                lineNumber: 0, columnNumber: 0, endLineNumber: 0, endColumnNumber: 0,
                message: $"plugin.json is not valid JSON: {ex.Message}");
            return false;
        }

        if (errors.Count == 0)
        {
            Log.LogMessage(MessageImportance.Normal,
                $"Surgewave: plugin.json validates against the v1 manifest schema.");
            return true;
        }

        foreach (var error in errors)
        {
            // LineNumber on NJsonSchema's ValidationError is 1-based, 0 when unknown.
            // MSBuild wants a 1-based line too; pass through verbatim, IDEs render it.
            Log.LogError(
                subcategory: "Surgewave",
                errorCode: "SWV-MANIFEST-" + error.Kind.ToString().ToUpperInvariant(),
                helpKeyword: null,
                file: ManifestPath,
                lineNumber: error.LineNumber,
                columnNumber: error.LinePosition,
                endLineNumber: 0, endColumnNumber: 0,
                message: $"{error.Path}: {error.Kind} ({error.Property})");
        }
        return false;
    }

    private static string LoadEmbeddedSchema()
    {
        var assembly = typeof(ValidatePluginManifestTask).Assembly;
        using var stream = assembly.GetManifestResourceStream(SchemaResourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource '{SchemaResourceName}' missing — check Surgewave.Build.csproj "
                + "EmbeddedResource include + LogicalName.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
