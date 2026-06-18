using System.Text.Json;
using System.Text.Json.Nodes;

namespace Kuestenlogik.Surgewave.Cli.Commands.Setup;

/// <summary>
/// Renders a <see cref="SetupAnswers"/> into a starter
/// <c>appsettings.json</c>. The output is intentionally a small
/// surface — only the keys the wizard's choices map onto, leaving
/// the operator's existing appsettings (if any) free to override.
/// </summary>
public static class AppSettingsGenerator
{
    public static string Render(SetupAnswers answers)
    {
        var surgewave = new JsonObject();

        if (answers.StorageEngine is not null)
        {
            // Convention: the operator sets the engine name via plugin
            // metadata after install. The wizard records the package id so
            // an operator looking at appsettings.json sees which plugin
            // owns the engine.
            surgewave["Storage"] = new JsonObject
            {
                ["EnginePlugin"] = answers.StorageEngine.PackageId,
            };
        }

        if (answers.Auth != SetupAuthMethod.None)
        {
            var security = new JsonObject
            {
                ["SaslEnabled"] = answers.Auth is SetupAuthMethod.SaslPlain or SetupAuthMethod.SaslScram,
                ["TlsEnabled"] = answers.Auth is SetupAuthMethod.Tls or SetupAuthMethod.MutualTls,
            };
            if (answers.Auth == SetupAuthMethod.SaslPlain) security["SaslMechanism"] = "PLAIN";
            else if (answers.Auth == SetupAuthMethod.SaslScram) security["SaslMechanism"] = "SCRAM-SHA-256";
            if (answers.Auth == SetupAuthMethod.MutualTls) security["RequireClientCertificate"] = true;
            surgewave["Security"] = security;
        }

        if (answers.TelemetryEnabled)
        {
            surgewave["Telemetry"] = new JsonObject
            {
                ["Enabled"] = true,
                ["OtlpEndpoint"] = answers.OtlpEndpoint ?? "http://localhost:4317",
            };
        }

        var root = new JsonObject { ["Surgewave"] = surgewave };
        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
    }
}
