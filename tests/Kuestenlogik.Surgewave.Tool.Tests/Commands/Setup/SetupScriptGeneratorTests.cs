using Kuestenlogik.Surgewave.Setup;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Tool.Tests.Commands.Setup;

[Trait("Category", TestCategories.Unit)]
public sealed class SetupScriptGeneratorTests
{
    private static SetupPluginRef Ref(string id, string version) => new(id, version);

    [Fact]
    public void RenderBash_EmptyAnswers_ProducesShebangAndFailFastAndPlaceholders()
    {
        var script = SetupScriptGenerator.RenderBash(new SetupAnswers());

        Assert.StartsWith("#!/usr/bin/env bash", script);
        Assert.Contains("set -euo pipefail", script);
        Assert.Contains("(none — staying on the built-in default)", script);
        Assert.Contains("(no protocols selected)", script);
        Assert.Contains("(no schema handlers selected)", script);
        Assert.Contains("(no connectors selected)", script);
        Assert.DoesNotContain("surgewave plugins install", script);
    }

    [Fact]
    public void RenderPowerShell_EmptyAnswers_HasErrorActionPreferenceButNoShebang()
    {
        var script = SetupScriptGenerator.RenderPowerShell(new SetupAnswers());

        Assert.DoesNotContain("#!/usr/bin/env bash", script);
        Assert.Contains("$ErrorActionPreference = 'Stop'", script);
    }

    [Fact]
    public void RenderBash_InstallOrder_StorageBeforeProtocolsBeforeSchemasBeforeConnectors()
    {
        var answers = new SetupAnswers
        {
            StorageEngine = Ref("Acme.Storage", "1.0.0"),
            Protocols = [Ref("Acme.Proto.Mqtt", "1.0.0")],
            SchemaHandlers = [Ref("Acme.Schema.Avro", "1.0.0")],
            Connectors = [Ref("Acme.Connector.Kafka", "1.0.0")],
        };

        var script = SetupScriptGenerator.RenderBash(answers);

        var posStorage = script.IndexOf("Acme.Storage", StringComparison.Ordinal);
        var posProto = script.IndexOf("Acme.Proto.Mqtt", StringComparison.Ordinal);
        var posSchema = script.IndexOf("Acme.Schema.Avro", StringComparison.Ordinal);
        var posConn = script.IndexOf("Acme.Connector.Kafka", StringComparison.Ordinal);

        Assert.True(posStorage < posProto, "Storage must install before protocols");
        Assert.True(posProto < posSchema, "Protocols must install before schema handlers");
        Assert.True(posSchema < posConn, "Schema handlers must install before connectors");
    }

    [Fact]
    public void Render_IsDeterministic_SameAnswersProduceByteIdenticalOutput()
    {
        var answers = new SetupAnswers
        {
            StorageEngine = Ref("Acme.Storage", "2.1.0"),
            Protocols = [Ref("Acme.Proto.Mqtt", "1.0.0")],
            TelemetryEnabled = true,
            OtlpEndpoint = "https://otel.example/v1",
        };

        Assert.Equal(SetupScriptGenerator.RenderBash(answers), SetupScriptGenerator.RenderBash(answers));
        Assert.Equal(SetupScriptGenerator.RenderPowerShell(answers), SetupScriptGenerator.RenderPowerShell(answers));
    }

    [Fact]
    public void RenderBash_InstallCommand_UsesFromNuGetSwitch()
    {
        var answers = new SetupAnswers
        {
            Connectors = [Ref("Acme.Connector.Kafka", "3.2.1")],
        };

        var script = SetupScriptGenerator.RenderBash(answers);

        Assert.Contains("surgewave plugins install Acme.Connector.Kafka --version 3.2.1 --from-nuget", script);
    }
}
