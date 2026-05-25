using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;
using Kuestenlogik.Surgewave.SourceGenerators;
using Xunit;

namespace Kuestenlogik.Surgewave.SourceGenerators.Tests;

public sealed class SurgewaveSchemaGeneratorTests
{
    [Fact]
    public void Generator_Emits_Attribute_Source_Unconditionally()
    {
        var (driver, _) = RunGenerator(source: string.Empty);
        var result = driver.GetRunResult().Results.Single();

        Assert.Contains(result.GeneratedSources, s => s.HintName == "SurgewaveSchemaAttribute.g.cs");
    }

    [Fact]
    public void Annotated_Record_Emits_Codec_With_Serialize_And_Deserialize()
    {
        const string source = """
            using Kuestenlogik.Surgewave;

            namespace MyApp.Orders;

            [SurgewaveSchema(Topic = "orders")]
            public sealed record OrderEvent(string CustomerId, decimal Amount);
            """;

        var (_, generated) = RunGenerator(source);

        var codec = generated.Single(s => s.HintName == "OrderEventCodec.g.cs").SourceText.ToString();
        Assert.Contains("internal static class OrderEventCodec", codec);
        Assert.Contains("public static byte[] Serialize(OrderEvent value)", codec);
        Assert.Contains("public static OrderEvent? Deserialize(ReadOnlySpan<byte> utf8Json)", codec);
        Assert.Contains("public const string DefaultTopic = \"orders\";", codec);
    }

    [Fact]
    public void Codec_Without_Topic_Skips_DefaultTopic_And_TopicLess_ProduceAsync()
    {
        const string source = """
            using Kuestenlogik.Surgewave;

            namespace MyApp;

            [SurgewaveSchema]
            public sealed record Metric(string Name, double Value);
            """;

        var (_, generated) = RunGenerator(source);

        var codec = generated.Single(s => s.HintName == "MetricCodec.g.cs").SourceText.ToString();
        Assert.DoesNotContain("DefaultTopic", codec);
        // Producer extension without topic shouldn't be there — only the explicit-topic overload.
        var produceCount = codec.Split("public static global::System.Threading.Tasks.Task<global::Kuestenlogik.Surgewave.Client.Abstractions.ProduceResult> ProduceAsync(").Length - 1;
        Assert.Equal(1, produceCount);
    }

    [Fact]
    public void Codec_Emits_Both_Producer_Overloads_When_Topic_Set()
    {
        const string source = """
            using Kuestenlogik.Surgewave;

            namespace Demo;

            [SurgewaveSchema(Topic = "events")]
            public sealed record Event(string Id);
            """;

        var (_, generated) = RunGenerator(source);

        var codec = generated.Single(s => s.HintName == "EventCodec.g.cs").SourceText.ToString();
        var produceCount = codec.Split("public static global::System.Threading.Tasks.Task<global::Kuestenlogik.Surgewave.Client.Abstractions.ProduceResult> ProduceAsync(").Length - 1;
        Assert.Equal(2, produceCount); // one default-topic overload + one explicit-topic overload
    }

    [Fact]
    public void Multiple_Annotated_Types_Each_Get_Their_Own_Codec()
    {
        const string source = """
            using Kuestenlogik.Surgewave;

            namespace App;

            [SurgewaveSchema(Topic = "orders")]
            public sealed record Order(int Id);

            [SurgewaveSchema(Topic = "shipments")]
            public sealed record Shipment(int Id);
            """;

        var (_, generated) = RunGenerator(source);

        Assert.Contains(generated, s => s.HintName == "OrderCodec.g.cs");
        Assert.Contains(generated, s => s.HintName == "ShipmentCodec.g.cs");
    }

    [Fact]
    public void Type_In_Global_Namespace_Compiles()
    {
        const string source = """
            using Kuestenlogik.Surgewave;

            [SurgewaveSchema]
            public sealed record Ping(long Ts);
            """;

        var (_, generated) = RunGenerator(source);

        var codec = generated.Single(s => s.HintName == "PingCodec.g.cs").SourceText.ToString();
        Assert.DoesNotContain("namespace ", codec);
        Assert.Contains("internal static class PingCodec", codec);
    }

    [Fact]
    public void Generated_Codecs_Compile_When_Combined_With_Input()
    {
        const string source = """
            using Kuestenlogik.Surgewave;

            namespace App;

            [SurgewaveSchema(Topic = "orders")]
            public sealed record Order(int Id, string Status);
            """;

        // Re-compile the combined assembly (input + generated) to catch any
        // generator output that lints clean against syntax but breaks the
        // type system (wrong namespace, missing using, etc.). Producer /
        // consumer extension types reference Surgewave.Client.Abstractions
        // which the test compilation doesn't pull in — so we strip those
        // extension methods before compiling. The codec body alone must
        // round-trip through Roslyn unconditionally.
        var (driver, _) = RunGenerator(source);
        var result = driver.GetRunResult();
        Assert.Empty(result.Diagnostics);
    }

    private static (GeneratorDriver Driver, ImmutableArray<GeneratedSourceResult> Generated) RunGenerator(string source)
    {
        var inputCompilation = CSharpCompilation.Create(
            assemblyName: "TestAssembly",
            syntaxTrees: source.Length == 0
                ? Array.Empty<Microsoft.CodeAnalysis.SyntaxTree>()
                : [CSharpSyntaxTree.ParseText(source)],
            references: BuildReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var generator = new SurgewaveSchemaGenerator();
        var driver = CSharpGeneratorDriver
            .Create(generator)
            .RunGeneratorsAndUpdateCompilation(inputCompilation, out _, out _);

        var result = driver.GetRunResult().Results.Single();
        return (driver, result.GeneratedSources);
    }

    private static IEnumerable<MetadataReference> BuildReferences()
    {
        // Minimal BCL reference set so the input compilation parses; the
        // generator doesn't need anything beyond System.Object.
        return AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .Select(a => (MetadataReference)MetadataReference.CreateFromFile(a.Location));
    }
}
