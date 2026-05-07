using Kuestenlogik.Surgewave.Schema.Registry.Evolution;
using Xunit;

namespace Kuestenlogik.Surgewave.Schema.Registry.Tests;

/// <summary>
/// Tests for the AI-assisted schema evolution analyzer, code generator, and LLM enhancer.
/// </summary>
public sealed class SchemaEvolutionTests
{
    private readonly ITestOutputHelper _output;
    private readonly SchemaEvolutionAnalyzer _analyzer = new();
    private readonly SchemaMigrationCodeGenerator _codeGen = new();

    // Base schema: OrderEvent v1
    private const string OrderSchemaV1 = """
        {
            "type": "object",
            "properties": {
                "orderId": { "type": "string" },
                "amount": { "type": "number" },
                "customerId": { "type": "string" },
                "legacyId": { "type": "integer" }
            },
            "required": ["orderId", "amount", "customerId"]
        }
        """;

    // v2: added discountCode (optional), removed legacyId
    private const string OrderSchemaV2 = """
        {
            "type": "object",
            "properties": {
                "orderId": { "type": "string" },
                "amount": { "type": "number" },
                "customerId": { "type": "string" },
                "discountCode": { "type": "string" }
            },
            "required": ["orderId", "amount", "customerId"]
        }
        """;

    // v3: type changed (amount: number -> string), customerId made nullable
    private const string OrderSchemaV3 = """
        {
            "type": "object",
            "properties": {
                "orderId": { "type": "string" },
                "amount": { "type": "string" },
                "customerId": { "type": ["string", "null"] },
                "discountCode": { "type": "string" }
            },
            "required": ["orderId", "amount"]
        }
        """;

    // Schema with only field additions (non-breaking)
    private const string UserSchemaV1 = """
        {
            "type": "object",
            "properties": {
                "id": { "type": "integer" },
                "name": { "type": "string" }
            },
            "required": ["id", "name"]
        }
        """;

    // User v2: added email (optional)
    private const string UserSchemaV2 = """
        {
            "type": "object",
            "properties": {
                "id": { "type": "integer" },
                "name": { "type": "string" },
                "email": { "type": "string" }
            },
            "required": ["id", "name"]
        }
        """;

    // User v3: renamed "name" -> "fullName" (heuristic: same type, removed+added)
    private const string UserSchemaV3 = """
        {
            "type": "object",
            "properties": {
                "id": { "type": "integer" },
                "fullName": { "type": "string" },
                "email": { "type": "string" }
            },
            "required": ["id", "fullName"]
        }
        """;

    // Schema where a field is made required
    private const string EventSchemaOptional = """
        {
            "type": "object",
            "properties": {
                "eventId": { "type": "string" },
                "source": { "type": "string" }
            },
            "required": ["eventId"]
        }
        """;

    private const string EventSchemaRequired = """
        {
            "type": "object",
            "properties": {
                "eventId": { "type": "string" },
                "source": { "type": "string" }
            },
            "required": ["eventId", "source"]
        }
        """;

    public SchemaEvolutionTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void AnalyzeChanges_FieldAdded()
    {
        var change = _analyzer.AnalyzeChanges(UserSchemaV1, UserSchemaV2, "user-value", 1, 2);

        _output.WriteLine($"Change type: {change.ChangeType}, Breaking: {change.Breaking}");
        foreach (var fc in change.FieldChanges)
        {
            _output.WriteLine($"  {fc.Type}: {fc.FieldName} ({fc.OldType} -> {fc.NewType})");
        }

        Assert.Equal(SchemaChangeType.FieldAdded, change.ChangeType);
        Assert.Single(change.FieldChanges);
        Assert.Equal("email", change.FieldChanges[0].FieldName);
        Assert.Equal(FieldChangeType.Added, change.FieldChanges[0].Type);
        Assert.True(change.FieldChanges[0].HasDefault); // not in required
        Assert.Equal(BreakingLevel.None, change.Breaking);
    }

    [Fact]
    public void AnalyzeChanges_FieldRemoved()
    {
        // OrderV1 -> OrderV2: legacyId removed (+ discountCode added, but let's focus on removal)
        var change = _analyzer.AnalyzeChanges(OrderSchemaV1, OrderSchemaV2, "orders-value", 1, 2);

        _output.WriteLine($"Change type: {change.ChangeType}, Breaking: {change.Breaking}");
        foreach (var fc in change.FieldChanges)
        {
            _output.WriteLine($"  {fc.Type}: {fc.FieldName} ({fc.OldType} -> {fc.NewType})");
        }

        // legacyId was removed — it's the same type as discountCode (string vs integer), so NOT a rename
        // Actually legacyId is integer and discountCode is string, so they WON'T be detected as rename
        var removed = change.FieldChanges.FirstOrDefault(fc => fc.Type == FieldChangeType.Removed);
        Assert.NotNull(removed);
        Assert.Equal("legacyId", removed!.FieldName);
        Assert.Equal(BreakingLevel.Major, removed.Breaking);

        var added = change.FieldChanges.FirstOrDefault(fc => fc.Type == FieldChangeType.Added);
        Assert.NotNull(added);
        Assert.Equal("discountCode", added!.FieldName);

        // Overall breaking is Major because of removal
        Assert.Equal(BreakingLevel.Major, change.Breaking);
    }

    [Fact]
    public void AnalyzeChanges_TypeChanged()
    {
        var change = _analyzer.AnalyzeChanges(OrderSchemaV2, OrderSchemaV3, "orders-value", 2, 3);

        _output.WriteLine($"Change type: {change.ChangeType}, Breaking: {change.Breaking}");
        foreach (var fc in change.FieldChanges)
        {
            _output.WriteLine($"  {fc.Type}: {fc.FieldName} ({fc.OldType} -> {fc.NewType})");
        }

        var typeChanged = change.FieldChanges.FirstOrDefault(fc => fc.Type == FieldChangeType.TypeChanged);
        Assert.NotNull(typeChanged);
        Assert.Equal("amount", typeChanged!.FieldName);
        Assert.Equal("number", typeChanged.OldType);
        Assert.Equal("string", typeChanged.NewType);
        Assert.Equal(BreakingLevel.Major, typeChanged.Breaking);
    }

    [Fact]
    public void AnalyzeChanges_FieldRenamed()
    {
        var change = _analyzer.AnalyzeChanges(UserSchemaV2, UserSchemaV3, "user-value", 2, 3);

        _output.WriteLine($"Change type: {change.ChangeType}, Breaking: {change.Breaking}");
        foreach (var fc in change.FieldChanges)
        {
            _output.WriteLine($"  {fc.Type}: {fc.FieldName} (old: {fc.OldFieldName}, {fc.OldType} -> {fc.NewType})");
        }

        // "name" (string) removed + "fullName" (string) added = rename detected
        var renamed = change.FieldChanges.FirstOrDefault(fc => fc.Type == FieldChangeType.Renamed);
        Assert.NotNull(renamed);
        Assert.Equal("fullName", renamed!.FieldName);
        Assert.Equal("name", renamed.OldFieldName);
        Assert.Equal(BreakingLevel.Major, renamed.Breaking);
    }

    [Fact]
    public void AnalyzeChanges_MadeNullable()
    {
        var change = _analyzer.AnalyzeChanges(OrderSchemaV2, OrderSchemaV3, "orders-value", 2, 3);

        var nullable = change.FieldChanges.FirstOrDefault(fc => fc.Type == FieldChangeType.MadeNullable);
        Assert.NotNull(nullable);
        Assert.Equal("customerId", nullable!.FieldName);
        Assert.Equal(BreakingLevel.None, nullable.Breaking);
    }

    [Fact]
    public void AssessBreaking_AddedWithDefault_IsNone()
    {
        var change = _analyzer.AnalyzeChanges(UserSchemaV1, UserSchemaV2, "user-value", 1, 2);

        Assert.Equal(BreakingLevel.None, _analyzer.AssessBreakingLevel(change));
    }

    [Fact]
    public void AssessBreaking_FieldRemoved_IsMajor()
    {
        var change = _analyzer.AnalyzeChanges(OrderSchemaV1, OrderSchemaV2, "orders-value", 1, 2);

        Assert.Equal(BreakingLevel.Major, _analyzer.AssessBreakingLevel(change));
    }

    [Fact]
    public void AssessBreaking_MadeRequired_IsMajor()
    {
        var change = _analyzer.AnalyzeChanges(EventSchemaOptional, EventSchemaRequired, "event-value", 1, 2);

        _output.WriteLine($"Breaking: {change.Breaking}");
        foreach (var fc in change.FieldChanges)
        {
            _output.WriteLine($"  {fc.Type}: {fc.FieldName}, breaking={fc.Breaking}");
        }

        Assert.Equal(BreakingLevel.Major, change.Breaking);
        var madeRequired = change.FieldChanges.FirstOrDefault(fc => fc.Type == FieldChangeType.MadeRequired);
        Assert.NotNull(madeRequired);
        Assert.Equal("source", madeRequired!.FieldName);
    }

    [Fact]
    public void GenerateModelClass_FromSchema()
    {
        var code = _codeGen.GenerateModelClass(UserSchemaV2, "UserEvent", "MyApp.Models");

        _output.WriteLine(code);

        Assert.Contains("namespace MyApp.Models;", code);
        Assert.Contains("class UserEvent", code);
        Assert.Contains("Id", code);
        Assert.Contains("Name", code);
        Assert.Contains("Email", code);
        Assert.Contains("int", code);   // id is integer
        Assert.Contains("string", code); // name, email are string
    }

    [Fact]
    public void GenerateMigrationCode_FieldAdded()
    {
        var change = _analyzer.AnalyzeChanges(UserSchemaV1, UserSchemaV2, "user-value", 1, 2);
        var code = _codeGen.GenerateMigrationCode(change, "UserEvent");

        _output.WriteLine(code);

        Assert.Contains("Migration: UserEvent v1 -> v2", code);
        Assert.Contains("Email", code);
        Assert.Contains("NEW in v2", code);
    }

    [Fact]
    public void GenerateMigrationCode_FieldRemoved()
    {
        var change = _analyzer.AnalyzeChanges(OrderSchemaV1, OrderSchemaV2, "orders-value", 1, 2);
        var code = _codeGen.GenerateMigrationCode(change, "OrderEvent");

        _output.WriteLine(code);

        Assert.Contains("Removed in v2", code);
        Assert.Contains("LegacyId", code);
    }

    [Fact]
    public void ImpactReport_IncludesAllChanges()
    {
        var change = _analyzer.AnalyzeChanges(OrderSchemaV2, OrderSchemaV3, "orders-value", 2, 3);
        var report = _analyzer.GenerateImpactReport(change);

        _output.WriteLine(report.Summary);
        _output.WriteLine($"Steps: {report.MigrationSteps.Count}");
        _output.WriteLine($"Code length: {report.GeneratedCode.Length}");

        Assert.NotEmpty(report.Summary);
        Assert.NotEmpty(report.MigrationSteps);
        Assert.NotEmpty(report.GeneratedCode);
        Assert.Contains("orders-value", report.Summary);
        Assert.Equal(change, report.Change);
    }

    [Fact]
    public void GenerateConsumerUpdateCode_ProducesActionableCode()
    {
        var change = _analyzer.AnalyzeChanges(UserSchemaV1, UserSchemaV2, "user-value", 1, 2);
        var code = _codeGen.GenerateConsumerUpdateCode(change, "UserEvent");

        _output.WriteLine(code);

        Assert.Contains("Consumer Update Guide", code);
        Assert.Contains("JsonSerializer.Deserialize", code);
    }

    [Fact]
    public void NoChanges_ProducesNoActionStep()
    {
        // Same schema → no changes
        var change = _analyzer.AnalyzeChanges(UserSchemaV1, UserSchemaV1, "user-value", 1, 1);
        var report = _analyzer.GenerateImpactReport(change);

        Assert.Single(report.MigrationSteps);
        Assert.Equal(MigrationAction.NoActionNeeded, report.MigrationSteps[0].Action);
        Assert.Equal(BreakingLevel.None, change.Breaking);
    }

    [Fact]
    public async Task LlmEnhancer_FallsBackToRuleBased_WhenNoLlm()
    {
        var enhancer = new SchemaEvolutionLlmEnhancer(null);
        var change = _analyzer.AnalyzeChanges(UserSchemaV1, UserSchemaV2, "user-value", 1, 2);

        var explanation = await enhancer.ExplainChangeAsync(change);

        _output.WriteLine(explanation);

        Assert.NotEmpty(explanation);
        Assert.Contains("user-value", explanation);
        Assert.Contains("non-breaking", explanation);
    }

    [Fact]
    public async Task LlmEnhancer_FallsBackOnFailure()
    {
        // Simulate LLM failure
        var enhancer = new SchemaEvolutionLlmEnhancer(
            (_, _, _) => throw new InvalidOperationException("LLM unavailable"));

        var change = _analyzer.AnalyzeChanges(UserSchemaV1, UserSchemaV2, "user-value", 1, 2);
        var explanation = await enhancer.ExplainChangeAsync(change);

        _output.WriteLine(explanation);

        // Should fall back to rule-based instead of throwing
        Assert.NotEmpty(explanation);
        Assert.Contains("user-value", explanation);
    }

    [Fact]
    public async Task LlmEnhancer_UsesLlm_WhenAvailable()
    {
        var llmCalled = false;
        var enhancer = new SchemaEvolutionLlmEnhancer(
            (system, user, ct) =>
            {
                llmCalled = true;
                return Task.FromResult("AI explanation: The schema added an email field.");
            });

        var change = _analyzer.AnalyzeChanges(UserSchemaV1, UserSchemaV2, "user-value", 1, 2);
        var explanation = await enhancer.ExplainChangeAsync(change);

        _output.WriteLine(explanation);

        Assert.True(llmCalled);
        Assert.Contains("AI explanation", explanation);
    }

    [Fact]
    public void ToPascalCase_HandlesVariousFormats()
    {
        Assert.Equal("OrderId", SchemaEvolutionAnalyzer.ToPascalCase("orderId"));
        Assert.Equal("DiscountCode", SchemaEvolutionAnalyzer.ToPascalCase("discount_code"));
        Assert.Equal("MyField", SchemaEvolutionAnalyzer.ToPascalCase("my-field"));
        Assert.Equal("Id", SchemaEvolutionAnalyzer.ToPascalCase("id"));
    }

    [Fact]
    public void DeriveClassName_HandlesSubjectNames()
    {
        Assert.Equal("OrdersValue", SchemaEvolutionAnalyzer.DeriveClassName("orders-value"));
        Assert.Equal("UserEvents", SchemaEvolutionAnalyzer.DeriveClassName("user-events"));
        Assert.Equal("MyTopic", SchemaEvolutionAnalyzer.DeriveClassName("my_topic"));
    }

    [Fact]
    public void MapJsonTypeToCSharp_CoversAllTypes()
    {
        Assert.Equal("string", SchemaEvolutionAnalyzer.MapJsonTypeToCSharp("string"));
        Assert.Equal("int", SchemaEvolutionAnalyzer.MapJsonTypeToCSharp("integer"));
        Assert.Equal("double", SchemaEvolutionAnalyzer.MapJsonTypeToCSharp("number"));
        Assert.Equal("bool", SchemaEvolutionAnalyzer.MapJsonTypeToCSharp("boolean"));
        Assert.Equal("List<object>", SchemaEvolutionAnalyzer.MapJsonTypeToCSharp("array"));
        Assert.Equal("object", SchemaEvolutionAnalyzer.MapJsonTypeToCSharp("object"));
        Assert.Equal("object", SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(null));
    }
}
