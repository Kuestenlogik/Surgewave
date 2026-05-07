using Kuestenlogik.Surgewave.Schema.Registry;
using Kuestenlogik.Surgewave.Schema.Registry.Evolution;
using Kuestenlogik.Surgewave.Schema.Registry.Handlers;
using Kuestenlogik.Surgewave.Schema.Registry.Inference;
using Kuestenlogik.Surgewave.Schema.Registry.Migration;

var builder = WebApplication.CreateBuilder(args);

var config = new SchemaRegistryConfig
{
    DataPath = builder.Configuration.GetValue("SchemaRegistry:DataPath", "./schema-data")!,
    DefaultCompatibility = Enum.TryParse<CompatibilityMode>(
        builder.Configuration.GetValue("SchemaRegistry:DefaultCompatibility", "Backward"), true, out var mode)
        ? mode
        : CompatibilityMode.Backward
};

builder.Services.AddSurgewaveSchemaRegistry(config);
builder.Services.AddAvroSchemaHandler();
builder.Services.AddJsonSchemaHandler();
builder.Services.AddProtobufSchemaHandler();
builder.Services.AddFlatBuffersSchemaHandler();

// Inference, evolution, migration — disabled in standalone (inference needs broker topic access)
builder.Services.AddSurgewaveSchemaInference(new SchemaInferenceConfig { Enabled = false });
builder.Services.AddSurgewaveSchemaEvolution(new SchemaEvolutionConfig { Enabled = false });
builder.Services.AddSurgewaveSchemaMigration(new SchemaMigrationConfig { Enabled = false });

var app = builder.Build();

app.MapSurgewaveSchemaRegistry();

var port = builder.Configuration.GetValue("SchemaRegistry:Port", 8081);
app.Urls.Add($"http://*:{port}");

app.Logger.LogInformation("Surgewave Schema Registry (standalone) listening on port {Port}", port);
app.Logger.LogInformation("  Confluent-compatible REST API: http://localhost:{Port}/subjects", port);
app.Logger.LogInformation("  OpenAPI docs: http://localhost:{Port}/scalar", port);

app.Run();
