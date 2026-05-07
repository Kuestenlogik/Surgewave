using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Core.Json;

/// <summary>
/// JSON source generator context for AOT-compatible serialization.
/// Using source generators eliminates reflection and enables Native AOT.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    WriteIndented = false,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(TopicMetadata))]
[JsonSerializable(typeof(List<TopicMetadata>))]
public partial class CoreJsonContext : JsonSerializerContext
{
}
