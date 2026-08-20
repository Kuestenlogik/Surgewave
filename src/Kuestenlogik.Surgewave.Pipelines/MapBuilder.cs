namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// Builds the field mappings of a Map node. Each mapping produces one field in the output
/// record, sourced from a JSON path into the input record or from a literal.
/// </summary>
public class MapBuilder
{
    private readonly Dictionary<string, string> _config = [];
    private bool _includeOriginal;

    /// <summary>
    /// Maps <paramref name="sourcePath"/> (a JSON path like <c>$.customer.id</c>, or
    /// <c>field[0]</c> for array elements) to <paramref name="targetField"/> in the output.
    /// </summary>
    public MapBuilder Field(string targetField, string sourcePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetField);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        _config["mapping." + targetField] = sourcePath;
        return this;
    }

    /// <summary>Sets <paramref name="targetField"/> to a literal string value.</summary>
    public MapBuilder Literal(string targetField, string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetField);
        ArgumentNullException.ThrowIfNull(value);

        // The Map node detects literals by their edge quotes and strips ALL leading/trailing
        // quote characters — user quotes at the edges would be silently lost.
        if (value.Length > 0 && (value[0] == '\'' || value[^1] == '\''))
        {
            throw new PipelineBuildException(
                $"The literal '{value}' starts or ends with a quote character, which the Map node's " +
                "literal parsing would strip.");
        }

        _config["mapping." + targetField] = $"'{value}'";
        return this;
    }

    /// <summary>Copies all fields of the input record into the output before applying mappings.</summary>
    public MapBuilder IncludeOriginal()
    {
        _includeOriginal = true;
        return this;
    }

    internal IReadOnlyDictionary<string, string> BuildConfig()
    {
        if (_config.Count == 0)
        {
            throw new PipelineBuildException("A Map stage needs at least one .Field(...) or .Literal(...) mapping.");
        }

        var config = new Dictionary<string, string>(_config);
        if (_includeOriginal)
        {
            config["include.original"] = "true";
        }

        return config;
    }
}
