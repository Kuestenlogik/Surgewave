using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Api.GraphQL;

/// <summary>
/// Configuration for the Surgewave GraphQL API.
/// Bound from the <c>Surgewave:GraphQL</c> configuration section.
/// </summary>
public sealed class GraphQLConfig : IValidatableConfig
{
    /// <summary>
    /// Configuration section name.
    /// </summary>
    public const string SectionName = "Surgewave:GraphQL";

    /// <summary>
    /// Enable the GraphQL API. Default: false (opt-in).
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// The HTTP path where the GraphQL endpoint is mapped.
    /// Banana Cake Pop (GraphQL IDE) is also available at this path.
    /// Default: "/graphql".
    /// </summary>
    [Required]
    [RegularExpression("^/.*", ErrorMessage = "Path must start with '/'.")]
    public string Path { get; set; } = "/graphql";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
