using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Core;

/// <summary>
/// Trifft die noch nicht abgedeckten Default-Properties auf <see cref="ConfigKey"/>
/// (88.8 % → 100 %): `with`-Mutationen ueber alle optionalen Felder + Equals/HashCode.
/// </summary>
public sealed class ConfigKeyFullCoverageTests
{
    private static ConfigKey Default() => new(
        Name: "x",
        Type: ConfigType.String,
        DefaultValue: null,
        Importance: Importance.Low,
        Documentation: "doc");

    [Fact]
    public void DefaultEditor_IsEditorHintDefault()
    {
        Assert.Equal(EditorHint.Default, Default().Editor);
    }

    [Fact]
    public void DefaultEditorLanguage_IsNull()
    {
        Assert.Null(Default().EditorLanguage);
    }

    [Fact]
    public void DefaultOptions_IsNull()
    {
        Assert.Null(Default().Options);
    }

    [Fact]
    public void With_ChangingDefaultValue_PreservesRest()
    {
        var a = Default();
        var b = a with { DefaultValue = 42 };

        Assert.Equal(42, b.DefaultValue);
        Assert.Equal(a.Name, b.Name);
        Assert.Equal(a.Documentation, b.Documentation);
    }

    [Fact]
    public void Equality_DifferentDocumentation_NotEqual()
    {
        var a = Default();
        var b = a with { Documentation = "other" };

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Equality_DifferentEditorLanguage_NotEqual()
    {
        var a = Default();
        var b = a with { EditorLanguage = "sql" };

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Equality_DifferentImportance_NotEqual()
    {
        var a = Default();
        var b = a with { Importance = Importance.High };

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Equality_DifferentEditor_NotEqual()
    {
        var a = Default();
        var b = a with { Editor = EditorHint.Code };

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void ToString_ContainsName()
    {
        Assert.Contains("x", Default().ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void GetHashCode_DependsOnAllFields()
    {
        var a = Default();
        var b = a with { Type = ConfigType.Int };

        Assert.NotEqual(a.GetHashCode(), b.GetHashCode());
    }
}
