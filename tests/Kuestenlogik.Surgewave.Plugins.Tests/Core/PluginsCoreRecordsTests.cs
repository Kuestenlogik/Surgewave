using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Core;

/// <summary>
/// Coverage fuer die "POCO"-Schicht des Plugins-Assemblys: <see cref="ConfigDef"/>-Builder,
/// <see cref="ConfigKey"/>-Record, <see cref="PluginInfo"/>, <see cref="PluginMetadataAttribute"/>,
/// <see cref="ControlNavItem"/>.
/// </summary>
public sealed class PluginsCoreRecordsTests
{
    // --- ConfigDef ---

    [Fact]
    public void ConfigDef_StartsEmpty()
    {
        var def = new ConfigDef();
        Assert.Empty(def.Keys);
    }

    [Fact]
    public void ConfigDef_Define_AppendsKey_WithDefaultEditor()
    {
        var def = new ConfigDef();

        def.Define("bootstrap.servers", ConfigType.String, "localhost:9092", Importance.High, "Broker bootstrap");

        var key = Assert.Single(def.Keys);
        Assert.Equal("bootstrap.servers", key.Name);
        Assert.Equal(ConfigType.String, key.Type);
        Assert.Equal("localhost:9092", key.DefaultValue);
        Assert.Equal(Importance.High, key.Importance);
        Assert.Equal(EditorHint.Default, key.Editor);
        Assert.Null(key.EditorLanguage);
        Assert.Null(key.Options);
    }

    [Fact]
    public void ConfigDef_DefineWithoutDefault_DefaultValueIsNull()
    {
        var def = new ConfigDef();

        def.Define("password", ConfigType.Password, Importance.High, "Secret");

        Assert.Null(def.Keys[0].DefaultValue);
    }

    [Fact]
    public void ConfigDef_DefineWithEditor_PassesEditorMetadata()
    {
        var def = new ConfigDef();

        def.Define("query", ConfigType.String, "SELECT 1", Importance.Medium, "SQL query",
            editor: EditorHint.Sql, editorLanguage: "sql", options: ["a", "b"]);

        var key = def.Keys[0];
        Assert.Equal(EditorHint.Sql, key.Editor);
        Assert.Equal("sql", key.EditorLanguage);
        Assert.NotNull(key.Options);
        Assert.Equal(["a", "b"], key.Options!);
    }

    [Fact]
    public void ConfigDef_DefineWithEditorWithoutDefault_NullDefaultPassesThrough()
    {
        var def = new ConfigDef();

        def.Define("filter", ConfigType.String, Importance.Low, "Filter expr",
            editor: EditorHint.Expression, editorLanguage: "jmespath");

        var key = def.Keys[0];
        Assert.Null(key.DefaultValue);
        Assert.Equal(EditorHint.Expression, key.Editor);
        Assert.Equal("jmespath", key.EditorLanguage);
    }

    [Fact]
    public void ConfigDef_Define_IsFluent()
    {
        var def = new ConfigDef()
            .Define("a", ConfigType.Int, 1, Importance.Low, "a")
            .Define("b", ConfigType.Boolean, true, Importance.Medium, "b");

        Assert.Equal(2, def.Keys.Count);
    }

    [Fact]
    public void ConfigKey_RecordEquality_SameValues_Equal()
    {
        var a = new ConfigKey("x", ConfigType.String, "v", Importance.Low, "doc");
        var b = new ConfigKey("x", ConfigType.String, "v", Importance.Low, "doc");

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    // --- PluginInfo ---

    [Fact]
    public void PluginInfo_RequiredFields_Set_OptionalsNull()
    {
        var info = new PluginInfo
        {
            Class = "x.Foo",
            Type = "source",
            Version = "1.0.0",
        };

        Assert.Equal("x.Foo", info.Class);
        Assert.Equal("source", info.Type);
        Assert.Equal("1.0.0", info.Version);
        Assert.Null(info.DisplayName);
        Assert.Null(info.Icon);
        Assert.Null(info.Category);
        Assert.Null(info.Description);
    }

    [Fact]
    public void PluginInfo_AllOptionalsPopulate()
    {
        var info = new PluginInfo
        {
            Class = "x",
            Type = "sink",
            Version = "2.0.0",
            DisplayName = "X Sink",
            Icon = "Output",
            Category = "Integration",
            Description = "Writes to X",
        };

        Assert.Equal("X Sink", info.DisplayName);
        Assert.Equal("Output", info.Icon);
        Assert.Equal("Integration", info.Category);
        Assert.Equal("Writes to X", info.Description);
    }

    // --- PluginMetadataAttribute ---

    [Fact]
    public void PluginMetadataAttribute_RequiredName_OptionalsDefaultNull()
    {
        var attr = new PluginMetadataAttribute { Name = "Test" };

        Assert.Equal("Test", attr.Name);
        Assert.Null(attr.Description);
        Assert.Null(attr.Version);
        Assert.Null(attr.Author);
        Assert.Null(attr.DocumentationUrl);
        Assert.Null(attr.LicenseUrl);
        Assert.Null(attr.Tags);
        Assert.Null(attr.Icon);
    }

    [Fact]
    public void PluginMetadataAttribute_AllFieldsRoundtrip()
    {
        var attr = new PluginMetadataAttribute
        {
            Name = "Hue Connector",
            Description = "Philips Hue",
            Version = "1.2.3",
            Author = "Kuestenlogik",
            DocumentationUrl = "https://surgewave.io/connectors/hue",
            LicenseUrl = "https://apache.org/licenses/LICENSE-2.0",
            Tags = "iot,hue",
            Icon = "Lightbulb",
        };

        Assert.Equal("Hue Connector", attr.Name);
        Assert.Equal("Philips Hue", attr.Description);
        Assert.Equal("1.2.3", attr.Version);
        Assert.Equal("Kuestenlogik", attr.Author);
        Assert.Equal("https://surgewave.io/connectors/hue", attr.DocumentationUrl);
        Assert.Equal("https://apache.org/licenses/LICENSE-2.0", attr.LicenseUrl);
        Assert.Equal("iot,hue", attr.Tags);
        Assert.Equal("Lightbulb", attr.Icon);
    }

    // --- ControlNavItem ---

    [Fact]
    public void ControlNavItem_RequiredPositionals_DefaultsForOptionals()
    {
        var item = new ControlNavItem("Topics", "/topics", "Topic");

        Assert.Equal("Topics", item.Title);
        Assert.Equal("/topics", item.Href);
        Assert.Equal("Topic", item.Icon);
        Assert.Null(item.Group);
        Assert.Equal(100, item.Order);
    }

    [Fact]
    public void ControlNavItem_RecordEquality_ByValue()
    {
        var a = new ControlNavItem("X", "/x", "ic", "Group", 10);
        var b = new ControlNavItem("X", "/x", "ic", "Group", 10);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void ControlNavItem_With_ChangesOrder_PreservesRest()
    {
        var a = new ControlNavItem("X", "/x", "ic", "G", 10);
        var b = a with { Order = 20 };

        Assert.Equal(20, b.Order);
        Assert.Equal(a.Title, b.Title);
        Assert.Equal(a.Group, b.Group);
    }

}
