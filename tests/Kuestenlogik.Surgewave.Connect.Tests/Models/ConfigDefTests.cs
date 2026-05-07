using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connect.Tests.Models;

/// <summary>
/// Tests for ConfigDef, ConfigKey, ConfigType, and Importance.
/// </summary>
public sealed class ConfigDefTests
{
    [Fact]
    public void ConfigDef_DefineWithDefault_AddsKey()
    {
        var configDef = new ConfigDef()
            .Define("my.setting", ConfigType.String, "default-val", Importance.High, "A string setting");

        Assert.Single(configDef.Keys);
        var key = configDef.Keys[0];
        Assert.Equal("my.setting", key.Name);
        Assert.Equal(ConfigType.String, key.Type);
        Assert.Equal("default-val", key.DefaultValue);
        Assert.Equal(Importance.High, key.Importance);
        Assert.Equal("A string setting", key.Documentation);
    }

    [Fact]
    public void ConfigDef_DefineWithoutDefault_SetsNullDefault()
    {
        var configDef = new ConfigDef()
            .Define("required.field", ConfigType.String, Importance.High, "A required field");

        Assert.Single(configDef.Keys);
        Assert.Null(configDef.Keys[0].DefaultValue);
    }

    [Fact]
    public void ConfigDef_ChainedDefine_AddsMultipleKeys()
    {
        var configDef = new ConfigDef()
            .Define("host", ConfigType.String, Importance.High, "Database host")
            .Define("port", ConfigType.Int, 5432, Importance.Medium, "Database port")
            .Define("ssl", ConfigType.Boolean, false, Importance.Low, "Use SSL")
            .Define("password", ConfigType.Password, Importance.High, "Database password");

        Assert.Equal(4, configDef.Keys.Count);
        Assert.Equal("host", configDef.Keys[0].Name);
        Assert.Equal("port", configDef.Keys[1].Name);
        Assert.Equal("ssl", configDef.Keys[2].Name);
        Assert.Equal("password", configDef.Keys[3].Name);
    }

    [Fact]
    public void ConfigDef_Empty_HasNoKeys()
    {
        var configDef = new ConfigDef();

        Assert.Empty(configDef.Keys);
    }

    [Fact]
    public void ConfigType_AllValues_Exist()
    {
        var values = Enum.GetValues<ConfigType>();

        Assert.Contains(ConfigType.String, values);
        Assert.Contains(ConfigType.Int, values);
        Assert.Contains(ConfigType.Long, values);
        Assert.Contains(ConfigType.Double, values);
        Assert.Contains(ConfigType.Boolean, values);
        Assert.Contains(ConfigType.List, values);
        Assert.Contains(ConfigType.Class, values);
        Assert.Contains(ConfigType.Password, values);
        Assert.Equal(8, values.Length);
    }

    [Fact]
    public void Importance_AllValues_Exist()
    {
        var values = Enum.GetValues<Importance>();

        Assert.Contains(Importance.High, values);
        Assert.Contains(Importance.Medium, values);
        Assert.Contains(Importance.Low, values);
        Assert.Equal(3, values.Length);
    }

    [Fact]
    public void ConfigKey_RecordEquality()
    {
        var key1 = new ConfigKey("name", ConfigType.String, "default", Importance.High, "docs");
        var key2 = new ConfigKey("name", ConfigType.String, "default", Importance.High, "docs");
        var key3 = new ConfigKey("other", ConfigType.String, "default", Importance.High, "docs");

        Assert.Equal(key1, key2);
        Assert.NotEqual(key1, key3);
    }

    [Fact]
    public void ConfigDef_AllConfigTypes_CanBeUsed()
    {
        var configDef = new ConfigDef()
            .Define("string.field", ConfigType.String, "s", Importance.High, "doc")
            .Define("int.field", ConfigType.Int, 42, Importance.High, "doc")
            .Define("long.field", ConfigType.Long, 100L, Importance.Medium, "doc")
            .Define("double.field", ConfigType.Double, 3.14, Importance.Medium, "doc")
            .Define("bool.field", ConfigType.Boolean, true, Importance.Low, "doc")
            .Define("list.field", ConfigType.List, "a,b,c", Importance.Low, "doc")
            .Define("class.field", ConfigType.Class, Importance.Low, "doc")
            .Define("pwd.field", ConfigType.Password, Importance.High, "doc");

        Assert.Equal(8, configDef.Keys.Count);
        Assert.Equal(ConfigType.String, configDef.Keys[0].Type);
        Assert.Equal(ConfigType.Int, configDef.Keys[1].Type);
        Assert.Equal(ConfigType.Long, configDef.Keys[2].Type);
        Assert.Equal(ConfigType.Double, configDef.Keys[3].Type);
        Assert.Equal(ConfigType.Boolean, configDef.Keys[4].Type);
        Assert.Equal(ConfigType.List, configDef.Keys[5].Type);
        Assert.Equal(ConfigType.Class, configDef.Keys[6].Type);
        Assert.Equal(ConfigType.Password, configDef.Keys[7].Type);
    }
}
