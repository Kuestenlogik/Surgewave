using Kuestenlogik.Surgewave.Plugins;

namespace Kuestenlogik.Surgewave.Connect.Tests.Models;

/// <summary>
/// Tests for PluginInfo, ConnectorMetadataAttribute, ConnectorStateInfo, and related models.
/// </summary>
public sealed class ConnectorPluginTests
{
    [Fact]
    public void ConnectorPlugin_AllProperties_AreSet()
    {
        var plugin = new PluginInfo
        {
            Class = "Kuestenlogik.Surgewave.Connector.FileStream.FileStreamSinkConnector",
            Type = "sink",
            Version = "1.2.3",
            DisplayName = "File Stream Sink",
            Icon = "FileDocument",
            Category = "Integration",
            Description = "Writes records to files"
        };

        Assert.Equal("Kuestenlogik.Surgewave.Connector.FileStream.FileStreamSinkConnector", plugin.Class);
        Assert.Equal("sink", plugin.Type);
        Assert.Equal("1.2.3", plugin.Version);
        Assert.Equal("File Stream Sink", plugin.DisplayName);
        Assert.Equal("FileDocument", plugin.Icon);
        Assert.Equal("Integration", plugin.Category);
        Assert.Equal("Writes records to files", plugin.Description);
    }

    [Fact]
    public void ConnectorPlugin_OptionalFields_DefaultToNull()
    {
        var plugin = new PluginInfo
        {
            Class = "TestConnector",
            Type = "source",
            Version = "1.0.0"
        };

        Assert.Null(plugin.DisplayName);
        Assert.Null(plugin.Icon);
        Assert.Null(plugin.Category);
        Assert.Null(plugin.Description);
    }

    [Fact]
    public void ConnectorMetadataAttribute_AllProperties_AreSet()
    {
        var attr = new ConnectorMetadataAttribute
        {
            Name = "My Connector",
            Description = "A test connector",
            Version = "2.0.0",
            Author = "Test Author",
            DocumentationUrl = "https://docs.example.com",
            LicenseUrl = "https://license.example.com",
            Tags = "test,example,unit",
            Icon = "Radar"
        };

        Assert.Equal("My Connector", attr.Name);
        Assert.Equal("A test connector", attr.Description);
        Assert.Equal("2.0.0", attr.Version);
        Assert.Equal("Test Author", attr.Author);
        Assert.Equal("https://docs.example.com", attr.DocumentationUrl);
        Assert.Equal("https://license.example.com", attr.LicenseUrl);
        Assert.Equal("test,example,unit", attr.Tags);
        Assert.Equal("Radar", attr.Icon);
    }

    [Fact]
    public void ConnectorMetadataAttribute_OptionalFields_DefaultToNull()
    {
        var attr = new ConnectorMetadataAttribute { Name = "Minimal" };

        Assert.Equal("Minimal", attr.Name);
        Assert.Null(attr.Description);
        Assert.Null(attr.Version);
        Assert.Null(attr.Author);
        Assert.Null(attr.DocumentationUrl);
        Assert.Null(attr.LicenseUrl);
        Assert.Null(attr.Tags);
        Assert.Null(attr.Icon);
    }

    [Fact]
    public void ConnectorStateInfo_AllProperties_AreSet()
    {
        var state = new ConnectorStateInfo
        {
            State = "RUNNING",
            WorkerId = "worker-1",
            Trace = "some trace info"
        };

        Assert.Equal("RUNNING", state.State);
        Assert.Equal("worker-1", state.WorkerId);
        Assert.Equal("some trace info", state.Trace);
    }

    [Fact]
    public void ConnectorStateInfo_TraceIsOptional()
    {
        var state = new ConnectorStateInfo
        {
            State = "RUNNING",
            WorkerId = "worker-0"
        };

        Assert.Null(state.Trace);
    }

    [Fact]
    public void TopicPartition_RecordEquality()
    {
        var tp1 = new TopicPartition("my-topic", 3);
        var tp2 = new TopicPartition("my-topic", 3);
        var tp3 = new TopicPartition("other-topic", 3);

        Assert.Equal(tp1, tp2);
        Assert.NotEqual(tp1, tp3);
    }

    [Fact]
    public void TopicPartition_Properties()
    {
        var tp = new TopicPartition("orders", 7);

        Assert.Equal("orders", tp.Topic);
        Assert.Equal(7, tp.Partition);
    }

    [Fact]
    public void TaskId_Properties()
    {
        var taskId = new TaskId
        {
            Connector = "file-connector",
            Task = 2
        };

        Assert.Equal("file-connector", taskId.Connector);
        Assert.Equal(2, taskId.Task);
    }

    [Fact]
    public void RecordMetadata_AllProperties()
    {
        var ts = DateTimeOffset.UtcNow;
        var metadata = new RecordMetadata
        {
            Topic = "events",
            Partition = 5,
            Offset = 12345,
            Timestamp = ts
        };

        Assert.Equal("events", metadata.Topic);
        Assert.Equal(5, metadata.Partition);
        Assert.Equal(12345, metadata.Offset);
        Assert.Equal(ts, metadata.Timestamp);
    }

    [Fact]
    public void RecordMetadata_RecordEquality()
    {
        var ts = DateTimeOffset.UtcNow;
        var m1 = new RecordMetadata { Topic = "t", Partition = 0, Offset = 1, Timestamp = ts };
        var m2 = new RecordMetadata { Topic = "t", Partition = 0, Offset = 1, Timestamp = ts };

        Assert.Equal(m1, m2);
    }
}
