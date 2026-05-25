namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class MaskFieldNodeTests
{
    private static SinkRecord CreateRecord(string value, string? key = null, string topic = "input", int partition = 0, long offset = 0, DateTimeOffset? timestamp = null, Dictionary<string, byte[]>? headers = null)
    {
        return new SinkRecord
        {
            Topic = topic,
            Partition = partition,
            Offset = offset,
            Timestamp = timestamp ?? DateTimeOffset.UtcNow,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = Encoding.UTF8.GetBytes(value),
            Headers = headers
        };
    }

    private static string GetEmittedValue(MaskFieldNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task MaskStringField_ReplacedWithEmpty()
    {
        var task = new MaskFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["mask.fields"] = "password"
        });

        var record = CreateRecord("{\"user\":\"alice\",\"password\":\"secret123\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"user\":\"alice\"", value);
        Assert.Contains("\"password\":\"\"", value);
        Assert.DoesNotContain("secret123", value);
    }

    [Fact]
    public async Task MaskNumberField_ReplacedWithZero()
    {
        var task = new MaskFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["mask.fields"] = "salary"
        });

        var record = CreateRecord("{\"name\":\"Bob\",\"salary\":95000}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"name\":\"Bob\"", value);
        Assert.Contains("\"salary\":0", value);
        Assert.DoesNotContain("95000", value);
    }

    [Fact]
    public async Task MaskBooleanField_ReplacedWithFalse()
    {
        var task = new MaskFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["mask.fields"] = "isAdmin"
        });

        var record = CreateRecord("{\"user\":\"carol\",\"isAdmin\":true}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"isAdmin\":false", value);
    }

    [Fact]
    public async Task MaskArrayField_ReplacedWithEmptyArray()
    {
        var task = new MaskFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["mask.fields"] = "tags"
        });

        var record = CreateRecord("{\"id\":1,\"tags\":[\"admin\",\"vip\"]}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"tags\":[]", value);
    }

    [Fact]
    public async Task MaskNestedField_DotNotation()
    {
        var task = new MaskFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["mask.fields"] = "address.street"
        });

        var record = CreateRecord("{\"name\":\"Dave\",\"address\":{\"street\":\"123 Main St\",\"city\":\"Berlin\"}}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"city\":\"Berlin\"", value);
        Assert.DoesNotContain("123 Main St", value);
    }

    [Fact]
    public async Task MaskMultipleFields_AllMasked()
    {
        var task = new MaskFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["mask.fields"] = "password,ssn,salary"
        });

        var record = CreateRecord("{\"name\":\"Eve\",\"password\":\"pw\",\"ssn\":\"123-45-6789\",\"salary\":100000}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"name\":\"Eve\"", value);
        Assert.Contains("\"password\":\"\"", value);
        Assert.Contains("\"ssn\":\"\"", value);
        Assert.Contains("\"salary\":0", value);
    }

    [Fact]
    public async Task CustomReplacement_OverridesTypeAware()
    {
        var task = new MaskFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["mask.fields"] = "email",
            ["mask.replacement"] = "***REDACTED***"
        });

        var record = CreateRecord("{\"name\":\"Frank\",\"email\":\"frank@example.com\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"email\":\"***REDACTED***\"", value);
    }

    [Fact]
    public async Task NonJsonValue_PassedThroughUnchanged()
    {
        var task = new MaskFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["mask.fields"] = "password"
        });

        var record = CreateRecord("plain text not json");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("plain text not json", GetEmittedValue(task));
    }

    [Fact]
    public async Task NoFieldsConfigured_PassedThroughUnchanged()
    {
        var task = new MaskFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"password\":\"secret\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Contains("\"password\":\"secret\"", GetEmittedValue(task));
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = new MaskFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["mask.fields"] = "password"
        });

        var record = CreateRecord("{\"password\":\"secret\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task FieldNotPresent_OtherFieldsUnaffected()
    {
        var task = new MaskFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["mask.fields"] = "nonExistent"
        });

        var record = CreateRecord("{\"name\":\"test\",\"value\":42}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"name\":\"test\"", value);
        Assert.Contains("\"value\":42", value);
    }
}
