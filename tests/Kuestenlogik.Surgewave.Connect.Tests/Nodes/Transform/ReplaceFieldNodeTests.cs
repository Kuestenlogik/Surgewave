namespace Kuestenlogik.Surgewave.Connect.Tests.Nodes.Transform;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Nodes.Transform;

public class ReplaceFieldNodeTests
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

    private static string GetEmittedValue(ReplaceFieldNodeTask task, int index = 0) =>
        Encoding.UTF8.GetString(task.EmittedRecords[index].Value);

    [Fact]
    public async Task IncludeFields_OnlyKeepsListedFields()
    {
        var task = new ReplaceFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields.include"] = "name,age"
        });

        var record = CreateRecord("{\"name\":\"Alice\",\"age\":30,\"email\":\"alice@test.com\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"name\":\"Alice\"", value);
        Assert.Contains("\"age\":30", value);
        Assert.DoesNotContain("email", value);
    }

    [Fact]
    public async Task ExcludeFields_RemovesListedFields()
    {
        var task = new ReplaceFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields.exclude"] = "password,secret"
        });

        var record = CreateRecord("{\"name\":\"Bob\",\"password\":\"123\",\"secret\":\"x\",\"role\":\"admin\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"name\":\"Bob\"", value);
        Assert.Contains("\"role\":\"admin\"", value);
        Assert.DoesNotContain("password", value);
        Assert.DoesNotContain("secret", value);
    }

    [Fact]
    public async Task ExcludeTakesPrecedenceOverInclude()
    {
        var task = new ReplaceFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields.include"] = "name,age,email",
            ["fields.exclude"] = "email"
        });

        var record = CreateRecord("{\"name\":\"Carol\",\"age\":25,\"email\":\"c@test.com\",\"extra\":\"x\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"name\":\"Carol\"", value);
        Assert.Contains("\"age\":25", value);
        Assert.DoesNotContain("email", value);
        Assert.DoesNotContain("extra", value);
    }

    [Fact]
    public async Task RenameFields_RenamesCorrectly()
    {
        var task = new ReplaceFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields.renames"] = "name:fullName,age:years"
        });

        var record = CreateRecord("{\"name\":\"Dave\",\"age\":40,\"city\":\"Berlin\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"fullName\":\"Dave\"", value);
        Assert.Contains("\"years\":40", value);
        Assert.Contains("\"city\":\"Berlin\"", value);
        Assert.DoesNotContain("\"name\"", value);
        Assert.DoesNotContain("\"age\"", value);
    }

    [Fact]
    public async Task IncludeExcludeAndRename_Combined()
    {
        var task = new ReplaceFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields.include"] = "name,age,email",
            ["fields.exclude"] = "email",
            ["fields.renames"] = "name:userName"
        });

        var record = CreateRecord("{\"name\":\"Eve\",\"age\":28,\"email\":\"e@test.com\",\"role\":\"user\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"userName\":\"Eve\"", value);
        Assert.Contains("\"age\":28", value);
        Assert.DoesNotContain("email", value);
        Assert.DoesNotContain("role", value);
    }

    [Fact]
    public async Task NonJsonValue_PassedThroughUnchanged()
    {
        var task = new ReplaceFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields.exclude"] = "foo"
        });

        var record = CreateRecord("not json at all");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("not json at all", GetEmittedValue(task));
    }

    [Fact]
    public async Task NoFilters_AllFieldsPreserved()
    {
        var task = new ReplaceFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output"
        });

        var record = CreateRecord("{\"a\":1,\"b\":2,\"c\":3}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        var value = GetEmittedValue(task);
        Assert.Contains("\"a\":1", value);
        Assert.Contains("\"b\":2", value);
        Assert.Contains("\"c\":3", value);
    }

    [Fact]
    public async Task NoOutputTopic_EmitsNothing()
    {
        var task = new ReplaceFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["fields.include"] = "name"
        });

        var record = CreateRecord("{\"name\":\"test\"}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Empty(task.EmittedRecords);
    }

    [Fact]
    public async Task EmptyObject_StaysEmpty()
    {
        var task = new ReplaceFieldNodeTask();
        task.Start(new Dictionary<string, string>
        {
            ["output.topic"] = "output",
            ["fields.include"] = "name"
        });

        var record = CreateRecord("{}");
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(task.EmittedRecords);
        Assert.Equal("{}", GetEmittedValue(task));
    }
}
