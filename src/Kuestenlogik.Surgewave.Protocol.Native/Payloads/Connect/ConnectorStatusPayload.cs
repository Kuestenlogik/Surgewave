using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Connect;

/// <summary>
/// Wire format for GetConnectorStatus response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct ConnectorStatusPayload
{
    public string Name { get; init; }
    public string Type { get; init; }
    public string State { get; init; }
    public string WorkerId { get; init; }
    public IReadOnlyList<ConnectorTaskStatusPayload> Tasks { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static ConnectorStatusPayload Read(ref SurgewavePayloadReader reader)
    {
        var name = reader.ReadString() ?? string.Empty;
        var type = reader.ReadString() ?? "unknown";
        var state = reader.ReadString() ?? "UNKNOWN";
        var workerId = reader.ReadString() ?? string.Empty;

        // Read task statuses
        var taskCount = reader.ReadInt32();
        var tasks = new List<ConnectorTaskStatusPayload>(taskCount);
        for (int i = 0; i < taskCount; i++)
        {
            tasks.Add(new ConnectorTaskStatusPayload
            {
                Id = reader.ReadInt32(),
                State = reader.ReadString() ?? "UNKNOWN",
                WorkerId = reader.ReadString() ?? string.Empty,
                Trace = reader.ReadString()
            });
        }

        return new ConnectorStatusPayload
        {
            Name = name,
            Type = type,
            State = state,
            WorkerId = workerId,
            Tasks = tasks
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteString(Type);
        writer.WriteString(State);
        writer.WriteString(WorkerId);

        // Write task statuses
        writer.WriteInt32(Tasks?.Count ?? 0);
        if (Tasks != null)
        {
            foreach (var task in Tasks)
            {
                writer.WriteInt32(task.Id);
                writer.WriteString(task.State);
                writer.WriteString(task.WorkerId);
                writer.WriteString(task.Trace);
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteString(Type);
        writer.WriteString(State);
        writer.WriteString(WorkerId);

        // Write task statuses
        writer.WriteInt32(Tasks?.Count ?? 0);
        if (Tasks != null)
        {
            foreach (var task in Tasks)
            {
                writer.WriteInt32(task.Id);
                writer.WriteString(task.State);
                writer.WriteString(task.WorkerId);
                writer.WriteString(task.Trace);
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        int size = 0;

        // Name, Type, State, WorkerId
        size += 2 + System.Text.Encoding.UTF8.GetByteCount(Name ?? "");
        size += 2 + System.Text.Encoding.UTF8.GetByteCount(Type ?? "");
        size += 2 + System.Text.Encoding.UTF8.GetByteCount(State ?? "");
        size += 2 + System.Text.Encoding.UTF8.GetByteCount(WorkerId ?? "");

        // Tasks count
        size += 4;
        if (Tasks != null)
        {
            foreach (var task in Tasks)
            {
                size += task.EstimateSize();
            }
        }

        return size;
    }
}
