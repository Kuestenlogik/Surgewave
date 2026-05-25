using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Manages the lifecycle of stream tasks, handling partition assignment and rebalancing.
/// </summary>
internal sealed class TaskManager : IDisposable
{
    private readonly Topology _topology;
    private readonly StreamsConfig _config;
    private readonly ProcessorContext _context;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<TaskId, StreamTask> _activeTasks = new();
    private readonly ConcurrentDictionary<TaskId, StreamTask> _standbyTasks = new();
    private readonly Dictionary<(string Topic, int Partition), StreamTask> _taskLookup = new();
    private bool _disposed;

    public IReadOnlyCollection<StreamTask> ActiveTasks => _activeTasks.Values.ToList();
    public IReadOnlyCollection<StreamTask> StandbyTasks => _standbyTasks.Values.ToList();

    public TaskManager(
        Topology topology,
        StreamsConfig config,
        ProcessorContext context,
        ILogger logger)
    {
        _topology = topology;
        _config = config;
        _context = context;
        _logger = logger;
    }

    /// <summary>
    /// Handles partition assignment from consumer group rebalance.
    /// Creates new tasks for newly assigned partitions.
    /// </summary>
    public void OnPartitionsAssigned(IEnumerable<TopicPartition> partitions)
    {
        var partitionList = partitions.ToList();
        _logger.LogInformation("Partitions assigned: {Partitions}",
            string.Join(", ", partitionList.Select(p => $"{p.Topic}-{p.Partition}")));

        // Group partitions by subtopology
        var subtopologyPartitions = GroupPartitionsBySubtopology(partitionList);

        foreach (var (subtopologyId, subPartitions) in subtopologyPartitions)
        {
            foreach (var partition in subPartitions.Select(p => p.Partition).Distinct())
            {
                var taskId = new TaskId(subtopologyId, partition)
                {
                    Partitions = subPartitions.Where(p => p.Partition == partition).ToList()
                };

                if (!_activeTasks.ContainsKey(taskId))
                {
                    CreateTask(taskId);
                }

                // Populate fast partition→task lookup
                if (_activeTasks.TryGetValue(taskId, out var task))
                {
                    foreach (var tp in task.Partitions)
                    {
                        _taskLookup[(tp.Topic, tp.Partition)] = task;
                    }
                }
            }
        }
    }

    /// <summary>
    /// Handles partition revocation from consumer group rebalance.
    /// Suspends and closes tasks for revoked partitions.
    /// </summary>
    public void OnPartitionsRevoked(IEnumerable<TopicPartition> partitions)
    {
        var partitionSet = partitions.ToHashSet();
        _logger.LogInformation("Partitions revoked: {Partitions}",
            string.Join(", ", partitionSet.Select(p => $"{p.Topic}-{p.Partition}")));

        var tasksToClose = _activeTasks.Values
            .Where(t => t.Partitions.Any(p => partitionSet.Contains(p)))
            .ToList();

        foreach (var task in tasksToClose)
        {
            CloseTask(task);
        }
    }

    /// <summary>
    /// Creates and initializes a new stream task.
    /// </summary>
    private void CreateTask(TaskId taskId)
    {
        _logger.LogDebug("Creating stream task {TaskId}", taskId);

        var task = new StreamTask(taskId, _topology, _context, _logger);
        task.Initialize();

        _activeTasks[taskId] = task;
    }

    /// <summary>
    /// Closes a stream task.
    /// </summary>
    private void CloseTask(StreamTask task)
    {
        _logger.LogDebug("Closing stream task {TaskId}", task.TaskId);

        // Remove from fast partition→task lookup
        foreach (var tp in task.Partitions)
        {
            _taskLookup.Remove((tp.Topic, tp.Partition));
        }

        task.Suspend();
        task.Close();

        _activeTasks.TryRemove(task.TaskId, out _);
    }

    /// <summary>
    /// Processes a record by routing it to the appropriate task.
    /// </summary>
    public bool Process(string topic, int partition, byte[] key, byte[] value, long timestamp, long offset)
    {
        if (!_taskLookup.TryGetValue((topic, partition), out var task))
        {
            _logger.LogWarning("No task found for {Topic}-{Partition}", topic, partition);
            return false;
        }

        task.Process(topic, partition, key, value, timestamp, offset);
        return true;
    }

    /// <summary>
    /// Punctuates all active tasks.
    /// </summary>
    public void Punctuate(long currentTime)
    {
        foreach (var task in _activeTasks.Values)
        {
            task.MaybePunctuate(currentTime);
        }
    }

    /// <summary>
    /// Commits all tasks that need committing.
    /// </summary>
    public int MaybeCommitAll()
    {
        var commitIntervalMs = _config.CommitIntervalMs;
        var committed = 0;

        foreach (var task in _activeTasks.Values)
        {
            if (task.MaybeCommit(commitIntervalMs))
                committed++;
        }

        return committed;
    }

    /// <summary>
    /// Forces commit on all tasks.
    /// </summary>
    public void CommitAll()
    {
        foreach (var task in _activeTasks.Values)
        {
            task.Commit();
        }
    }

    /// <summary>
    /// Groups partitions by their subtopology based on source topics.
    /// </summary>
    private Dictionary<int, List<TopicPartition>> GroupPartitionsBySubtopology(
        IEnumerable<TopicPartition> partitions)
    {
        var result = new Dictionary<int, List<TopicPartition>>();
        var subtopologyId = 0;

        // For now, treat all sources as one subtopology
        // In a full implementation, this would analyze the topology graph
        foreach (var source in _topology.Sources)
        {
            var topicProp = source.GetType().GetProperty("TopicPattern");
            var topic = topicProp?.GetValue(source)?.ToString();

            if (topic != null)
            {
                var matching = partitions.Where(p => p.Topic == topic).ToList();
                if (matching.Count > 0)
                {
                    if (!result.ContainsKey(subtopologyId))
                        result[subtopologyId] = [];

                    result[subtopologyId].AddRange(matching);
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Gets the task for a specific partition.
    /// </summary>
    public StreamTask? GetTask(string topic, int partition)
    {
        return _taskLookup.GetValueOrDefault((topic, partition));
    }

    /// <summary>
    /// Suspends all tasks.
    /// </summary>
    public void SuspendAll()
    {
        foreach (var task in _activeTasks.Values)
        {
            task.Suspend();
        }
    }

    /// <summary>
    /// Resumes all suspended tasks.
    /// </summary>
    public void ResumeAll()
    {
        foreach (var task in _activeTasks.Values)
        {
            task.Resume();
        }
    }

    /// <summary>
    /// Closes all tasks.
    /// </summary>
    public void CloseAll()
    {
        foreach (var task in _activeTasks.Values.ToList())
        {
            CloseTask(task);
        }

        foreach (var task in _standbyTasks.Values.ToList())
        {
            task.Close();
            _standbyTasks.TryRemove(task.TaskId, out _);
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        CloseAll();
        _disposed = true;
    }
}
