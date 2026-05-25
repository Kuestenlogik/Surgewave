using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Kuestenlogik.Surgewave.Streams.ExceptionHandling;
using Kuestenlogik.Surgewave.Streams.Monitoring;
using Kuestenlogik.Surgewave.Streams.Processors;
using Kuestenlogik.Surgewave.Streams.Runtime;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Builder for creating stream processing topologies.
/// </summary>
public sealed class StreamsBuilder
{
    private readonly ConcurrentDictionary<string, ProcessorNode> _sources = new();
    private readonly ConcurrentDictionary<string, ProcessorNode> _sinks = new();
    private readonly ConcurrentDictionary<string, ProcessorNode> _repartitionNodes = new();
    private readonly List<object> _stateStoreSuppliers = new();
    private int _nodeCounter;
    private int _storeCounter;

    /// <summary>
    /// The application ID used for internal topic naming (e.g., repartition topics).
    /// Set this before building if you need consistent repartition topic names.
    /// </summary>
    public string ApplicationId { get; set; } = "surgewave-streams";

    /// <summary>
    /// Creates a stream from a topic using default JSON serdes.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="topic">The source topic name.</param>
    /// <returns>A stream reading from the specified topic.</returns>
    /// <example>
    /// <code>
    /// var builder = new StreamsBuilder();
    /// var stream = builder.Stream&lt;string, Order&gt;("orders");
    /// stream.Filter((k, v) => v.Amount > 100).To("large-orders");
    /// </code>
    /// </example>
    public IStream<TKey, TValue> Stream<TKey, TValue>(string topic)
    {
        return Stream<TKey, TValue>(topic, Consumed<TKey, TValue>.With(Serdes.Json<TKey>(), Serdes.Json<TValue>()));
    }

    /// <summary>
    /// Creates a stream from a topic with specific serialization/deserialization configuration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="topic">The source topic name.</param>
    /// <param name="consumed">The serialization configuration specifying key and value serdes.</param>
    /// <returns>A stream reading from the specified topic.</returns>
    public IStream<TKey, TValue> Stream<TKey, TValue>(string topic, Consumed<TKey, TValue> consumed)
    {
        var sourceName = NextNodeName("SOURCE");
        var source = new SourceNode<TKey, TValue>(sourceName, topic, consumed.KeySerde, consumed.ValueSerde);
        _sources[sourceName] = source;

        return new StreamImpl<TKey, TValue>(this, sourceName, source, consumed.KeySerde, consumed.ValueSerde);
    }

    /// <summary>
    /// Creates a stream that merges records from multiple topics.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="topics">The source topic names to merge.</param>
    /// <returns>A merged stream reading from all specified topics.</returns>
    public IStream<TKey, TValue> Stream<TKey, TValue>(IEnumerable<string> topics)
    {
        var consumed = Consumed<TKey, TValue>.With(Serdes.Json<TKey>(), Serdes.Json<TValue>());
        IStream<TKey, TValue>? result = null;

        foreach (var topic in topics)
        {
            var stream = Stream<TKey, TValue>(topic, consumed);
            result = result == null ? stream : result.Merge(stream);
        }

        return result ?? throw new ArgumentException("At least one topic required");
    }

    /// <summary>
    /// Creates a table from a topic using changelog semantics (latest value per key).
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="topic">The source topic name.</param>
    /// <returns>A table backed by a state store.</returns>
    public ITable<TKey, TValue> Table<TKey, TValue>(string topic)
        where TKey : notnull
    {
        return Table<TKey, TValue>(topic, Consumed<TKey, TValue>.With(Serdes.Json<TKey>(), Serdes.Json<TValue>()));
    }

    /// <summary>
    /// Creates a table from a topic with specific serialization configuration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="topic">The source topic name.</param>
    /// <param name="consumed">The serialization configuration specifying key and value serdes.</param>
    /// <returns>A table backed by a state store.</returns>
    public ITable<TKey, TValue> Table<TKey, TValue>(string topic, Consumed<TKey, TValue> consumed)
        where TKey : notnull
    {
        var stream = Stream(topic, consumed);
        return stream.ToTable();
    }

    /// <summary>
    /// Creates a table from a topic with materialization to a named state store.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="topic">The source topic name.</param>
    /// <param name="consumed">The serialization configuration.</param>
    /// <param name="materialized">The materialization configuration specifying the state store.</param>
    /// <returns>A table backed by the specified state store.</returns>
    public ITable<TKey, TValue> Table<TKey, TValue>(string topic, Consumed<TKey, TValue> consumed, Materialized<TKey, TValue> materialized)
        where TKey : notnull
    {
        var stream = Stream(topic, consumed);
        var table = stream.ToTable();

        if (materialized.StoreName != null)
        {
            AddStateStore(Stores.KeyValueStore<TKey, TValue>(materialized.StoreName));
        }

        return table;
    }

    /// <summary>
    /// Creates a global table from a topic, fully replicated on every application instance.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="topic">The source topic name.</param>
    /// <returns>A global table that can be used in stream-global table joins.</returns>
    public IGlobalTable<TKey, TValue> GlobalTable<TKey, TValue>(string topic)
        where TKey : notnull
    {
        return GlobalTable<TKey, TValue>(topic, Consumed<TKey, TValue>.With(Serdes.Json<TKey>(), Serdes.Json<TValue>()));
    }

    /// <summary>
    /// Creates a global table from a topic with specific serialization configuration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="topic">The source topic name.</param>
    /// <param name="consumed">The serialization configuration specifying key and value serdes.</param>
    /// <returns>A global table that can be used in stream-global table joins.</returns>
    public IGlobalTable<TKey, TValue> GlobalTable<TKey, TValue>(string topic, Consumed<TKey, TValue> consumed)
        where TKey : notnull
    {
        var storeName = NextStoreName("GLOBAL-TABLE");

        // Register a source node to populate the global table
        var sourceName = NextNodeName("GLOBAL-SOURCE");
        var source = new SourceNode<TKey, TValue>(sourceName, topic, consumed.KeySerde, consumed.ValueSerde);
        _sources[sourceName] = source;

        // Register a state store for the global table
        AddStateStore(Stores.KeyValueStore<TKey, TValue>(storeName));

        return new GlobalTableImpl<TKey, TValue>(storeName) { SourceTopic = topic };
    }

    /// <summary>
    /// Adds a state store to the topology for use by processors.
    /// </summary>
    /// <typeparam name="TStore">The state store type.</typeparam>
    /// <param name="storeSupplier">The store supplier that creates the state store.</param>
    /// <returns>This builder for fluent chaining.</returns>
    public StreamsBuilder AddStateStore<TStore>(IStoreSupplier<TStore> storeSupplier)
        where TStore : IStateStore
    {
        _stateStoreSuppliers.Add(storeSupplier);
        return this;
    }

    /// <summary>
    /// Adds a global state store populated from a source topic.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="storeSupplier">The store supplier that creates the state store.</param>
    /// <param name="topic">The source topic to populate the store from.</param>
    /// <param name="consumed">The serialization configuration.</param>
    /// <param name="stateUpdateSupplier">A processor supplier that handles state updates.</param>
    /// <returns>This builder for fluent chaining.</returns>
    public StreamsBuilder AddGlobalStore<TKey, TValue>(
        IStoreSupplier<IKeyValueStore<TKey, TValue>> storeSupplier,
        string topic,
        Consumed<TKey, TValue> consumed,
        IProcessorSupplier<TKey, TValue, object, object> stateUpdateSupplier)
        where TKey : notnull
    {
        _stateStoreSuppliers.Add(storeSupplier);
        return this;
    }

    /// <summary>
    /// Builds the topology.
    /// </summary>
    public Topology Build()
    {
        return new Topology(
            _sources.Values,
            _sinks.Values.Cast<ProcessorNode>(),
            _repartitionNodes.Values,
            _stateStoreSuppliers);
    }

    internal string NextNodeName(string prefix)
    {
        return $"{prefix}-{Interlocked.Increment(ref _nodeCounter):D4}";
    }

    internal string NextStoreName(string prefix)
    {
        return $"{prefix}-STORE-{Interlocked.Increment(ref _storeCounter):D4}";
    }

    internal void AddSink<TKey, TValue>(SinkNode<TKey, TValue> sink)
    {
        _sinks[sink.Name] = sink;
    }

    internal void AddRepartitionNode(ProcessorNode node)
    {
        _repartitionNodes[node.Name] = node;
    }

    /// <summary>
    /// Gets all repartition nodes in the topology.
    /// </summary>
    internal IEnumerable<ProcessorNode> RepartitionNodes => _repartitionNodes.Values;
}

/// <summary>
/// Represents the processing topology.
/// </summary>
public sealed class Topology
{
    private readonly IReadOnlyCollection<ProcessorNode> _sources;
    private readonly IReadOnlyCollection<ProcessorNode> _sinks;
    private readonly IReadOnlyCollection<ProcessorNode> _repartitionNodes;
    private readonly IReadOnlyList<object> _stateStoreSuppliers;

    internal Topology(
        IEnumerable<ProcessorNode> sources,
        IEnumerable<ProcessorNode> sinks,
        IEnumerable<ProcessorNode> repartitionNodes,
        IEnumerable<object> stateStoreSuppliers)
    {
        _sources = sources.ToList();
        _sinks = sinks.ToList();
        _repartitionNodes = repartitionNodes.ToList();
        _stateStoreSuppliers = stateStoreSuppliers.ToList();
    }

    /// <summary>Gets the source processor nodes in this topology.</summary>
    public IReadOnlyCollection<ProcessorNode> Sources => _sources;

    /// <summary>Gets the sink processor nodes in this topology.</summary>
    public IReadOnlyCollection<ProcessorNode> Sinks => _sinks;

    /// <summary>Gets the repartition processor nodes in this topology.</summary>
    public IReadOnlyCollection<ProcessorNode> RepartitionNodes => _repartitionNodes;

    /// <summary>Gets the state store suppliers registered in this topology.</summary>
    public IReadOnlyList<object> StateStoreSuppliers => _stateStoreSuppliers;

    /// <summary>
    /// Describes the topology as a string.
    /// </summary>
    public string Describe()
    {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine("Topologies:");
        sb.AppendLine("   Sub-topology: 0");

        foreach (var source in _sources)
        {
            DescribeNode(sb, source, 2);
        }

        return sb.ToString();
    }

    private void DescribeNode(System.Text.StringBuilder sb, ProcessorNode node, int indent)
    {
        var prefix = new string(' ', indent * 2);

        if (node is SourceNode<object, object>)
        {
            sb.AppendLine($"{prefix}Source: {node.Name}");
        }
        else if (node is SinkNode<object, object> sink)
        {
            sb.AppendLine($"{prefix}Sink: {node.Name}");
        }
        else
        {
            sb.AppendLine($"{prefix}Processor: {node.Name}");
        }

        foreach (var child in node.Children)
        {
            DescribeNode(sb, child, indent + 1);
        }
    }
}

/// <summary>
/// Streams application that runs the topology.
/// Supports multi-threaded processing via NumStreamThreads config.
/// Each stream thread owns a subset of partitions and processes them independently via Channels.
/// </summary>
public sealed class StreamsApplication : IAsyncDisposable
{
    private readonly StreamsConfig _config;
    private readonly Topology _topology;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly ProcessorContext _context;
    private readonly TaskManager _taskManager;
    private readonly StreamsConsumer _consumer;
    private readonly StreamsProducer _producer;
    private readonly CancellationTokenSource _cts = new();
    private readonly StreamsLagProvider _lagProvider;

    // Multi-thread support
    private readonly StreamThread[] _streamThreads;
    private readonly Dictionary<TopicPartition, int> _partitionToThread = new();

    // Peer queries (Remote Interactive Queries) — opt-in via WithPeerQueries / WithInteractiveQueries
    private IPeerQueryProvider? _peerQueries;
    private readonly HashSet<TopicPartition> _assignedPartitions = [];

    /// <summary>
    /// The peer query provider registered on this application, or <c>null</c> if none is configured.
    /// Register one via <see cref="WithPeerQueries(IPeerQueryProvider)"/> or the
    /// <c>WithInteractiveQueries(HostInfo)</c> extension method from <c>Kuestenlogik.Surgewave.Streams.InteractiveQueries</c>.
    /// </summary>
    public IPeerQueryProvider? PeerQueries => _peerQueries;

    /// <summary>
    /// Registers a peer query provider on this application. The provider's lifecycle is tied to
    /// <see cref="Start"/> / <see cref="DisposeAsync"/>. Only one provider can be active per application.
    /// Upon registration, the current local metadata (store names, assigned partitions) is pushed
    /// into the provider so that <c>AllMetadata</c> queries work immediately — before <see cref="Start"/>
    /// runs. This matches the pre-refactor behaviour of auto-activation via <c>ApplicationServer</c>.
    /// </summary>
    public StreamsApplication WithPeerQueries(IPeerQueryProvider provider)
    {
        _peerQueries = provider ?? throw new ArgumentNullException(nameof(provider));
        _peerQueries.UpdateLocalMetadata(GetLocalMetadata());
        return this;
    }

    /// <summary>
    /// Returns the live state store with the given name, or <c>null</c>. Used by peer query
    /// providers and interactive-query consumers to reach runtime stores from outside the
    /// processor context.
    /// </summary>
    public IStateStore? GetStateStore(string name) => _context.GetStateStore<IStateStore>(name);

    private Task? _processingTask;

    /// <summary>
    /// Fired when graceful shutdown begins.
    /// </summary>
    public event EventHandler? ShutdownStarted;

    /// <summary>
    /// Fired when graceful shutdown completes.
    /// </summary>
    public event EventHandler? ShutdownCompleted;

    /// <summary>
    /// Creates a new streams application with the specified configuration and topology.
    /// </summary>
    /// <param name="config">The streams configuration.</param>
    /// <param name="topology">The processing topology built by <see cref="StreamsBuilder"/>.</param>
    /// <param name="loggerFactory">The logger factory for diagnostic output.</param>
    public StreamsApplication(StreamsConfig config, Topology topology, ILoggerFactory loggerFactory)
    {
        _config = config;
        _topology = topology;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<StreamsApplication>();

        var metrics = new StreamsMetrics();
        _context = new ProcessorContext(config, metrics, _logger);

        // Initialize runtime components
        _consumer = new StreamsConsumer(config, _logger);
        _producer = new StreamsProducer(config, _logger);
        _taskManager = new TaskManager(topology, config, _context, _logger);

        // Create stream threads
        var numThreads = Math.Max(1, config.NumStreamThreads);
        _streamThreads = new StreamThread[numThreads];

        if (numThreads > 1)
        {
            // Multi-threaded: create N independent StreamThread instances
            for (var i = 0; i < numThreads; i++)
            {
                _streamThreads[i] = CreateStreamThread(i);
            }

            // Wire up consumer events to distribute partitions across threads
            _consumer.PartitionsAssigned += OnPartitionsAssignedMultiThread;
            _consumer.PartitionsRevoked += OnPartitionsRevokedMultiThread;
        }
        else
        {
            // Single-threaded: use the existing TaskManager directly (no Channel overhead)
            _consumer.PartitionsAssigned += partitions =>
            {
                _taskManager.OnPartitionsAssigned(partitions);
                OnPartitionsChanged(partitions, added: true);
            };
            _consumer.PartitionsRevoked += partitions =>
            {
                _taskManager.OnPartitionsRevoked(partitions);
                OnPartitionsChanged(partitions, added: false);
            };
        }

        // Initialize lag provider
        _lagProvider = new StreamsLagProvider(config.ApplicationId, _taskManager, _consumer, metrics);

        // Remote Interactive Queries are opt-in: register a provider via WithPeerQueries
        // / WithInteractiveQueries(HostInfo). Actual startup happens in Start() below.

        // Initialize state stores for the context
        foreach (var supplier in topology.StateStoreSuppliers)
        {
            var getMethod = supplier.GetType().GetMethod("Get");
            if (getMethod != null)
            {
                var store = (IStateStore)getMethod.Invoke(supplier, null)!;
                _context.RegisterStateStore(store);
            }
        }

        // Initialize processor nodes for direct API access
        foreach (var source in topology.Sources)
        {
            InitializeNode(source);
        }
    }

    private StreamThread CreateStreamThread(int threadId)
    {
        var thread = new StreamThread(threadId, _config, _topology, _loggerFactory);
        thread.OnUncaughtException += HandleThreadUncaughtException;
        return thread;
    }

    private void HandleThreadUncaughtException(StreamThread deadThread, Exception exception)
    {
        _logger.LogError(exception, "Uncaught exception in {ThreadName}", deadThread.ThreadName);

        var response = _config.UncaughtExceptionHandler.Handle(deadThread.ThreadName, exception);
        _logger.LogInformation("UncaughtExceptionHandler returned {Response} for {ThreadName}",
            response, deadThread.ThreadName);

        _context.Metrics.RecordUncaughtException();

        switch (response)
        {
            case StreamsUncaughtExceptionResponse.ReplaceThread:
                _ = Task.Run(() => ReplaceDeadThread(deadThread));
                break;
            case StreamsUncaughtExceptionResponse.ShutdownClient:
                _ = Task.Run(CloseAsync);
                break;
            case StreamsUncaughtExceptionResponse.ShutdownApplication:
                _ = Task.Run(async () =>
                {
                    await CloseAsync();
                    Environment.Exit(1);
                });
                break;
        }
    }

    private void ReplaceDeadThread(StreamThread deadThread)
    {
        try
        {
            var threadId = deadThread.ThreadId;
            _logger.LogInformation("Replacing dead thread {ThreadName} (id={ThreadId})", deadThread.ThreadName, threadId);

            // Collect partitions owned by the dead thread
            var ownedPartitions = _partitionToThread
                .Where(kv => kv.Value == threadId)
                .Select(kv => kv.Key)
                .ToList();

            // Dispose dead thread resources
            deadThread.DisposeAsync().AsTask().GetAwaiter().GetResult();

            // Create a fresh thread with the same ID
            var newThread = CreateStreamThread(threadId);
            _streamThreads[threadId] = newThread;
            newThread.Start();

            // Reassign the partitions to the new thread
            if (ownedPartitions.Count > 0)
            {
                newThread.OnPartitionsAssigned(ownedPartitions);
                _logger.LogInformation("Reassigned {Count} partitions to replacement thread {ThreadName}",
                    ownedPartitions.Count, newThread.ThreadName);
            }

            _context.Metrics.RecordThreadReplacement();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to replace dead thread {ThreadId}. Shutting down client.", deadThread.ThreadId);
            _ = Task.Run(CloseAsync);
        }
    }

    private void InitializeNode(ProcessorNode node)
    {
        node.Init(_context);
        foreach (var child in node.Children)
        {
            InitializeNode(child);
        }
    }

    /// <summary>Gets the current state of the streams application.</summary>
    public StreamsState State { get; private set; } = StreamsState.Created;

    /// <summary>
    /// Gets the task manager for accessing tasks (single-thread mode or direct API).
    /// </summary>
    internal TaskManager Tasks => _taskManager;

    /// <summary>
    /// Gets the number of stream threads.
    /// </summary>
    public int NumStreamThreads => _streamThreads.Length;

    /// <summary>
    /// Gets stream thread info for monitoring.
    /// </summary>
    public IReadOnlyList<StreamThreadInfo> StreamThreads =>
        _streamThreads
            .Where(t => t != null)
            .Select(t => new StreamThreadInfo(t.ThreadId, t.ThreadName, t.State.ToString(), t.ProcessedRecords))
            .ToList();

    /// <summary>
    /// Starts the streams application.
    /// </summary>
    public void Start()
    {
        if (State != StreamsState.Created)
            throw new InvalidOperationException($"Cannot start from state {State}");

        State = StreamsState.Rebalancing;
        _logger.LogInformation("Starting streams application {ApplicationId} with {NumThreads} stream thread(s)",
            _config.ApplicationId, _streamThreads.Length);

        // Subscribe to source topics
        var topics = _topology.Sources
            .Select(s => s.GetType().GetProperty("TopicPattern")?.GetValue(s)?.ToString())
            .Where(t => t != null)
            .Cast<string>()
            .Distinct()
            .ToList();

        _consumer.Subscribe(topics);

        // Initialize transactions if exactly-once is enabled
        if (_config.ProcessingGuarantee == ProcessingGuarantee.ExactlyOnce)
        {
            _producer.InitTransactions();
        }

        // Start peer queries if a provider is registered (opt-in via WithInteractiveQueries)
        if (_peerQueries != null)
        {
            _peerQueries.Start(new PeerQueryContext(
                StoreResolver: name => _context.GetStateStore<IStateStore>(name),
                GetLocalMetadata: GetLocalMetadata,
                Logger: _logger));

            // Push current local metadata into the provider
            UpdateLocalMetadata();
        }

        if (_streamThreads.Length > 1)
        {
            // Multi-threaded: start all stream threads, then run dispatcher loop
            for (var i = 0; i < _streamThreads.Length; i++)
            {
                _streamThreads[i].Start();
            }
            _processingTask = Task.Run(MultiThreadDispatchLoop);
        }
        else
        {
            // Single-threaded: direct processing (no Channel overhead)
            _processingTask = Task.Run(ProcessingLoop);
        }

        State = StreamsState.Running;
    }

    /// <summary>
    /// Single-threaded processing loop (NumStreamThreads == 1).
    /// </summary>
    private async Task ProcessingLoop()
    {
        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                var records = await _consumer.PollAsync(_config.PollTimeout, _cts.Token);

                foreach (var record in records)
                {
                    _taskManager.Process(
                        record.Topic,
                        record.Partition,
                        record.Key,
                        record.Value,
                        record.Timestamp,
                        record.Offset);
                }

                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _taskManager.Punctuate(now);
                _taskManager.MaybeCommitAll();
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in processing loop");
            State = StreamsState.Error;
        }
    }

    /// <summary>
    /// Multi-threaded dispatch loop: polls consumer and distributes records to StreamThreads via Channels.
    /// The consumer runs on this single thread; processing is parallelized across StreamThreads.
    /// </summary>
    private async Task MultiThreadDispatchLoop()
    {
        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                var records = await _consumer.PollAsync(_config.PollTimeout, _cts.Token);

                foreach (var record in records)
                {
                    var threadIdx = GetThreadForPartition(record.Topic, record.Partition);
                    var thread = _streamThreads[threadIdx];

                    // Write to thread's channel — backpressure if thread can't keep up
                    if (!thread.TryWrite(record))
                    {
                        await thread.WriteAsync(record, _cts.Token);
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in multi-thread dispatch loop");
            State = StreamsState.Error;
        }
    }

    /// <summary>
    /// Assigns partitions across stream threads using the configured partition assignor.
    /// </summary>
    private void OnPartitionsAssignedMultiThread(IEnumerable<TopicPartition> partitions)
    {
        var partitionList = partitions.ToList();

        // Use pluggable assignor with previous assignment for sticky behavior
        var assignment = _config.PartitionAssignor.Assign(
            partitionList,
            _streamThreads.Length,
            _partitionToThread.Count > 0 ? _partitionToThread : null);

        // Update partition-to-thread mapping
        _partitionToThread.Clear();
        foreach (var (threadIdx, threadPartitions) in assignment)
        {
            foreach (var tp in threadPartitions)
            {
                _partitionToThread[tp] = threadIdx;
            }
        }

        // Notify each thread of its assigned partitions
        foreach (var (threadIdx, threadPartitions) in assignment)
        {
            if (threadPartitions.Count > 0)
                _streamThreads[threadIdx].OnPartitionsAssigned(threadPartitions);
        }

        _logger.LogInformation("Assigned {Count} partitions across {Threads} threads using {Assignor}",
            partitionList.Count, _streamThreads.Length, _config.PartitionAssignor.GetType().Name);

        OnPartitionsChanged(partitionList, added: true);
    }

    /// <summary>
    /// Revokes partitions from the appropriate stream threads.
    /// </summary>
    private void OnPartitionsRevokedMultiThread(IEnumerable<TopicPartition> partitions)
    {
        var threadPartitions = new List<TopicPartition>[_streamThreads.Length];
        for (var i = 0; i < threadPartitions.Length; i++)
            threadPartitions[i] = [];

        foreach (var tp in partitions)
        {
            if (_partitionToThread.TryGetValue(tp, out var threadIdx))
            {
                threadPartitions[threadIdx].Add(tp);
                _partitionToThread.Remove(tp);
            }
        }

        for (var i = 0; i < _streamThreads.Length; i++)
        {
            if (threadPartitions[i].Count > 0)
                _streamThreads[i].OnPartitionsRevoked(threadPartitions[i]);
        }

        OnPartitionsChanged(partitions, added: false);
    }

    /// <summary>
    /// Gets the thread index for a given partition using the cached assignment.
    /// Falls back to hash-based assignment if not found.
    /// </summary>
    private int GetThreadForPartition(string topic, int partition)
    {
        var tp = new TopicPartition(topic, partition);
        if (_partitionToThread.TryGetValue(tp, out var threadIdx))
            return threadIdx;

        // Fallback: consistent hash
        return (int)((uint)HashCode.Combine(topic, partition) % (uint)_streamThreads.Length);
    }

    /// <summary>
    /// Processes a single record through the topology (for testing).
    /// </summary>
    public void ProcessRecord<TKey, TValue>(string topic, TKey key, TValue value, long timestamp = 0)
    {
        var source = _topology.Sources
            .OfType<SourceNode<TKey, TValue>>()
            .FirstOrDefault(s => s.TopicPattern == topic);

        if (source == null)
        {
            _logger.LogWarning("No source found for topic {Topic}", topic);
            return;
        }

        if (timestamp == 0)
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _context.Topic = topic;
        _context.Timestamp = timestamp;

        var keySerde = Serdes.Json<TKey>();
        var valueSerde = Serdes.Json<TValue>();

        var keyBytes = keySerde.Serialize(key);
        var valueBytes = valueSerde.Serialize(value);

        source.Process(keyBytes, valueBytes, timestamp);
        _context.Metrics.RecordProcessed(keyBytes.Length + valueBytes.Length);
    }

    /// <summary>
    /// Processes a tombstone (delete) record through the topology (for testing).
    /// Sends an empty value byte array, which signals deletion to the processor.
    /// </summary>
    public void ProcessRecordTombstone<TKey>(string topic, TKey key, long timestamp = 0)
    {
        var source = _topology.Sources
            .FirstOrDefault(s => s is ITopicSource ts && ts.TopicPattern == topic);

        if (source == null)
        {
            _logger.LogWarning("No source found for topic {Topic}", topic);
            return;
        }

        if (timestamp == 0)
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _context.Topic = topic;
        _context.Timestamp = timestamp;

        var keySerde = Serdes.Json<TKey>();
        var keyBytes = keySerde.Serialize(key);

        source.Process(keyBytes, [], timestamp);
        _context.Metrics.RecordProcessed(keyBytes.Length);
    }

    /// <summary>
    /// Assigns partitions manually (for standalone mode or testing).
    /// </summary>
    public void AssignPartitions(IEnumerable<TopicPartition> partitions)
    {
        _consumer.SimulateAssignment(partitions);
    }

    /// <summary>
    /// Gets a state store by name.
    /// </summary>
    public TStore? GetStateStore<TStore>(string name) where TStore : class, IStateStore
    {
        return _context.GetStateStore<TStore>(name);
    }

    /// <summary>
    /// Gets the metrics.
    /// </summary>
    public StreamsMetrics Metrics => _context.Metrics;

    /// <summary>
    /// Gets the consumer lag provider for monitoring consumer lag.
    /// </summary>
    public IStreamsLagProvider Lag => _lagProvider;

    /// <summary>
    /// Gets the consumer for advanced access.
    /// </summary>
    internal StreamsConsumer Consumer => _consumer;

    /// <summary>
    /// Gets the producer for advanced access.
    /// </summary>
    internal StreamsProducer Producer => _producer;

    /// <summary>
    /// Commits all tasks.
    /// </summary>
    public void Commit()
    {
        if (_streamThreads.Length > 1)
        {
            for (var i = 0; i < _streamThreads.Length; i++)
                _streamThreads[i]?.Tasks.CommitAll();
        }
        else
        {
            _taskManager.CommitAll();
        }
        _consumer.CommitSync();
        _producer.Flush();
    }

    // --- Peer queries delegation ---
    //
    // The peer-query / remote interactive query API lives in the Kuestenlogik.Surgewave.Streams.InteractiveQueries
    // assembly (namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries). Register an implementation via
    // WithPeerQueries(...) or the WithInteractiveQueries(HostInfo) extension method and call the
    // extension methods on the builder: AllMetadata(), AllMetadataForStore(name),
    // MetadataForKey(...), RegisterPeerAsync(...), CreateCompositeStore(...).
    //
    // This keeps the Streams core free of AspNetCore/TCP-server baggage while still allowing full
    // interactive-query functionality when the opt-in assembly is referenced.

    private void OnPartitionsChanged(IEnumerable<TopicPartition> partitions, bool added)
    {
        foreach (var tp in partitions)
        {
            if (added) _assignedPartitions.Add(tp);
            else _assignedPartitions.Remove(tp);
        }
        UpdateLocalMetadata();
    }

    private void UpdateLocalMetadata()
    {
        if (_peerQueries == null) return;
        _peerQueries.UpdateLocalMetadata(GetLocalMetadata());
    }

    private StreamsMetadata GetLocalMetadata()
    {
        var host = _peerQueries?.LocalHost ?? new HostInfo("unknown", 0);
        var storeNames = _topology.StateStoreSuppliers
            .Select(s => s.GetType().GetProperty("Name")?.GetValue(s)?.ToString())
            .Where(n => n != null)
            .Cast<string>()
            .ToList();

        return new StreamsMetadata(host, storeNames, _assignedPartitions);
    }

    /// <summary>
    /// Closes the streams application with graceful shutdown.
    /// </summary>
    public async Task CloseAsync()
    {
        if (State == StreamsState.NotRunning)
            return;

        _logger.LogInformation("Closing streams application {ApplicationId}", _config.ApplicationId);
        State = StreamsState.PendingShutdown;

        ShutdownStarted?.Invoke(this, EventArgs.Empty);

        // Stop peer queries (if a provider was registered)
        if (_peerQueries != null)
        {
            await _peerQueries.DisposeAsync();
            _peerQueries = null;
        }

        _cts.Cancel();

        // Shut down stream threads
        if (_streamThreads.Length > 1)
        {
            var shutdownTasks = new Task[_streamThreads.Length];
            for (var i = 0; i < _streamThreads.Length; i++)
            {
                shutdownTasks[i] = _streamThreads[i].DisposeAsync().AsTask();
            }
            await Task.WhenAll(shutdownTasks);
        }

        if (_processingTask != null)
        {
            try
            {
                await _processingTask;
            }
            catch (OperationCanceledException) { }
        }

        // Close runtime components
        _taskManager.CloseAll();
        _producer.Dispose();
        _consumer.Dispose();

        // Graceful shutdown with reverse-topological order and lifecycle hooks
        var orchestrator = new ShutdownOrchestrator(_logger, _config.ShutdownTimeout);
        orchestrator.Shutdown(_topology.Sources, _context);

        State = StreamsState.NotRunning;

        ShutdownCompleted?.Invoke(this, EventArgs.Empty);
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync();
        _taskManager.Dispose();
        _context.Metrics.Dispose();
        _cts.Dispose();
    }
}

/// <summary>
/// Information about a stream thread for monitoring.
/// </summary>
public readonly record struct StreamThreadInfo(
    int ThreadId,
    string ThreadName,
    string State,
    long ProcessedRecords);

/// <summary>
/// Lifecycle state of a <see cref="StreamsApplication"/>.
/// </summary>
public enum StreamsState
{
    /// <summary>The application has been created but not yet started.</summary>
    Created,
    /// <summary>The application is rebalancing partition assignments.</summary>
    Rebalancing,
    /// <summary>The application is actively processing records.</summary>
    Running,
    /// <summary>Shutdown has been requested; the application is draining in-flight records.</summary>
    PendingShutdown,
    /// <summary>The application has been stopped.</summary>
    NotRunning,
    /// <summary>The application encountered a fatal error.</summary>
    Error
}
